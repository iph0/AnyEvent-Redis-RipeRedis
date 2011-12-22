package AnyEvent::RipeRedis;

use 5.010000;
use strict;
use warnings;

use fields qw(
  host
  port
  password
  encoding
  reconnect
  reconnect_after
  max_reconnect_attempts
  on_connect
  on_auth
  on_stop_reconnect
  on_error
  handle
  reconnect_attempt
  commands_queue
  transac_begun
  subscr_active
  subscrs
  psubscrs
);

our $VERSION = '0.300010';

use AnyEvent;
use AnyEvent::Handle;
use Encode qw( find_encoding is_utf8 );
use Scalar::Util 'looks_like_number';
use Carp 'croak';

# Available error codes
our $ERR_CONNECT = 1;
our $ERR_HANDLE = 2;
our $ERR_EOF = 3;
our $ERR_COMMAND = 4;
our $ERR_SUBS = 5;
our $ERR_PARSING = 6;

my $EOL = "\r\n";


# Constructor
sub new {
  my $proto = shift;
  my $opts = shift;

  if ( ref( $opts ) ne 'HASH' ) {
    $opts = {};
  }

  my $class = ref( $proto ) || $proto;
  my $self = fields::new( $class );

  $self->{ 'host' } = $opts->{ 'host' } || 'localhost';
  $self->{ 'port' } = $opts->{ 'port' } || '6379';
  $self->{ 'password' } = $opts->{ 'password' };

  if ( defined( $opts->{ 'encoding' } ) ) {
    $self->{ 'encoding' } = find_encoding( $opts->{ 'encoding' } );

    if ( !defined( $self->{ 'encoding' } ) ) {
      croak "Encoding \"$opts->{ 'encoding' }\" not found.";
    }
  }

  $self->{ 'reconnect' } = $opts->{ 'reconnect' };

  if ( $self->{ 'reconnect' } ) {

    if ( defined( $opts->{ 'reconnect_after' } ) ) {

      if ( looks_like_number( $opts->{ 'reconnect_after' } ) 
        && $opts->{ 'reconnect_after' } > 0 ) {

        $self->{ 'reconnect_after' } = $opts->{ 'reconnect_after' };
      }
      else {
        croak '"reconnect_interval" must be a positive number';
      }
    }
    else {
      $self->{ 'reconnect_after' } = 5;
    }

    if ( defined( $opts->{ 'max_reconnect_attempts' } ) ) {

      if ( looks_like_number( $opts->{ 'max_reconnect_attempts' } ) 
        && $opts->{ 'max_reconnect_attempts' } > 0 ) {

        $self->{ 'max_reconnect_attempts' } = $opts->{ 'max_reconnect_attempts' };
      }
      else {
        croak '"max_reconnect_attempts" must be a positive number';
      }
    }
  }

  if ( defined( $opts->{ 'on_connect' } ) ) {

    if ( ref( $opts->{ 'on_connect' } ) eq 'CODE' ) {
      $self->{ 'on_connect' } = $opts->{ 'on_connect' };
    }
    else {
      croak '"on_connect" callback must be a CODE reference';
    }
  }

  if ( defined( $opts->{ 'on_auth' } ) ) {

    if ( ref( $opts->{ 'on_auth' } ) eq 'CODE' ) {
      $self->{ 'on_auth' } = $opts->{ 'on_auth' };
    }
    else {
      croak '"on_auth" callback must be a CODE reference';
    }
  }

  if ( defined( $opts->{ 'on_stop_reconnect' } ) ) {

    if ( ref( $opts->{ 'on_stop_reconnect' } ) eq 'CODE' ) {
      $self->{ 'on_stop_reconnect' } = $opts->{ 'on_stop_reconnect' };
    }
    else {
      croak '"on_stop_reconnect" callback must be a CODE reference';
    }
  }

  if ( defined( $opts->{ 'on_error' } ) ) {

    if ( ref( $opts->{ 'on_error' } ) eq 'CODE' ) {
      $self->{ 'on_error' } = $opts->{ 'on_error' };
    }
    else {
      croak '"on_error" callback must be a CODE reference';
    }
  }
  else {
    $self->{ 'on_error' } = sub { warn "$_[ 0 ]\n" };
  }

  $self->{ 'handle' } = undef;
  $self->{ 'reconnect_attempt' } = 0;
  $self->{ 'commands_queue' } = [];
  $self->{ 'transac_begun' } = 0;
  $self->{ 'subscr_active' } = 0;
  $self->{ 'subscrs' } = {};
  $self->{ 'psubscrs' } = {};

  $self->_connect();

  return $self;
}


# Public methods

####
sub reconnect {
  my $self = shift;

  $self->disconnect();

  my $after = ( $self->{ 'reconnect_attempt' } > 0 ) ? $self->{ 'reconnect_after' } : 0;

  my $timer;

  $timer = AnyEvent->timer(
    after => $after,
    cb => sub {
      undef( $timer );

      ++$self->{ 'reconnect_attempt' };

      $self->_connect();
    }
  );
  
  return 1;
}

####
sub disconnect {
  my $self = shift;

  if ( defined( $self->{ 'handle' } ) && !$self->{ 'handle' }->destroyed() ) {
    $self->{ 'handle' }->destroy();
  }
  
  undef( $self->{ 'handle' } );
  $self->{ 'transac_begun' } = 0;
  $self->{ 'subscr_active' } = 0;

  return 1;
}


# Private methods

####
sub _connect {
  my $self = shift;
  
  $self->{ 'handle' } = AnyEvent::Handle->new(
    connect => [ $self->{ 'host' }, $self->{ 'port' } ],
    keepalive => 1,

    on_connect => sub {
      return $self->_post_connect();
    },
    
    on_connect_error => sub {
      $self->{ 'on_error' }->( $_[ 1 ], $ERR_CONNECT );
      $self->_attempt_to_reconnect();
    },

    on_error => sub {
      $self->{ 'on_error' }->( $_[ 2 ], $ERR_HANDLE );
      $self->_attempt_to_reconnect();
    },

    on_eof => sub {
      $self->{ 'on_error' }->( 'End-of-file detected', $ERR_EOF );
      $self->_attempt_to_reconnect();
    },

    on_read => sub {
      my $hdl = shift;

      return $self->_on_read( $hdl );
    }
  );

  if ( defined( $self->{ 'password' } ) && $self->{ 'password' } ne '' ) {
    $self->_execute_command( 'auth', $self->{ 'password' }, {
      failover => 0,
      cb => sub {

        if ( defined( $self->{ 'on_auth' } ) ) {
          my $data = shift;

          $self->{ 'on_auth' }->( $data );
        }

        return 1;
      }
    } );
  }

#  my $cmd_queue = $self->{ 'commands_queue' };
#  $self->{ 'commands_queue' } = [];
#
#  foreach my $cmd_args ( @{ $cmd_queue } ) {
#    $self->_execute_command( @{ $cmd_args } );
#  }
#  
#  foreach my $ch ( keys( %{ $self->{ 'failover_subs' } } ) ) {
#    $self->_subscribe( @{ $self->{ 'failover_subs' }->{ $ch } } );
#  }

  return 1;
}

####
sub _post_connect {
  my $self = shift;

  if ( defined( $self->{ 'on_connect' } ) ) {
    $self->{ 'on_connect' }->( $self->{ 'reconnect_attempt' } );
  }

  $self->{ 'reconnect_attempt' } = 0;

  return 1;
}

####
sub _attempt_to_reconnect {
  my $self = shift;
  
  if ( $self->{ 'reconnect' } && ( !defined( $self->{ 'max_reconnect_attempts' } )
    || $self->{ 'reconnect_attempt' } < $self->{ 'max_reconnect_attempts' } ) ) {

    $self->reconnect();
  }
  else {

    if ( defined( $self->{ 'on_stop_reconnect' } ) ) {
      $self->{ 'on_stop_reconnect' }->();
    }
  }
}

####
sub _execute_command {
  my $self = shift;
  my $cmd_name = shift;

  my $params = {};

  if ( ref( $_[ -1 ] ) eq 'HASH' ) {
    $params = pop( @_ );
  }

  my @args = @_;

  if ( !defined( $params->{ 'failover' } ) ) {
    $params->{ 'failover' } = 1;
  }

  if ( defined( $params->{ 'cb' } ) && ref( $params->{ 'cb' } ) ne 'CODE' ) {
    croak 'Callback must be a CODE reference';
  }

  my $cmd_q = $self->{ 'commands_queue' };

  my $cmd_params = {
    cmd_name => $cmd_name,
    args => \@args,
    failover => $params->{ 'failover' }
  };

  if ( $self->_is_pubsub_command( $cmd_name ) ) {
    $cmd_params->{ 'proc_cnt' } = scalar( @args );

    if ( defined( $params->{ 'on_subscribe' } ) ) {
      $cmd_params->{ 'on_subscribe' } = $params->{ 'on_subscribe' };
    }

    if ( defined( $params->{ 'on_message' } ) ) {
      $cmd_params->{ 'on_message' } = $params->{ 'on_message' };
    }
  }
  else {
    $cmd_params->{ 'proc_cnt' } = 1;
    
    if ( defined( $params->{ 'cb' } ) ) {
      $cmd_params->{ 'cb' } = $params->{ 'cb' };
    }

    push( @{ $cmd_q }, $cmd_params );
  } 

  my $cmd = $self->_prepare_command( $cmd_name, @args );
  $self->{ 'handle' }->push_write( $cmd );

  return 1;
}

####
sub _on_read {
  my $self = shift;
  my $hdl = shift;
 
  # TODO Обрабатывать WATCH и MULTI

#*3 
#$7
#message
#$3
#ch1
#$6
#232234

#*4
#$8
#pmessage
#$3
#ch*
#$3
#ch5
#$6
#ddfsdf

  $self->{ 'handle' }->push_read( 
    line => $self->_process_response( sub {
      my $data = shift;
      my $err = shift;

      my $cmd = shift( @{ $self->{ 'commands_queue' } } );

      if ( exists( $cmd->{ 'cb' } ) ) {
        $cmd->{ 'cb' }->( $data );
      }
    } ) 
  );
 
  return 1;
}

####
sub _is_pubsub_command {
  my $cmd_name = $_[ 1 ];

  return $cmd_name eq 'subscribe' || $cmd_name eq 'psubscribe' 
      || $cmd_name eq 'unsubscribe' || $cmd_name eq 'punsubscribe';
}

####
sub _prepare_command {
  my $self = shift;
  my @tkns = @_;

  my $cmd = '*' . scalar( @tkns ) . $EOL;

  foreach my $tkn ( @tkns ) {

    if ( $self->{ 'encoding' } && is_utf8( $tkn ) ) {
      $tkn = $self->{ 'encoding' }->encode( $tkn );
    }

    if ( defined( $tkn ) ) {
      $cmd .= '$' . length( $tkn ) . $EOL . $tkn . $EOL;
    }
    else {
      $cmd .= '$-1' . $EOL;
    }
  }

  return $cmd;
}

####
sub _process_response {
  my $self = shift;
  my $cb = shift;

  return sub {
    my $hdl = shift;
    my $str = shift;

    if ( defined( $str ) && $str ne '' ) {
      my $type = substr( $str, 0, 1 );
      my $val = substr( $str, 1 );

      if ( $type eq '+' || $type eq ':' ) {
        $cb->( $val );
      }
      elsif ( $type eq '-' ) {
        $cb->( $val, 1 );
      }
      elsif ( $type eq '$' ) {
        my $bulk_len = $val;

        if ( $bulk_len >= 0 ) {
          $hdl->unshift_read( chunk => $bulk_len + length( $EOL ), sub {
            my $data = $_[ 1 ];

            local $/ = $EOL;
            chomp( $data );

            if ( $self->{ 'encoding' } ) {
              $data = $self->{ 'encoding' }->decode( $data );
            }

            $cb->( $data );

            return 1;
          } );
        }
        else {
          $cb->( undef );
        }
      }
      elsif ( $type eq '*' ) {
        my $args_num = $val;

        if ( $args_num > 0 ) {
          my @mbulk_data = ();
          my $mbulk_len = 0;

          my $arg_cb = sub {
            my $data = shift;

            push( @mbulk_data, $data );

            if ( ++$mbulk_len == $args_num ) {
              $cb->( \@mbulk_data );
            }

            return 1;
          };

          for ( my $i = 0; $i < $args_num; $i++ ) {
            $hdl->unshift_read( line => $self->_process_response( $arg_cb ) );
          }
        }
        elsif ( $args_num == 0 ) {
          $cb->( [] );
        }
        else {
          $cb->();
        }
      }
      else {
        $self->{ 'on_error' }->( "Unexpected response type: \"$type\"", $ERR_PARSING );
        $self->_attempt_to_reconnect();

        return;
      }
    }
    else {
      $cb->();
    }

    return 1;
  };
}

####
sub AUTOLOAD {
  my $self = shift;

  our $AUTOLOAD;

  my $cmd_name = $AUTOLOAD;
  $cmd_name =~ s/^.+:://o;
  $cmd_name = lc( $cmd_name );

  $self->_execute_command( $cmd_name, @_ );

  return 1;
}

####
sub DESTROY {
  my $self = shift;

  $self->disconnect();

  return 1;
};

1;
__END__

=head1 NAME

AnyEvent::RipeRedis - Non-blocking Redis client with self reconnect feature on
loss connection

=head1 SYNOPSIS

  use AnyEvent::RipeRedis;

=head1 DESCRIPTION

This module is an AnyEvent user, you need to make sure that you use and run a
supported event loop.

AnyEvent::RipeRedis is non-blocking Redis client with self reconnect feature on
loss connection. If connection lost or some socket error is occurr, module try
re-connect, and re-execute not finished commands.

Module requires Redis 1.2 or higher.

=head1 METHODS

=head1 SEE ALSO

Redis, AnyEvent::Redis, AnyEvent

=head1 AUTHOR

Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2011, Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>. All rights reserved.

This module is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut
