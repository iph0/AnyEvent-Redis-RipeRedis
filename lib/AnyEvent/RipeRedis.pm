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
  subs
  subs_num
  on_read_is_set
  reconnect_attempt
  commands_queue
  multi_begun
  failover_subs
);

our $VERSION = '0.300010';

use AnyEvent;
use AnyEvent::Handle;
use Encode qw( find_encoding is_utf8 );
use Scalar::Util qw( looks_like_number );
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

      if ( looks_like_number( $opts->{ 'reconnect_after' } ) ) {
        $self->{ 'reconnect_after' } = abs( $opts->{ 'reconnect_after' } );
      }
      else {
        croak '"reconnect_interval" must be a number';
      }
    }
    else {
      $self->{ 'reconnect_after' } = 5;
    }

    if ( defined( $opts->{ 'max_reconnect_attempts' } ) ) {

      if ( looks_like_number( $opts->{ 'max_reconnect_attempts' } ) ) {
        $self->{ 'max_reconnect_attempts' } = abs( $opts->{ 'max_reconnect_attempts' } );
      }
      else {
        croak '"max_reconnect_attempts" must be a number';
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
  $self->{ 'subs' } = {};
  $self->{ 'subs_num' } = 0;
  $self->{ 'on_read_is_set' } = 0;
  $self->{ 'reconnect_attempt' } = 0;
  $self->{ 'commands_queue' } = [];
  $self->{ 'multi_begun' } = 0;
  $self->{ 'failover_subs' } = {};

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
  
  $self->{ 'multi_begun' } = 0;
  $self->{ 'subs' } = {};
  $self->{ 'subs_num' } = 0;
  $self->{ 'on_read_is_set' } = 0;

  return 1;
}


# Private methods

####
sub _connect {
  my $self = shift;
  
  $self->{ 'handle' } = AnyEvent::Handle->new(
    connect => [ $self->{ 'host' }, $self->{ 'port' } ],
    peername => $self->{ 'host' },
    keepalive => 1,

    on_connect => sub {
      $self->_post_connect();
    },
    
    on_connect_error => sub {
      $self->{ 'on_error' }->( $_[ 1 ], $ERR_CONNECT );
      $self->_attempt_to_reconnect();

      return 1;
    },

    on_error => sub {
      $self->{ 'on_error' }->( $_[ 2 ], $ERR_HANDLE );
      $self->_attempt_to_reconnect();

      return 1;
    },

    on_eof => sub {
      $self->{ 'on_error' }->( 'Unexpected end-of-file', $ERR_EOF );
      $self->_attempt_to_reconnect();

      return 1;
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

  my $cmd_queue = $self->{ 'commands_queue' };
  $self->{ 'commands_queue' } = [];

  foreach my $cmd_args ( @{ $cmd_queue } ) {
    $self->_execute_command( @{ $cmd_args } );
  }
  
  foreach my $ch ( keys( %{ $self->{ 'failover_subs' } } ) ) {
    $self->_subscribe( @{ $self->{ 'failover_subs' }->{ $ch } } );
  }

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
  my $cmd = shift;

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

  my $cb = sub {
    my $data = shift;
    my $err = shift;

    if ( $params->{ 'failover' } ) {

      if ( $cmd eq 'multi' && !$err ) {
        $self->{ 'multi_begun' } = 1;
      }
      elsif ( $cmd eq 'exec' && !$err ) {
        $self->{ 'multi_begun' } = 0;

        while( my $cmd_args = shift( @{ $self->{ 'commands_queue' } } ) ) {
          
          if ( $cmd_args->[ 0 ] eq 'exec' ) {
            last;
          }
        }
      }
      elsif ( !$self->{ 'multi_begun' } ) {
        shift( @{ $self->{ 'commands_queue' } } ); # TODO подумать об очереди транзакций [[]] !!!!!!
      }
    }

    if ( $err ) {
      $self->{ 'on_error' }->( $data, $ERR_COMMAND );
    }
    elsif ( defined( $params->{ 'cb' } ) ) {
      $params->{ 'cb' }->( $data );
    }

    return 1;
  };

  if ( $params->{ 'failover' } ) {
    push( @{ $self->{ 'commands_queue' } }, [ $cmd, @args, $params ] );
  }
  
  if ( !$self->{ 'handle' }->destroyed() ) {
    my $cmd_str = $self->_prepare_command( $cmd, @args );
    $self->{ 'handle' }->push_write( $cmd_str );
  }

  if ( !$self->{ 'handle' }->destroyed() ) {
    $self->{ 'handle' }->push_read( line => $self->_process_response( $cb ) );
  }

  return 1;
}

####
sub _subscribe {
  my $self = shift;
  my $cmd = lc( shift );

  my $params = {};

  if ( ref( $_[ -1 ] ) eq 'HASH' ) {
    $params = pop( @_ );
  }

  my @args = @_;

  if ( !defined( $params->{ 'failover' } ) ) {
    $params->{ 'failover' } = 1;
  }

  if ( !defined( $params->{ 'cb' } ) ) {
    croak 'You must specify callback';
  }
  if ( ref( $params->{ 'cb' } ) ne 'CODE' ) {
    croak 'Callback must be a CODE reference';
  }

  if ( $params->{ 'failover' } ) {

    foreach my $ch ( @args ) {
      $self->{ 'failover_subs' }->{ $ch } = [ $cmd, $ch, $params ];
    }
  }

  if ( !$self->{ 'handle' }->destroyed() ) {

    foreach my $ch ( @args ) {
      $self->{ 'subs' }->{ $ch } = $params->{ 'cb' };
    }

    $self->{ subs_num } += scalar( @args );

    my $cmd_str = $self->_prepare_command( $cmd, @args );
    $self->{ 'handle' }->push_write( $cmd_str );
  }

  if ( !$self->{ 'handle' }->destroyed() ) {
    if ( !$self->{ 'on_read_is_set' } ) {
      $self->{ 'handle' }->on_read(
        sub {
          $self->{ 'handle' }->push_read( line => $self->_process_response(
              $self->_process_pubsub() ) );
        }
      );

      $self->{ 'on_read_is_set' } = 1;
    }
  }

  return 1;
}

####
sub _unsubscribe {
  my $self = shift;
  my $cmd = shift;
  my @args = @_;

  if ( !$self->{ 'handle' }->destroyed() ) {
    my $cmd_str = $self->_prepare_command( $cmd, @args );
    $self->{ 'handle' }->push_write( $cmd_str );
  }

  # TODO repeat unsubscribe

  return 1;
}

####
sub _prepare_command {
  my $self = shift;
  my @tkns = @_;

  my $cmd_str = '*' . scalar( @tkns ) . $EOL;

  foreach my $tkn ( @tkns ) {

    if ( $self->{ 'encoding' } && is_utf8( $tkn ) ) {
      $tkn = $self->{ 'encoding' }->encode( $tkn );
    }

    if ( defined( $tkn ) ) {
      $cmd_str .= '$' . length( $tkn ) . $EOL . $tkn . $EOL;
    }
    else {
      $cmd_str .= '$-1' . $EOL;
    }
  }

  return $cmd_str;
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
          $cb->( undef );
        }
      }
      else {
        $self->{ 'on_error' }->( "Unexpected response type: \"$type\"", $ERR_PARSING );
        $self->_attempt_to_reconnect();

        return;
      }
    }
    else {
      $cb->( undef );
    }

    return 1;
  };
}

####
sub _process_pubsub {
  my $self = shift;

  return sub {
    my $data = shift;

    if ( !defined( $data ) ) {
      $self->{ 'on_error' }->( 'Pub/Sub data is undefined', $ERR_SUBS );

      return;
    }
    elsif ( ref( $data ) ne 'ARRAY' ) {
      $self->{ 'on_error' }->( 'Pub/Sub data must be a array reference', $ERR_SUBS );

      return;
    }

    my $action = $data->[ 0 ];

    if ( !defined( $action ) ) {
      $self->{ 'on_error' }->( 'Pub/Sub action is undefined', $ERR_SUBS );

      return;
    }

    my $ch = $data->[ 1 ];

    if ( !defined( $action ) ) {
      $self->{ 'on_error' }->( 'Pub/Sub channel name or pattern is undefined', $ERR_SUBS );

      return;
    }

    if ( $action eq 'subscribe' || $action eq 'psubscribe' ) {
      $self->{ 'subs' }->{ $ch }->( $data );
    }
    elsif ( $action eq 'unsubscribe' || $action eq 'punsubscribe' ) {
      my $del_cb = delete( $self->{ 'subs' }->{ $ch } );
      $self->{ 'subs_num' } = $data->[ 2 ];

      if ( $self->{ 'subs_num' } == 0 ) {
        $self->{ 'handle' }->on_read( undef );
        $self->{ 'on_read_is_set' } = 0;
      }

      $del_cb->( $data );
    }
    elsif ( $action eq 'message' || $action eq 'pmessage' ) {
      $self->{ 'subs' }->{ $ch }->( $data );
    }
    else {
      $self->{ 'on_error' }->( "Unexpected Pub/Sub action: \"$action\"", $ERR_SUBS );

      return;
    }

    return 1;
  };
}

####
sub AUTOLOAD {
  my $self = shift;

  our $AUTOLOAD;

  my $cmd = $AUTOLOAD;
  $cmd =~ s/^.+:://o;
  $cmd = lc( $cmd );

  if ( $cmd eq 'subscribe' || $cmd eq 'psubscribe' ) {
    $self->_subscribe( $cmd, @_ );
  }
  elsif ( $cmd eq 'unsubscribe' || $cmd eq 'punsubscribe' ) {
    $self->_unsubscribe( $cmd, @_ );
  }
  elsif ( $self->{ 'subs_num' } == 0 ) {
    $self->_execute_command( $cmd, @_ );
  }
  else {
    croak 'Only (P)SUBSCRIBE and (P)UNSUBSCRIBE commands allowed when'
        . ' subscription enabled.';
  }

  return 1;
}

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
