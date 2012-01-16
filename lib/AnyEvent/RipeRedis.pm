package AnyEvent::RipeRedis;

# TODO clean tail white spaces

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
  sub_lock
  subs
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
our $ERR_PROCESS = 5;

my $EOL = "\r\n";
my $EOL_LENGTH = length( $EOL );


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
  $self->{ 'sub_lock' } = undef;
  $self->{ 'subs' } = {};

  $self->_connect();

  return $self;
}


# Public methods

####
sub execute_command {
  my $self = shift;
  my $cmd_name = shift;

  my $params = {};

  if ( ref( $_[ -1 ] ) eq 'HASH' ) {
    $params = pop( @_ );
  }

  my @args = @_;

  my $cmd = {
    name => $cmd_name,
    args => \@args,
  };

  if ( defined( $params->{ 'cb' } ) ) {

    if ( ref( $params->{ 'cb' } ) ne 'CODE' ) {
      croak 'Callback must be a CODE reference';
    }

    $cmd->{ 'cb' } = $params->{ 'cb' };
  }

  if ( $cmd_name eq 'multi' ) {
    $self->{ 'sub_lock' } = 1;
  }
  elsif ( $cmd_name eq 'exec' ) {
    undef( $self->{ 'sub_lock' } );
  }
  elsif ( $cmd_name eq 'subscribe' || $cmd_name eq 'psubscribe'
      || $cmd_name eq 'unsubscribe' || $cmd_name eq 'punsubscribe' ) {

    if ( $self->{ 'sub_lock' } ) {
      croak "Command \"$cmd_name\" not allowed in this context."
          . " First, the transaction must be completed.";
    }

    $cmd->{ 'resp_remaining' } = scalar( @args );

    if ( $cmd_name eq 'subscribe' || $cmd_name eq 'psubscribe' ) {

      if ( !defined( $params->{ 'on_message' } ) ) {
        croak '"on_message" callback must be specified';
      }
      elsif ( ref( $params->{ 'on_message' } ) ne 'CODE' ) {
        croak '"on_message" callback must be a CODE reference';
      }

      $cmd->{ 'on_message' } = $params->{ 'on_message' };
    }
  }

  $self->_push_command( $cmd );

  return 1;
}

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

  $self->{ 'handle' } = undef;
  $self->{ 'reconnect_attempt' } = 0;
  $self->{ 'commands_queue' } = [];
  $self->{ 'sub_lock' } = undef;
  $self->{ 'subs' } = {};

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
      $self->{ 'on_error' }->( 'EOF detected', $ERR_EOF );
      $self->_attempt_to_reconnect();
    },

    on_read => $self->_prepare_read_cb( 
      sub { 
        $self->_prcoess_response( @_ );
      }
    )
  );

  if ( defined( $self->{ 'password' } ) && $self->{ 'password' } ne '' ) {
    $self->_push_command( {
      name => 'auth',
      args => [  $self->{ 'password' } ],

      cb => sub {

        if ( defined( $self->{ 'on_auth' } ) ) {
          my $data = shift;

          $self->{ 'on_auth' }->( $data );
        }
      }
    } );
  }
}

####
sub _post_connect {
  my $self = shift;

  $self->{ 'reconnect_attempt' } = 0;

  if ( defined( $self->{ 'on_connect' } ) ) {
    $self->{ 'on_connect' }->( $self->{ 'reconnect_attempt' } );
  }
}

####
sub _push_command {
  my $self = shift;
  my $cmd = shift;

  push( @{ $self->{ 'commands_queue' } }, $cmd );

  if ( defined( $self->{ 'handle' } ) ) {
    my $cmd_str = $self->_serialize_command( $cmd );
    $self->{ 'handle' }->push_write( $cmd_str );
  }
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
sub _serialize_command {
  my $self = shift;
  my $cmd = shift;

  my $bulk_len = scalar( @{ $cmd->{ 'args' } } ) + 1;
  my $cmd_str = "*$bulk_len$EOL";

  foreach my $tkn ( $cmd->{ 'name' }, @{ $cmd->{ 'args' } } ) {

    if ( exists( $self->{ 'encoding' } ) && is_utf8( $tkn ) ) {
      $tkn = $self->{ 'encoding' }->encode( $tkn );
    }

    if ( defined( $tkn ) ) {
      my $tkn_len =  length( $tkn );
      $cmd_str .= "\$$tkn_len$EOL$tkn$EOL";
    }
    else {
      $cmd_str .= "\$-1$EOL";
    }
  }

  return $cmd_str;
}

####
sub _prepare_read_cb {
  my $self = shift;
  my $input_cb = shift;
  my $remaining_items = shift;

  my $bulk_len;
  my $cb;
  my $read_cb;

  if ( defined( $remaining_items ) ) {
    $cb = sub {
      $input_cb->( @_ );

      if ( --$remaining_items == 0 ) {
        undef( $read_cb );
          
        return 1;
      }

      return;
    };
  }
  else {
    $cb = sub {
      $input_cb->( @_ );

      return;
    };
  }
  
  $read_cb = sub {
    my $hdl = shift;
    
    local $/ = $EOL;

    while ( 1 ) {
      
      if ( defined( $bulk_len ) ) {
        my $bulk_eol_len += $bulk_len + $EOL_LENGTH;

        if ( length( substr( $hdl->{ 'rbuf' }, 0, $bulk_eol_len ) ) == $bulk_eol_len ) {
          my $bulk_data = substr( $hdl->{ 'rbuf' }, 0, $bulk_eol_len, '' );
          chomp( $bulk_data );

          if ( $self->{ 'encoding' } ) {
            $bulk_data = $self->{ 'encoding' }->decode( $bulk_data );
          }
          
          undef( $bulk_len );

          $cb->( $bulk_data ) && return 1;
        }
        else {
          last;
        }
      }

      my $eol_pos = index( $hdl->{ 'rbuf' }, $EOL );

      if ( $eol_pos >= 0 ) {
        my $val = substr( $hdl->{ 'rbuf' }, 0, $eol_pos + $EOL_LENGTH, '' );
        my $type = substr( $val, 0, 1, '' );
        chomp( $val );
        
        if ( $type eq '+' || $type eq ':' ) {
          $cb->( $val ) && return 1;
        }
        elsif ( $type eq '-' ) {
          $cb->( $val, 1 ) && return 1;
        }
        elsif ( $type eq '$' ) {

          if ( $val > 0 ) {
            $bulk_len = $val;
          }
          else {
            $cb->( $val ) && return 1;
          }
        }
        elsif ( $type eq '*' ) {
          
          if ( $val > 0 ) {
            my @mbulk_data;

            my $mbulk_cb = sub {
              my $data = shift;
              my $err_ocurred = shift;

              if ( $err_ocurred ) {
                $self->{ 'on_error' }->( $data, $ERR_COMMAND );

                return;
              }
              
              push( @mbulk_data, $data );

              if ( scalar( @mbulk_data ) == $val ) {
                $cb->( \@mbulk_data );
              }
            };

            $hdl->push_read( $self->_prepare_read_cb( $mbulk_cb, $val ) );

            if ( defined( $remaining_items ) && $remaining_items > 1 ) {
              $hdl->push_read( $read_cb );
            }

            return 1;
          }
          elsif ( $val < 0 ) {
            $cb->() && return 1;
          }
          else {
            $cb->( [] ) && return 1;
          }
        }
        else {
          $self->{ 'on_error' }->( "Unexpected response type: \"$type\"", $ERR_PROCESS );
        }
      }
      else {
        last;
      }
    }
    
    return;
  };

  return $read_cb;
}

####
#sub _prepare_read_cb {
#  my $self = shift;
#  my $cb = shift;
#
#  return sub {
#    my $hdl = shift;
#    my $line = shift;
#
#    if ( !defined( $line ) || $line eq '' ) {
#      $cb->();
#    }
#
#    my $type = substr( $line, 0, 1 );
#    my $val = substr( $line, 1 );
#
#    if ( $type eq '+' || $type eq ':' ) {
#      $cb->( $val );
#    }
#    elsif ( $type eq '-' ) {
#      $cb->( $val, 1 );
#    }
#    elsif ( $type eq '$' ) {
#      my $bulk_len = $val;
#
#      if ( $bulk_len >= 0 ) {
#        $hdl->unshift_read( chunk => $bulk_len + length( $EOL ), sub {
#          my $data = $_[ 1 ];
#
#          local $/ = $EOL;
#          chomp( $data );
#
#          if ( $self->{ 'encoding' } ) {
#            $data = $self->{ 'encoding' }->decode( $data );
#          }
#
#          $cb->( $data );
#        } );
#      }
#      else {
#        $cb->();
#      }
#    }
#    elsif ( $type eq '*' ) {
#      my $args_num = $val;
#
#      if ( $args_num > 0 ) {
#        my @mbulk_data;
#        my $mbulk_len = 0;
#
#        my $arg_cb = sub {
#          my $data = shift;
#
#          push( @mbulk_data, $data );
#
#          if ( ++$mbulk_len == $args_num ) {
#            $cb->( \@mbulk_data );
#          }
#        };
#
#        for ( my $i = 0; $i < $args_num; $i++ ) {
#          $hdl->unshift_read( line => $self->_prepare_read_cb( $arg_cb ) );
#        }
#      }
#      elsif ( $args_num == 0 ) {
#        $cb->( [] );
#      }
#      else {
#        $cb->();
#      }
#    }
#    else {
#      $self->{ 'on_error' }->( "Unexpected response type: \"$type\"", $ERR_PROCESS );
#      $self->_attempt_to_reconnect();
#    }
#  };
#}

####
sub _prcoess_response {
  my $self = shift;
  my $data = shift;
  my $err_ocurred = shift;

  if ( $err_ocurred ) {
    $self->{ 'on_error' }->( $data, $ERR_COMMAND );

    shift( @{ $self->{ 'commands_queue' } } );

    return;
  }

  if ( %{ $self->{ 'subs' } } ) {
    my $msg;

    if ( $self->_looks_like_sub_msg( $data ) ) {
      $msg = $data->[ 2 ];
    }
    elsif ( $self->_looks_like_psub_msg( $data ) ) {
      $msg = $data->[ 3 ];
    }

    if ( defined( $msg ) ) {
      $self->{ 'subs' }->{ $data->[ 1 ] }->( $msg );

      return 1;
    }
  }

  if ( !@{ $self->{ 'commands_queue' } } ) {
    $self->{ 'on_error' }->( 'Do not know how process response. '
        . 'Queue of commands is empty', $ERR_PROCESS );

    return;
  }

  my $cmd = $self->{ 'commands_queue' }->[ 0 ];

  if ( $cmd->{ 'name' } eq 'subscribe' || $cmd->{ 'name' } eq 'psubscribe'
      || $cmd->{ 'name' } eq 'unsubscribe' || $cmd->{ 'name' } eq 'punsubscribe' ) {

    $self->_process_sub( $cmd, $data );

    return 1;
  }

  if ( exists( $cmd->{ 'cb' } ) ) {
    $cmd->{ 'cb' }->( $data );
  }

  if ( $cmd->{ 'name' } eq 'quit' ) {
    $self->disconnect();
  }

  shift( @{ $self->{ 'commands_queue' } } );
}

####
sub _process_sub {
  my $self = shift;
  my $cmd = shift;
  my $data = shift;

  if ( !( ref( $data ) eq 'ARRAY' && scalar( @{ $data } ) == 3
      && $data->[ 0 ] eq $cmd->{ 'name' } && !ref( $data->[ 1 ] )
      && $data->[ 1 ] ~~ $cmd->{ 'args' } && looks_like_number( $data->[ 2 ] ) ) ) {

    $self->{ 'on_error' }->( "Unexpected response to a \"$cmd->{ 'name' }\" command", $ERR_PROCESS );

    return;
  }

  if ( $cmd->{ 'name' } eq 'subscribe' || $cmd->{ 'name' } eq 'psubscribe' ) {
    $self->{ 'subs' }->{ $data->[ 1 ] } = $cmd->{ 'on_message' };
  }
  else {
    delete( $self->{ 'subs' }->{ $data->[ 1 ] } );
  }

  --$cmd->{ 'resp_remaining' };

  if ( $cmd->{ 'resp_remaining' } == 0 ) {
    shift( @{ $self->{ 'commands_queue' } } );
  }

  if ( exists( $cmd->{ 'cb' } ) ) {
    $cmd->{ 'cb' }->( $data->[ 2 ] );
  }
}

####
sub _looks_like_sub_msg {
  my $data = $_[ 1 ];

  return ref( $data ) eq 'ARRAY' && scalar( @{ $data } ) == 3
      && $data->[ 0 ] eq 'message' && !ref( $data->[ 1 ] ) && !ref( $data->[ 2 ] );
}

####
sub _looks_like_psub_msg {
  my $data = $_[ 1 ];

  return ref( $data ) eq 'ARRAY' && scalar( @{ $data } ) == 4
      && $data->[ 0 ] eq 'pmessage' && !ref( $data->[ 1 ] ) && !ref( $data->[ 2 ] )
      && !ref( $data->[ 3 ] );
}


####
sub AUTOLOAD {
  our $AUTOLOAD;

  my $cmd_name = $AUTOLOAD;
  $cmd_name =~ s/^.+:://o;
  $cmd_name = lc( $cmd_name );

  my $sub = sub {
    my $self = shift;

    $self->execute_command( $cmd_name, @_ );
  };

  do {
    no strict 'refs';

    *{ $AUTOLOAD } = $sub;
  };

  goto &{ $sub };
}


####
sub DESTROY {
  my $self = shift;

  $self->disconnect();
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
