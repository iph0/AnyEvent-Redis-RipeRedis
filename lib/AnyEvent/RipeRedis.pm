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

    on_read => $self->_prepare_on_read_cb( sub {
      $self->_prcoess_response( @_ );
    } )
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
sub _prepare_on_read_cb {
  my $self = shift;
  my $cb = shift;
  my $mbulk_remaining = shift;

  my $bulk_eol_len;
  my @mbulk_data;
  my $on_read_cb;
  my $on_data_cb;

  if ( defined( $mbulk_remaining ) ) {
    $on_data_cb = sub {
      my $data = shift;

      push( @mbulk_data, $data );

      if ( --$mbulk_remaining == 0 ) {
        undef( $on_read_cb );

        $cb->( \@mbulk_data );

        return 1;
      }
    }
  }
  else {
    $on_data_cb = sub {
      my $data = shift;

      $cb->( $data );

      return;
    }
  }

  $on_read_cb = sub {
    my $hdl = shift;

    local $/ = $EOL;

    while ( 1 ) {

      if ( defined( $bulk_eol_len ) ) {

        if ( length( substr( $hdl->{ 'rbuf' }, 0, $bulk_eol_len ) ) == $bulk_eol_len ) {
          my $data = substr( $hdl->{ 'rbuf' }, 0, $bulk_eol_len, '' );
          chomp( $data );

          if ( $self->{ 'encoding' } ) {
            $data = $self->{ 'encoding' }->decode( $data );
          }

          undef( $bulk_eol_len );

          if ( $on_data_cb->( $data ) ) {
            return 1;
          }
        }
        else {
          return;
        }
      }

      my $eol_pos = index( $hdl->{ 'rbuf' }, $EOL );

      if ( $eol_pos >= 0 ) {
        my $line = substr( $hdl->{ 'rbuf' }, 0, $eol_pos + $EOL_LENGTH, '' );
        my $type = substr( $line, 0, 1, '' );
        chomp( $line );

        if ( $type eq '+' || $type eq ':' ) {

          if ( $on_data_cb->( $line ) ) {
            return 1;
          }
        }
        elsif ( $type eq '-' ) {
          $self->{ 'on_error' }->( $line, $ERR_COMMAND );

          if ( !defined( $mbulk_remaining ) ) {
            shift( @{ $self->{ 'commands_queue' } } );
          }
        }
        elsif ( $type eq '$' ) {
          my $bulk_len = $line;

          if ( $bulk_len > 0 ) {
            $bulk_eol_len = $bulk_len + $EOL_LENGTH;
          }
          else {

            if ( $on_data_cb->() ) {
              return 1;
            }
          }
        }
        elsif ( $type eq '*' ) {
          my $mbulk_len = $line;

          if ( $mbulk_len > 0 ) {

            if ( defined( $mbulk_remaining ) && $mbulk_remaining > 1 ) {
              $hdl->unshift_read( $on_read_cb );
            }

            $hdl->unshift_read( $self->_prepare_on_read_cb( $on_data_cb, $mbulk_len ) );

            return 1;
          }
          elsif ( $mbulk_len < 0 ) {

            if ( $on_data_cb->() ) {
              return 1;
            }
          }
          else {

            if ( $on_data_cb->( [] ) ) {
              return 1;
            }
          }
        }
      }
      else {
        return;
      }
    }
  };

  return $on_read_cb;
}

####
sub _prcoess_response {
  my $self = shift;
  my $data = shift;

  if ( %{ $self->{ 'subs' } } ) {

    if ( ref( $data ) eq 'ARRAY' ) {

      if ( $data->[ 0 ] eq 'message' ) {
        $self->{ 'subs' }->{ $data->[ 1 ] }->( $data->[ 2 ] );

        return 1;
      }
      elsif ( $data->[ 0 ] eq 'pmessage' ) {
        $self->{ 'subs' }->{ $data->[ 1 ] }->( $data->[ 3 ] );

        return 1;
      }
    }
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

  if ( $cmd->{ 'name' } eq 'subscribe' || $cmd->{ 'name' } eq 'psubscribe' ) {
    $self->{ 'subs' }->{ $data->[ 1 ] } = $cmd->{ 'on_message' };
  }
  else {
    delete( $self->{ 'subs' }->{ $data->[ 1 ] } );
  }

  if ( --$cmd->{ 'resp_remaining' } == 0 ) {
    shift( @{ $self->{ 'commands_queue' } } );
  }

  if ( exists( $cmd->{ 'cb' } ) ) {
    $cmd->{ 'cb' }->( $data->[ 2 ] );
  }
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
