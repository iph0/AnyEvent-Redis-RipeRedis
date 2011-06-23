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
  commands_queue
  reconnect_attempt
);

our $VERSION = '0.012010';

use AnyEvent;
use AnyEvent::Handle;
use Encode qw( find_encoding is_utf8 );
use Scalar::Util qw( looks_like_number );
use Carp 'croak';

our $EOL = "\r\n";
our $ERR_HANDLE = 0;
our $ERR_PROTOCOL = 1;
our $ERR_COMMAND = 2;


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

  if ( $opts->{ 'encoding' } ) {
    $self->{ 'encoding' } = find_encoding( $opts->{ 'encoding' } );

    if ( !$self->{ 'encoding' } ) {
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
    $self->{ 'on_error' } = sub { die "$_[ 0 ]\n" };
  }

  $self->{ 'handle' } = undef;
  $self->{ 'commands_queue' } = [];
  $self->{ 'reconnect_attempt' } = 0;

  $self->_connect();

  return $self;
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
      my $msg = $_[ 1 ];

      $self->_error( $msg, $ERR_HANDLE );

      return 1;
    },

    on_error => sub {
      my $msg = $_[ 2 ];

      $self->_error( $msg, $ERR_HANDLE );

      return 1;
    }
  );

  if ( $self->{ 'password' } ) {
    $self->_execute_command( 'auth', $self->{ 'password' },
      sub {
        
        if ( defined( $self->{ 'on_auth' } ) ) {
          my $data = shift;

          $self->{ 'on_auth' }->( $data );
        }
      }
    );
  }

  foreach my $cmd_args ( @{ $self->{ 'commands_queue' } } ) {
    $self->_execute_command( @{ $cmd_args } );
  }

  return 1;
}

####
sub _post_connect {
  my $self = shift;

  if ( defined( $self->{ 'on_connect' } ) ) {
    my $connect_attempt = $self->{ 'reconnect_attempt' } ? $self->{ 'reconnect_attempt' } : 1;
    $self->{ 'on_connect' }->( $connect_attempt );
  }

  $self->{ 'reconnect_attempt' } = 0;

  return 1;
}

####
sub _error {
  my $self = shift;
  my $msg = shift;
  my $type = shift;

  $self->{ 'on_error' }->( $msg, $type );

  if ( $type == $ERR_COMMAND ) {
    shift( @{ $self->{ 'commands_queue' } } );
  }
  else {
    $self->{ 'handle' }->destroy();

    if ( $self->{ 'reconnect' } && ( !defined( $self->{ 'max_reconnect_attempts' } )
      || $self->{ 'reconnect_attempt' } < $self->{ 'max_reconnect_attempts' } ) ) {

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
    }
    else {

      if ( $self->{ 'on_stop_reconnect' } ) {
        $self->{ 'on_stop_reconnect' }->();
      }
    }
  }

  return 1;
}

####
sub _execute_command {
  my $self = shift;

  my $cb = pop( @_ );
  my @args = @_;

  my $args_num = scalar( @args );
  my $cmd = '*' . $args_num . $EOL;

  foreach my $arg ( @args ) {

    if ( $self->{ 'encoding' } && is_utf8( $arg ) ) {
      $arg = $self->{ 'encoding' }->encode( $arg );
    }
    
    if ( defined( $arg ) ) {
      my $arg_len = length( $arg );
      $cmd .= '$' . $arg_len . $EOL . $arg . $EOL;
    }
    else {
      $cmd .= '$-1' . $EOL;
    }
  }

  $self->{ 'handle' }->push_write( $cmd );

  if ( $args[ 0 ] =~ m/^p?subscribe$/io ) {
    $self->{ 'handle' }->on_read( sub {
      $self->{ 'handle' }->push_read( line => $self->_read_response( $cb ) );
    } );
  }
  elsif ( $args[ 0 ] =~ m/^p?unsubscribe$/io ) {
    $self->{ 'handle' }->on_read( undef );
  }

  $self->{ 'handle' }->push_read( line => $self->_read_response( $cb ) );

  return 1;
}

####
sub _read_response {
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
        $self->_error( $val, $ERR_COMMAND );
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

          my $read_arg = sub {
            my $data = shift;

            push( @mbulk_data, $data );

            if ( ++$mbulk_len == $args_num ) {
              $cb->( \@mbulk_data );
            }

            return 1;
          };

          for ( my $i = 0; $i < $args_num; $i++ ) {
            $hdl->unshift_read( line => $self->_read_response( $read_arg ) );
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
        $self->_error( "Unknown response type: \"$type\"", $ERR_PROTOCOL );
      }
    }
    else {
      $cb->( undef );
    }

    return 1;
  };
}

####
sub AUTOLOAD {
  my $self = shift;

  my $params = {};

  if ( ref( $_[ -1 ] ) eq 'HASH' ) {
    $params = pop( @_ );
  }

  my @args = @_;

  our $AUTOLOAD;

  my $cmd = $AUTOLOAD;
  $cmd =~ s/^.+:://o;

  unshift( @args, $cmd );

  if ( defined( $params->{ 'cb' } ) && ref( $params->{ 'cb' } ) ne 'CODE' ) {
    croak 'Callback must be a CODE reference';
  }

  my $retry = 1;

  if ( defined( $params->{ 'retry' } ) ) {
    $retry = $params->{ 'retry' };
  }

  my $cb = sub {

    if ( defined( $params->{ 'cb' } ) ) {
      my $data = shift;

      $params->{ 'cb' }->( $data );
    }

    if ( $retry ) {
      shift( @{ $self->{ 'commands_queue' } } );
    }

    return 1;
  };

  if ( $retry ) {
    push( @{ $self->{ 'commands_queue' } }, [ @args, $cb ] );
  }

  $self->_execute_command( @args, $cb );

  return 1;
}

####
sub DESTROY {
  my $self = shift;
  
  if ( defined( $self->{ 'handle' } ) && !$self->{ 'handle' }->destroyed() ) {
    $self->{ 'handle' }->destroy();
  }

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
