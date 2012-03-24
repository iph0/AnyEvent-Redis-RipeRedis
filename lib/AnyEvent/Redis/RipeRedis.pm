package AnyEvent::Redis::RipeRedis;

use 5.006000;
use strict;
use warnings;

use fields qw(
  host
  port
  encoding
  reconnect
  reconnect_after
  max_connect_attempts
  on_connect
  on_stop_reconnect
  on_connect_error
  on_error
  handle
  connect_attempt
  command_queue
  sub_lock
  subs
);

our $VERSION = '0.700007';

use AnyEvent::Handle;
use Encode qw( find_encoding is_utf8 );
use Scalar::Util 'looks_like_number';
use Carp qw( croak confess );

my $DEFAULT = {
  host => 'localhost',
  port => '6379',
  reconnect_after => 5,
};

my $EOL = "\r\n";
my $EOL_LENGTH = length( $EOL );


# Constructor
sub new {
  my $proto = shift;
  my $params = { @_ };

  my __PACKAGE__ $self = fields::new( $proto );

  $params = $self->_validate_new( $params );

  my @keys = keys( %{ $params } );
  @{ $self }{ @keys } = @{ $params }{ @keys };
  $self->{handle} = undef;
  $self->{connect_attempt} = 0;
  $self->{command_queue} = [];
  $self->{sub_lock} = undef;
  $self->{subs} = {};

  $self->_connect();

  return $self;
}


# Private methods

####
sub _validate_new {
  my __PACKAGE__ $self = shift;
  my $params = shift;

  if ( defined( $params->{encoding} ) ) {
    my $enc = $params->{encoding};
    $params->{encoding} = find_encoding( $enc );

    if ( !defined( $params->{encoding} ) ) {
      croak "Encoding '$enc' not found";
    }
  }

  if ( $params->{reconnect} ) {
    if ( defined( $params->{reconnect_after} ) ) {
      if (
        !looks_like_number( $params->{reconnect_after} )
          || $params->{reconnect_after} <= 0
          ) {
        croak "'reconnect_after' must be a positive number";
      }
    }
    else {
      $params->{reconnect_after} = $DEFAULT->{reconnect_after};
    }

    if (
      defined( $params->{max_connect_attempts} )
        && ( $params->{max_connect_attempts} =~ m/[^0-9]/o )
        ) {
      croak "'max_connect_attempts' must be a positive integer number";
    }
  }

  foreach my $cb_name ( qw( on_connect on_stop_reconnect on_connect_error on_error ) ) {
    if ( defined( $params->{ $cb_name } ) && ref( $params->{ $cb_name } ) ne 'CODE' ) {
      croak "'$cb_name' callback must be a CODE reference";
    }
  }

  if ( !defined( $params->{on_error} ) ) {
    $params->{on_error} = sub {
      my $err = shift;
      warn "$err\n";
    };
  }

  return $params;
}

####
sub _connect {
  my __PACKAGE__ $self = shift;

  ++$self->{connect_attempt};

  $self->{handle} = AnyEvent::Handle->new(
    connect => [ $self->{host}, $self->{port} ],
    keepalive => 1,

    on_connect => sub {
      if ( defined( $self->{on_connect} ) ) {
        $self->{on_connect}->( $self->{connect_attempt} );
      }
      $self->{connect_attempt} = 0;
    },

    on_connect_error => sub {
      my $err = pop;

      undef( $self->{handle} );
      $err = "Can't connect to $self->{host}:$self->{port}; $err";
      if ( defined( $self->{on_connect_error} ) ) {
        $self->{on_connect_error}->( $err, $self->{connect_attempt} );
      }
      else {
        $self->{on_error}->( $err );
      }
      $self->_abort_all();
      $self->_try_to_reconnect();
    },

    on_error => sub {
      my $err = pop;

      undef( $self->{handle} );
      $self->{on_error}->( $err );
      $self->_abort_all();
      $self->_try_to_reconnect();
    },

    on_eof => sub {
      undef( $self->{handle} );
      $self->{on_error}->( 'Connection lost' );
      $self->_abort_all();
      $self->_try_to_reconnect();
    },

    on_read => $self->_prepare_read(
      sub {
        return $self->_prcoess_response( @_ );
      }
    ),
  );

  return;
}

####
sub _exec_command {
  my __PACKAGE__ $self = shift;
  my $cmd_name = shift;
  my @args = @_;
  my $params = {};
  if ( ref( $args[ -1 ] ) eq 'HASH' ) {
    $params = pop( @args );
  }

  $params = $self->_validate_exec_cmd( $params );

  my $cmd = {
    name => $cmd_name,
    args => \@args,
    %{ $params },
  };

  if ( $self->_is_sub_action_cmd( $cmd_name ) ) {
    if ( $self->{sub_lock} ) {
      croak "Command '$cmd_name' not allowed in this context."
          . " First, the transaction must be completed.";
    }
    $cmd->{resp_remaining} = scalar( @args );
  }
  elsif ( $cmd_name eq 'multi' ) {
    $self->{sub_lock} = 1;
  }
  elsif ( $cmd_name eq 'exec' ) {
    undef( $self->{sub_lock} );
  }

  if ( !defined( $self->{handle} ) ) {
    $cmd->{on_error}->( "Can't execute command '$cmd_name'."
        . " Connection not established" );

    return;
  }

  $self->_push_command( $cmd );

  return;
}

####
sub _validate_exec_cmd {
  my __PACKAGE__ $self = shift;
  my $params = shift;

  foreach my $cb_name ( qw( on_done on_message on_error ) ) {
    if ( defined( $params->{ $cb_name } ) && ref( $params->{ $cb_name } ) ne 'CODE' ) {
      croak "'$cb_name' callback must be a CODE reference";
    }
  }

  if ( !defined( $params->{on_error} ) ) {
    $params->{on_error} = $self->{on_error};
  }

  return $params;
}

####
sub _push_command {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;

  push( @{ $self->{command_queue} }, $cmd );
  my $cmd_szd = $self->_serialize_command( $cmd );
  $self->{handle}->push_write( $cmd_szd );

  return;
}

####
sub _serialize_command {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;

  my @args = grep { defined( $_ ) } @{ $cmd->{args} };
  my $bulk_len = scalar( @args ) + 1;
  my $cmd_szd = "*$bulk_len$EOL";
  foreach my $tkn ( $cmd->{name}, @args ) {
    if ( defined( $self->{encoding} ) && is_utf8( $tkn ) ) {
      $tkn = $self->{encoding}->encode( $tkn );
    }
    my $tkn_len =  length( $tkn );
    $cmd_szd .= "\$$tkn_len$EOL$tkn$EOL";
  }

  return $cmd_szd;
}

####
sub _prepare_read {
  my __PACKAGE__ $self = shift;
  my $cb = shift;

  my $bulk_len;

  return sub {
    my $hdl = shift;

    while ( 1 ) {
      if ( defined( $bulk_len ) ) {
        my $bulk_eol_len = $bulk_len + $EOL_LENGTH;
        if ( length( substr( $hdl->{rbuf}, 0, $bulk_eol_len ) ) == $bulk_eol_len ) {
          my $data = substr( $hdl->{rbuf}, 0, $bulk_len, '' );
          substr( $hdl->{rbuf}, 0, $EOL_LENGTH, '' );
          chomp( $data );
          if ( $self->{encoding} ) {
            $data = $self->{encoding}->decode( $data );
          }
          undef( $bulk_len );

          return 1 if $cb->( $data );
        }
        else {
          return;
        }
      }

      my $eol_pos = index( $hdl->{rbuf}, $EOL );

      if ( $eol_pos >= 0 ) {
        my $data = substr( $hdl->{rbuf}, 0, $eol_pos, '' );
        my $type = substr( $data, 0, 1, '' );
        substr( $hdl->{rbuf}, 0, $EOL_LENGTH, '' );

        if ( $type eq '+' || $type eq ':' ) {
          return 1 if $cb->( $data );
        }
        elsif ( $type eq '-' ) {
          return 1 if $cb->( $data, 1 );
        }
        elsif ( $type eq '$' ) {
          if ( $data > 0 ) {
            $bulk_len = $data;
          }
          else {
            return 1 if $cb->();
          }
        }
        elsif ( $type eq '*' ) {
          my $mbulk_len = $data;
          if ( $mbulk_len > 0 ) {
            $self->_unshift_read( $hdl, $mbulk_len, $cb );
            return 1;
          }
          elsif ( $mbulk_len < 0 ) {
            return 1 if $cb->();
          }
          else {
            return 1 if $cb->( [] );
          }
        }
      }
      else {
        return;
      }
    }
  };
}

####
sub _unshift_read {
  my __PACKAGE__ $self = shift;
  my $hdl = shift;
  my $mbulk_len = shift;
  my $cb = shift;

  my $read_cb;
  my @data_list;
  my @errors;

  my $remaining = $mbulk_len;

  my $cb_wrap = sub {
    my $data = shift;
    my $is_err = shift;

    if ( $is_err ) {
      push( @errors, $data );
    }
    else {
      push( @data_list, $data );
    }

    --$remaining;

    if ( ref( $data ) eq 'ARRAY' && @{ $data } && $remaining > 0 ) {
      $hdl->unshift_read( $read_cb );
    }
    elsif ( $remaining == 0 ) {
      undef( $read_cb );

      if ( @errors ) {
        my $err = join( "\n", @errors );
        $cb->( $err, 1 );
      }
      else {
        $cb->( \@data_list );
      }

      return 1;
    }
  };

  $read_cb = $self->_prepare_read( $cb_wrap );
  $hdl->unshift_read( $read_cb );

  return;
}

####
sub _prcoess_response {
  my __PACKAGE__ $self = shift;
  my $data = shift;
  my $is_err = shift;

  if ( $is_err ) {
    my $cmd = shift( @{ $self->{command_queue} } );
    if ( defined( $cmd ) ) {
      $cmd->{on_error}->( $data );
    }
    else {
      $self->{on_error}->( $data );
    }

    return;
  }

  if ( %{ $self->{subs} } && $self->_is_sub_message( $data ) ) {
    if ( exists( $self->{subs}{ $data->[ 1 ] } ) ) {
      return $self->_process_sub_message( $data );
    }
  }

  my $cmd = $self->{command_queue}[ 0 ];

  if ( !defined( $cmd ) ) {
    $self->{on_error}->( "Don't known how process response data."
      . " Command queue is empty" );
    return;
  }

  if ( $self->_is_sub_action_cmd( $cmd->{name} ) ) {
    return $self->_process_sub_action( $cmd, $data );
  }

  if ( defined( $cmd->{on_done} ) ) {
    $cmd->{on_done}->( $data );
  }

  shift( @{ $self->{command_queue} } );

  if ( $cmd->{name} eq 'quit' ) {
    undef( $self->{handle} );
    $self->_abort_all();
  }

  return;
}

####
sub _process_sub_action {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;
  my $data = shift;

  if ( $cmd->{name} eq 'subscribe' || $cmd->{name} eq 'psubscribe' ) {
    my $sub = {};
    if ( defined( $cmd->{on_done} ) ) {
      $sub->{on_done} = $cmd->{on_done};
      $sub->{on_done}->( $data->[ 1 ], $data->[ 2 ] );
    }
    if ( defined( $cmd->{on_message} ) ) {
      $sub->{on_message} = $cmd->{on_message};
    }
    $self->{subs}{ $data->[ 1 ] } = $sub;
  }
  else {
    if ( defined( $cmd->{on_done} ) ) {
      $cmd->{on_done}->( $data->[ 1 ], $data->[ 2 ] );
    }
    if ( exists( $self->{subs}{ $data->[ 1 ] } ) ) {
      delete( $self->{subs}{ $data->[ 1 ] } );
    }
  }

  if ( --$cmd->{resp_remaining} == 0 ) {
    shift( @{ $self->{command_queue} } );
  }

  return;
}

####
sub _process_sub_message {
  my __PACKAGE__ $self = shift;
  my $data = shift;

  my $sub = $self->{subs}{ $data->[ 1 ] };
  if ( exists( $sub->{on_message} ) ) {
    if ( $data->[ 0 ] eq 'message' ) {
      $sub->{on_message}->( $data->[ 1 ], $data->[ 2 ] );
    }
    else {
      $sub->{on_message}->( $data->[ 2 ], $data->[ 3 ], $data->[ 1 ] );
    }
  }

  return;
}

####
sub _abort_all {
  my __PACKAGE__ $self = shift;

  while ( my $cmd = shift( @{ $self->{command_queue} } ) ) {
    $cmd->{on_error}->( "Command '$cmd->{name}' failed" );
  }

  undef( $self->{sub_lock} );
  $self->{subs} = {};

  return;
}

####
sub _try_to_reconnect {
  my __PACKAGE__ $self = shift;

  if (
    $self->{reconnect}
      && ( !defined( $self->{max_connect_attempts} )
        || $self->{connect_attempt} < $self->{max_connect_attempts} )
      ) {
    $self->_reconnect();
  }
  else {
    if ( defined( $self->{on_stop_reconnect} ) ) {
      $self->{on_stop_reconnect}->();
    }
  }

  return;
}

####
sub _reconnect {
  my __PACKAGE__ $self = shift;

  if ( $self->{connect_attempt} > 0 ) {
    my $timer;
    $timer = AnyEvent->timer(
      after => $self->{reconnect_after},
      cb => sub {
        undef( $timer );
        $self->_connect();
      },
    );
  }
  else {
    $self->_connect();
  }

  return;
}

####
sub _is_sub_action_cmd {
  my $cmd_name = pop;
  return $cmd_name eq 'subscribe' || $cmd_name eq 'unsubscribe'
      || $cmd_name eq 'psubscribe' || $cmd_name eq 'punsubscribe';
}

####
sub _is_sub_message {
  my $data = pop;
  return ref( $data ) eq 'ARRAY' && ( $data->[ 0 ] eq 'message'
      || $data->[ 0 ] eq 'pmessage' );
}

####
sub AUTOLOAD {
  our $AUTOLOAD;
  my $cmd_name = $AUTOLOAD;
  $cmd_name =~ s/^.+:://o;
  $cmd_name = lc( $cmd_name );

  my $sub = sub {
    my __PACKAGE__ $self = shift;
    $self->_exec_command( $cmd_name, @_ );
  };

  do {
    no strict 'refs';
    *{ $AUTOLOAD } = $sub;
  };

  goto &{ $sub };
}

####
sub DESTROY {}

1;
__END__

=head1 NAME

AnyEvent::Redis::RipeRedis - Non-blocking Redis client with self reconnect
feature on loss connection

=head1 SYNOPSIS

  use AnyEvent::Redis::RipeRedis;

=head1 DESCRIPTION

=head1 SEE ALSO

Redis, AnyEvent::Redis, AnyEvent

=head1 AUTHOR

Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2012, Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>. All rights reserved.

This module is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut
