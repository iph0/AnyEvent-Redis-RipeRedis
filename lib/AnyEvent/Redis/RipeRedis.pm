package AnyEvent::Redis::RipeRedis;

use 5.006000;
use strict;
use warnings;
use fields qw(
  host
  port
  password
  need_auth
  connection_timeout
  reconnect
  encoding
  on_connect
  on_disconnect
  on_error
  handle
  command_queue
  sub_lock
  subs
);

our $VERSION = '0.805300';

use AnyEvent::Handle;
use Encode qw( find_encoding is_utf8 );
use Scalar::Util 'looks_like_number';
use Carp 'croak';

my %DEFAULT = (
  host => 'localhost',
  port => '6379',
);

my %SUB_ACTION_CMDS = (
  subscribe => 1,
  psubscribe => 1,
  unsubscribe => 1,
  punsubscribe => 1,
);

my $EOL = "\r\n";
my $EOL_LEN = length( $EOL );


# Constructor
sub new {
  my $proto = shift;
  my $params = { @_ };

  my __PACKAGE__ $self = fields::new( $proto );

  $params = $self->_validate_new( $params );

  $self->{host} = $params->{host};
  $self->{port} = $params->{port};
  if ( defined( $params->{password} ) && $params->{password} ne '' ) {
    $self->{password} = $params->{password};
    $self->{need_auth} = 1;
  }
  $self->{connection_timeout} = $params->{connection_timeout};
  $self->{reconnect} = $params->{reconnect};
  $self->{encoding} = $params->{encoding};
  $self->{on_connect} = $params->{on_connect};
  $self->{on_disconnect} = $params->{on_disconnect};
  $self->{on_error} = $params->{on_error};
  $self->{handle} = undef;
  $self->{command_queue} = [];
  $self->{sub_lock} = undef;
  $self->{subs} = {};

  $self->_connect();
  if ( $self->{need_auth} ) {
    $self->_auth();
  }

  return $self;
}


# Private methods

####
sub _validate_new {
  my __PACKAGE__ $self = shift;
  my $params = shift;

  if ( !defined( $params->{host} ) || $params->{host} eq '' ) {
    $params->{host} = $DEFAULT{host};
  }
  if ( !defined( $params->{port} ) || $params->{port} eq '' ) {
    $params->{port} = $DEFAULT{port};
  }
  if (
    defined( $params->{connection_timeout} )
      && ( !looks_like_number( $params->{connection_timeout} )
        || $params->{connection_timeout} < 0 )
      ) {
    croak 'Connection timeout must be a positive number';
  }
  if ( !defined( $params->{reconnect} ) ) {
    $params->{reconnect} = 1;
  }
  if ( defined( $params->{encoding} ) ) {
    my $enc = $params->{encoding};
    $params->{encoding} = find_encoding( $enc );

    if ( !defined( $params->{encoding} ) ) {
      croak "Encoding '$enc' not found";
    }
  }
  foreach my $cb_name ( qw( on_connect on_disconnect on_error ) ) {
    if (
      defined( $params->{$cb_name} )
        && ref( $params->{$cb_name} ) ne 'CODE'
        ) {
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

  my %hdl_params = (
    connect => [ $self->{host}, $self->{port} ],
    keepalive => 1,

    on_connect_error => sub {
      my $err = pop;

      $self->_reset_handle();
      $err = "Can't connect to $self->{host}:$self->{port}. $err";
      $self->{on_error}->( $err );
      $self->_abort_commands( $err );
    },

    on_eof => sub {
      $self->_reset_handle();
      if ( defined( $self->{on_disconnect} ) ) {
        $self->{on_disconnect}->();
      }
      $self->_abort_commands( 'Connection closed by remote host' );
    },

    on_error => sub {
      my $err = pop;

      $self->_reset_handle();
      $self->{on_error}->( $err );
      if ( defined( $self->{on_disconnect} ) ) {
        $self->{on_disconnect}->();
      }
      $self->_abort_commands( $err );
    },

    on_read => $self->_prepare_read_cb(
      sub {
        return $self->_prcoess_response( @_ );
      }
    ),
  );
  if ( defined( $self->{connection_timeout} ) ) {
    $hdl_params{on_prepare} = sub {
      return $self->{connection_timeout};
    };
  }
  if ( defined( $self->{on_connect} ) ) {
    $hdl_params{on_connect} = sub {
      $self->{on_connect}->();
    };
  }
  $self->{handle} = AnyEvent::Handle->new( %hdl_params );

  return;
}

####
sub _auth {
  my __PACKAGE__ $self = shift;

  undef( $self->{need_auth} );
  $self->_push_command( {
    name => 'auth',
    args => [ $self->{password} ],

    on_error => sub {
      my $err = shift;

      $self->{need_auth} = 1;
      $self->{on_error}->( $err );
    },
  } );

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
    %{$params},
  };
  if ( exists( $SUB_ACTION_CMDS{$cmd_name} ) ) {
    if ( $self->{sub_lock} ) {
      croak "Command '$cmd_name' not allowed in this context."
          . ' First, the transaction must be completed';
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
    if ( $self->{reconnect} ) {
      $self->_connect();
    }
    else {
      $cmd->{on_error}->( "Can't execute command '$cmd_name'."
          . " Connection not established" );
      return;
    }
  }
  if ( $self->{need_auth} ) {
    $self->_auth();
  }

  $self->_push_command( $cmd );

  return;
}

####
sub _validate_exec_cmd {
  my __PACKAGE__ $self = shift;
  my $params = shift;

  foreach my $cb_name ( qw( on_done on_message on_error ) ) {
    if (
      defined( $params->{$cb_name} )
        && ref( $params->{$cb_name} ) ne 'CODE'
        ) {
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

  push( @{$self->{command_queue}}, $cmd );

  my $cmd_szd = '';
  my $m_bulk_len = 0;
  foreach my $tkn ( $cmd->{name}, @{$cmd->{args}} ) {
    if ( defined( $tkn ) ) {
      if ( defined( $self->{encoding} ) && is_utf8( $tkn ) ) {
        $tkn = $self->{encoding}->encode( $tkn );
      }
      my $tkn_len = length( $tkn );
      $cmd_szd .= "\$$tkn_len$EOL$tkn$EOL";
      ++$m_bulk_len;
    }
  }
  $cmd_szd = "*$m_bulk_len$EOL$cmd_szd";

  $self->{handle}->push_write( $cmd_szd );

  return;
}

####
sub _prepare_read_cb {
  my __PACKAGE__ $self = shift;
  my $cb = shift;

  my $bulk_len;

  return sub {
    my $hdl = shift;

    while ( 1 ) {
      if ( defined( $bulk_len ) ) {
        my $bulk_eol_len = $bulk_len + $EOL_LEN;
        if (
          length( substr( $hdl->{rbuf}, 0, $bulk_eol_len ) )
              == $bulk_eol_len
            ) {
          my $data = substr( $hdl->{rbuf}, 0, $bulk_len, '' );
          substr( $hdl->{rbuf}, 0, $EOL_LEN, '' );
          chomp( $data );
          if ( defined( $self->{encoding} ) ) {
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
        substr( $hdl->{rbuf}, 0, $EOL_LEN, '' );

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
          my $m_bulk_len = $data;
          if ( $m_bulk_len > 0 ) {
            $self->_unshift_read_cb( $hdl, $m_bulk_len, $cb );
            return 1;
          }
          elsif ( $m_bulk_len < 0 ) {
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
sub _unshift_read_cb {
  my __PACKAGE__ $self = shift;
  my $hdl = shift;
  my $m_bulk_len = shift;
  my $cb = shift;

  my $read_cb;
  my @data;
  my @errors;
  my $remaining_num = $m_bulk_len;
  my $cb_wrap = sub {
    my $data_chunk = shift;
    my $is_err = shift;

    if ( $is_err ) {
      push( @errors, $data_chunk );
    }
    else {
      push( @data, $data_chunk );
    }

    $remaining_num--;
    if (
      ref( $data_chunk ) eq 'ARRAY' && @{$data_chunk}
        && $remaining_num > 0
        ) {
      $hdl->unshift_read( $read_cb );
    }
    elsif ( $remaining_num == 0 ) {
      undef( $read_cb ); # Collect garbage
      if ( @errors ) {
        my $err = join( "\n", @errors );
        $cb->( $err, 1 );
      }
      else {
        $cb->( \@data );
      }

      return 1;
    }
  };

  $read_cb = $self->_prepare_read_cb( $cb_wrap );

  $hdl->unshift_read( $read_cb );

  return;
}

####
sub _prcoess_response {
  my __PACKAGE__ $self = shift;
  my $data = shift;
  my $is_err = shift;

  if ( $is_err ) {
    my $cmd = shift( @{$self->{command_queue}} );
    if ( defined( $cmd ) ) {
      $cmd->{on_error}->( $data );
    }
    else {
      $self->{on_error}->( $data );
    }

    return;
  }

  if ( %{$self->{subs}} && $self->_is_sub_message( $data ) ) {
    if ( exists( $self->{subs}{$data->[1]} ) ) {
      return $self->_process_sub_message( $data );
    }
  }

  my $cmd = $self->{command_queue}[0];

  if ( !defined( $cmd ) ) {
    $self->{on_error}->( "Don't known how process response data."
      . " Command queue is empty" );

    return;
  }

  if ( exists( $SUB_ACTION_CMDS{$cmd->{name}} ) ) {
    return $self->_process_sub_action( $cmd, $data );
  }

  if ( defined( $cmd->{on_done} ) ) {
    $cmd->{on_done}->( $data );
  }

  shift( @{$self->{command_queue}} );

  if ( $cmd->{name} eq 'quit' ) {
    $self->_disconnect();
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
      $sub->{on_done}->( $data->[1], $data->[2] );
    }
    if ( defined( $cmd->{on_message} ) ) {
      $sub->{on_message} = $cmd->{on_message};
    }
    $self->{subs}{$data->[1]} = $sub;
  }
  else {
    if ( defined( $cmd->{on_done} ) ) {
      $cmd->{on_done}->( $data->[1], $data->[2] );
    }
    if ( exists( $self->{subs}{$data->[1]} ) ) {
      delete( $self->{subs}{$data->[1]} );
    }
  }

  if ( --$cmd->{resp_remaining} == 0 ) {
    shift( @{$self->{command_queue}} );
  }

  return;
}

####
sub _process_sub_message {
  my __PACKAGE__ $self = shift;
  my $data = shift;

  my $sub = $self->{subs}{$data->[1]};
  if ( exists( $sub->{on_message} ) ) {
    if ( $data->[0] eq 'message' ) {
      $sub->{on_message}->( $data->[1], $data->[2] );
    }
    else {
      $sub->{on_message}->( $data->[2], $data->[3], $data->[1] );
    }
  }

  return;
}

####
sub _reset_handle {
  my __PACKAGE__ $self = shift;

  undef( $self->{handle} );
  if ( defined( $self->{password} ) ) {
    $self->{need_auth} = 1;
  }
  undef( $self->{sub_lock} );
  $self->{subs} = {};

  return;
}

####
sub _abort_commands {
  my __PACKAGE__ $self = shift;
  my $err = shift;

  if ( defined( $self->{command_queue} ) ) {
    my @cmd_queue = @{$self->{command_queue}};
    $self->{command_queue} = [];
    foreach my $cmd ( @cmd_queue ) {
      $cmd->{on_error}->( "$err. Command '$cmd->{name}' aborted" );
    }
  }

  return;
}

####
sub _is_sub_message {
  my $data = pop;
  return ref( $data ) eq 'ARRAY' && ( $data->[0] eq 'message'
      || $data->[0] eq 'pmessage' );
}

####
sub _disconnect {
  my __PACKAGE__ $self = shift;

  $self->_reset_handle();
  $self->_abort_commands( "Connection closed by client" );

  return;
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
    *{$AUTOLOAD} = $sub;
  };

  goto &{$sub};
}

####
sub DESTROY {
  my __PACKAGE__ $self = shift;

  $self->_disconnect();

  return;
}

1;
__END__

=head1 NAME

AnyEvent::Redis::RipeRedis - Non-blocking Redis client with auto reconnect feature

=head1 SYNOPSIS

  use AnyEvent;
  use AnyEvent::Redis::RipeRedis;

  my $cv = AnyEvent->condvar();

  my $redis = AnyEvent::Redis::RipeRedis->new(
    host => 'localhost',
    port => '6379',
    password => 'your_password',
    encoding => 'utf8',

    on_connect => sub {
      print "Connected\n";
    },

    on_error => sub {
      my $err = shift;
      warn "$err\n";
    },
  );

  # Set value
  $redis->set( 'bar', 'Some string', {
    on_done => sub {
      my $data = shift;
      print "$data\n";
      $cv->send();
    },

    on_error => sub {
      my $err = shift;
      warn "$err\n";
      $cv->croak();
    },
  } );

  $cv->recv();

=head1 DESCRIPTION

AnyEvent::Redis::RipeRedis is a non-blocking Redis client with auto reconnect
feature. It supports subscriptions, transactions, has simple API and it faster
than AnyEvent::Redis.

Requires Redis 1.2 or higher and any supported event loop.

=head1 CONSTRUCTOR

  my $redis = AnyEvent::Redis::RipeRedis->new(
    host => 'localhost',
    port => '6379',
    password => 'your_password',
    connection_timeout => 5,
    reconnect => 1,
    encoding => 'utf8',

    on_connect => sub {
      print "Connected\n";
    },

    on_disconnect => sub {
      print "Disconnected\n";
    },

    on_error => sub {
      my $err = shift;
      warn "$err\n";
    },
  );

=head2 host

Server hostname (default: 127.0.0.1)

=head2 port

Server port (default: 6379)

=head2 password

Authentication password. If it specified, client sends AUTH command after
establishing a connection. If it not specified, you can send AUTH command
yourself.

=head2 connection_timeout

Connection timeout. If after this timeout client could not
connect to the server, callback "on_error" will be called.

=head2 reconnect

If this parameter is TRUE, client in case of lost connection will automatically
reconnect during executing next command. If this parameter is FALSE, client don't
reconnect automaticaly. By default is TRUE.

=head2 encoding

Will be used to encode strings before sending them to the server and to decode
strings after receiving them from the server. If this parameter not specified,
encode and decode operations not performed.

=head2 on_connect

This callback will be called when connection will be established

=head2 on_disconnect

This callback will be called in case of disconnection

=head2 on_error

This callback will be called if occurred any errors

=head1 COMMAND EXECUTION

=head2 <command>( [ @cmd_args[, \%params ] ] )

  # Increment
  $redis->incr( 'foo', {
    on_done => sub {
      my $data = shift;
      print "$data\n";
    },

    on_error => sub {
      my $err = shift;
      warn "$err\n";
    },
  } );

  # Set value
  $redis->set( 'bar', 'Some string' );

  # Get value
  $redis->get( 'bar', {
    on_done => sub {
      my $data = shift;
      print "$data\n";
    },
  } );

  # Push values
  for ( my $i = 1; $i <= 3; $i++ ) {
    $redis->rpush( 'list', "element_$i", {
      on_done => sub {
        my $data = shift;
        print "$data\n";
      },
    } );
  }

  # Get list of values
  $redis->lrange( 'list', 0, -1, {
    on_done => sub {
      my $data = shift;
      foreach my $val ( @{ $data } ) {
        print "$val\n";
      }
    },

    on_error => sub {
      my $err = shift;
      warn "$err\n";
    },
  } );

Full list of Redis commands can be found here: L<http://redis.io/commands>

=head1 SUBSCRIPTIONS

=head2 subscribe( @channels[, \%params ] )

Subscribe to channel by name

  $redis->subscribe( qw( ch_foo ch_bar ), {
    on_done =>  sub {
      my $ch_name = shift;
      my $subs_num = shift;

      print "Subscribed: $ch_name. Active: $subs_num\n";
    },

    on_message => sub {
      my $ch_name = shift;
      my $msg = shift;

      print "$ch_name: $msg\n";
    },
  } );

=head2 psubscribe( @patterns[, \%params ] )

Subscribe to group of channels by pattern

  $redis->psubscribe( qw( info_* err_* ), {
    on_done =>  sub {
      my $ch_pattern = shift;
      my $subs_num = shift;

      print "Subscribed: $ch_pattern. Active: $subs_num\n";
    },

    on_message => sub {
      my $ch_name = shift;
      my $msg = shift;
      my $ch_pattern = shift;

      print "$ch_name ($ch_pattern): $msg\n";
    },

    on_error => sub {
      my $err = shift;
      warn "$err\n";
    },
  } );

=head2 unsubscribe( @channels[, \%params ] )

Unsubscribe from channel by name

  $redis->unsubscribe( qw( ch_foo ch_bar ), {
    on_done => sub {
      my $ch_name = shift;
      my $subs_num = shift;

      print "Unsubscribed: $ch_name. Active: $subs_num\n";
    },

    on_error => sub {
      my $err = shift;
      warn "$err\n";
    },
  } );

=head2 punsubscribe( @patterns[, \%params ] )

Unsubscribe from group of channels by pattern

  $redis->punsubscribe( qw( info_* err_* ), {
    on_done => sub {
      my $ch_pattern = shift;
      my $subs_num = shift;

      print "Unsubscribed: $ch_pattern. Active: $subs_num\n";
    },

    on_error => sub {
      my $err = shift;
      warn "$err\n";
    },
  } );

=head1 CONNECTION VIA UNIX-SOCKET

Redis 2.2 and higher support connection via UNIX domain socket. To connect via
a UNIX-socket in the parameter "host" you must specify "unix/", and in parameter
"port" you must specify the path to the socket.

  my $redis = AnyEvent::Redis::RipeRedis->new(
    host => 'unix/',
    port => '/tmp/redis.sock',
  );

=head1 SEE ALSO

L<AnyEvent>, L<AnyEvent::Redis>, L<Redis>, L<Redis::hiredis>

=head1 AUTHOR

Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>

=head2 Special thanks to:

=over

=item Alexey Shrub

=item Vadim Vlasov

=item Konstantin Uvarin

=back

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2012, Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>. All rights
reserved.

This module is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut
