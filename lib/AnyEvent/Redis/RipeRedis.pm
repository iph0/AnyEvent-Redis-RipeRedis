use 5.008000;
use strict;
use warnings;

####
package AnyEvent::Redis::RipeRedis;

use base qw( Exporter );

use fields qw(
  host
  port
  password
  database
  connection_timeout
  read_timeout
  reconnect
  encoding
  on_connect
  on_disconnect
  on_connect_error
  on_error

  _handle
  _connected
  _lazy_conn_st
  _auth_st
  _db_select_st
  _ready_to_write
  _input_queue
  _tmp_queue
  _processing_queue
  _sub_lock
  _subs
);

our $VERSION = '1.344';

use AnyEvent;
use AnyEvent::Handle;
use Encode qw( find_encoding is_utf8 );
use Scalar::Util qw( looks_like_number weaken );
use Digest::SHA qw( sha1_hex );
use Carp qw( confess );

BEGIN {
  our @EXPORT_OK = qw(
    E_CANT_CONN
    E_LOADING_DATASET
    E_IO
    E_CONN_CLOSED_BY_REMOTE_HOST
    E_CONN_CLOSED_BY_CLIENT
    E_NO_CONN
    E_OPRN_ERROR
    E_UNEXPECTED_DATA
    E_NO_SCRIPT
    E_READ_TIMEDOUT
    E_BUSY
    E_MASTER_DOWN
    E_MISCONF
    E_READONLY
    E_OOM
    E_EXEC_ABORT
    E_NO_AUTH
    E_WRONG_TYPE
    E_NO_REPLICAS
  );

  our %EXPORT_TAGS = (
    err_codes => \@EXPORT_OK,
  );
}

use constant {
  # Default values
  D_HOST     => 'localhost',
  D_PORT     => 6379,
  D_DB_INDEX => 0,

  # Error codes
  E_CANT_CONN                  => 1,
  E_LOADING_DATASET            => 2,
  E_IO                         => 3,
  E_CONN_CLOSED_BY_REMOTE_HOST => 4,
  E_CONN_CLOSED_BY_CLIENT      => 5,
  E_NO_CONN                    => 6,
  E_OPRN_ERROR                 => 9,
  E_UNEXPECTED_DATA            => 10,
  E_NO_SCRIPT                  => 11,
  E_READ_TIMEDOUT              => 12,
  E_BUSY                       => 13,
  E_MASTER_DOWN                => 14,
  E_MISCONF                    => 15,
  E_READONLY                   => 16,
  E_OOM                        => 17,
  E_EXEC_ABORT                 => 18,
  E_NO_AUTH                    => 19,
  E_WRONG_TYPE                 => 20,
  E_NO_REPLICAS                => 21,

  # Command status
  S_NEED_PERFORM => 1,
  S_IN_PROGRESS  => 2,
  S_IS_DONE      => 3,

  # String terminator
  EOL     => "\r\n",
  EOL_LEN => 2,
};

my %SUB_CMDS = (
  subscribe  => 1,
  psubscribe => 1,
);
my %SUB_UNSUB_CMDS = (
  %SUB_CMDS,
  unsubscribe  => 1,
  punsubscribe => 1,
);
my %SUB_MSG_TYPES = (
  message  => 1,
  pmessage => 1,
);

my %SPECIAL_CMDS = (
  %SUB_UNSUB_CMDS,
  exec  => 1,
  multi => 1,
);

my %ERR_PREFIXES_MAP = (
  LOADING    => E_LOADING_DATASET,
  NOSCRIPT   => E_NO_SCRIPT,
  BUSY       => E_BUSY,
  MASTERDOWN => E_MASTER_DOWN,
  MISCONF    => E_MISCONF,
  READONLY   => E_READONLY,
  OOM        => E_OOM,
  EXECABORT  => E_EXEC_ABORT,
  NOAUTH     => E_NO_AUTH,
  WRONGTYPE  => E_WRONG_TYPE,
  NOREPLICAS => E_NO_REPLICAS,
);

my %EVAL_CACHE;


# Constructor
sub new {
  my $proto = shift;
  my $params = { @_ };

  my __PACKAGE__ $self = ref( $proto ) ? $proto : fields::new( $proto );

  $self->{host}     = $params->{host} || D_HOST;
  $self->{port}     = $params->{port} || D_PORT;
  $self->{password} = $params->{password};

  if ( !defined $params->{database} ) {
    $params->{database} = D_DB_INDEX;
  }
  $self->{database} = $params->{database};

  $self->connection_timeout( $params->{connection_timeout} );
  $self->read_timeout( $params->{read_timeout} );

  if ( !exists $params->{reconnect} ) {
    $params->{reconnect} = 1;
  }
  $self->{reconnect} = $params->{reconnect};

  $self->encoding( $params->{encoding} );

  $self->{on_connect}       = $params->{on_connect};
  $self->{on_disconnect}    = $params->{on_disconnect};
  $self->{on_connect_error} = $params->{on_connect_error};
  $self->on_error( $params->{on_error} );

  $self->{_handle}           = undef;
  $self->{_connected}        = 0;
  $self->{_lazy_conn_st}     = $params->{lazy};
  $self->{_auth_st}          = S_NEED_PERFORM;
  $self->{_db_select_st}     = S_NEED_PERFORM;
  $self->{_ready_to_write}   = 0;
  $self->{_input_queue}      = [];
  $self->{_tmp_queue}        = [];
  $self->{_processing_queue} = [];
  $self->{_sub_lock}         = 0;
  $self->{_subs}             = {};

  if ( !$self->{_lazy_conn_st} ) {
    $self->_connect();
  }

  return $self;
}

####
sub eval_cached {
  my __PACKAGE__ $self = shift;
  my @args = @_;

  my $cmd = {};
  if ( ref( $args[-1] ) eq 'CODE' ) {
    $cmd->{on_reply} = pop( @args );
  }
  elsif ( ref( $args[-1] ) eq 'HASH' ) {
    $cmd = pop( @args );
  }
  $cmd->{keyword} = 'evalsha';
  $cmd->{args} = \@args;

  $cmd->{script} = $args[0];
  if ( !exists $EVAL_CACHE{ $cmd->{script} } ) {
    $EVAL_CACHE{ $cmd->{script} } = sha1_hex( $cmd->{script} );
  }
  $args[0] = $EVAL_CACHE{ $cmd->{script} };

  $self->_execute_cmd( $cmd );

  return;
}

####
sub disconnect {
  my __PACKAGE__ $self = shift;

  $self->_disconnect();

  return;
}

####
sub encoding {
  my __PACKAGE__ $self = shift;

  if ( @_ ) {
    my $enc = shift;

    if ( defined $enc ) {
      $self->{encoding} = find_encoding( $enc );
      if ( !defined $self->{encoding} ) {
        confess "Encoding '$enc' not found";
      }
    }
    else {
      undef $self->{encoding};
    }
  }

  return $self->{encoding};
}

####
sub on_error {
  my __PACKAGE__ $self = shift;

  if ( @_ ) {
    my $on_error = shift;
    if ( !defined $on_error ) {
      $on_error = sub {
        my $err_msg = shift;

        warn "$err_msg\n";
      };
    }
    $self->{on_error} = $on_error;
  }

  return $self->{on_error};
}

# Generate more accessors
{
  no strict 'refs';

  foreach my $field_pref ( qw( connection read ) ) {
    my $field_name = $field_pref . '_timeout';

    *{$field_name} = sub {
      my __PACKAGE__ $self = shift;

      if ( @_ ) {
        my $timeout = shift;
        if (
          defined $timeout
            and ( !looks_like_number( $timeout ) or $timeout < 0 )
            ) {
          confess ucfirst( $field_pref ) . ' timeout must be a positive number';
        }
        $self->{ $field_name } = $timeout;
      }

      return $self->{ $field_name };
    }
  }

  foreach my $field_name (
    qw( reconnect on_connect on_disconnect on_connect_error )
      ) {
    *{$field_name} = sub {
      my __PACKAGE__ $self = shift;

      if ( @_ ) {
        $self->{ $field_name } = shift;
      }

      return $self->{ $field_name };
    }
  }
}

####
sub selected_database {
  my __PACKAGE__ $self = shift;

  return $self->{database};
}

####
sub _connect {
  my __PACKAGE__ $self = shift;

  $self->{_handle} = AnyEvent::Handle->new(
    connect          => [ $self->{host}, $self->{port} ],
    on_prepare       => $self->_get_prepare_cb(),
    on_connect       => $self->_get_connect_cb(),
    on_connect_error => $self->_get_connect_error_cb(),
    on_rtimeout      => $self->_get_rtimeout_cb(),
    on_eof           => $self->_get_eof_cb(),
    on_error         => $self->_get_client_error_cb(),
    on_read          => $self->_get_read_cb( $self->_get_reply_cb() ),
  );

  return;
}

####
sub _get_prepare_cb {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  return sub {
    if ( defined $self->{connection_timeout} ) {
      return $self->{connection_timeout};
    }

    return;
  };
}

####
sub _get_connect_cb {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  return sub {
    $self->{_connected} = 1;
    if ( !defined $self->{password} ) {
      $self->{_auth_st} = S_IS_DONE;
    }
    if ( $self->{database} == 0 ) {
      $self->{_db_select_st} = S_IS_DONE;
    }

    if ( $self->{_auth_st} == S_NEED_PERFORM ) {
      $self->_auth();
    }
    elsif ( $self->{_db_select_st} == S_NEED_PERFORM ) {
      $self->_select_db();
    }
    else {
      $self->{_ready_to_write} = 1;
      $self->_flush_input_queue();
    }

    if ( defined $self->{on_connect} ) {
      $self->{on_connect}->();
    }
  };
}

####
sub _get_connect_error_cb {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  return sub {
    my $err_msg = pop;

    $self->_disconnect(
        "Can't connect to $self->{host}:$self->{port}: $err_msg",
        E_CANT_CONN );
  };
}

####
sub _get_rtimeout_cb {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  return sub {
    if ( @{$self->{_processing_queue}} ) {
      $self->_disconnect( 'Read timed out.', E_READ_TIMEDOUT );
    }
    else {
      $self->{_handle}->rtimeout( undef );
    }
  };
}

####
sub _get_eof_cb {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  return sub {
    $self->_disconnect( 'Connection closed by remote host.',
        E_CONN_CLOSED_BY_REMOTE_HOST );
  };
}

####
sub _get_client_error_cb {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  return sub {
    my $err_msg = pop;

    $self->_disconnect( $err_msg, E_IO );
  };
}

####
sub _get_read_cb {
  my __PACKAGE__ $self = shift;
  my $cb = shift;

  my $bulk_len;

  weaken( $self );

  return sub {
    my $hdl = shift;

    while ( 1 ) {
      if ( $hdl->destroyed() ) {
        return;
      }

      if ( defined $bulk_len ) {
        if ( length( $hdl->{rbuf} ) < $bulk_len + EOL_LEN ) {
          return;
        }

        my $reply = substr( $hdl->{rbuf}, 0, $bulk_len, '' );
        substr( $hdl->{rbuf}, 0, EOL_LEN, '' );
        if ( defined $self->{encoding} ) {
          $reply = $self->{encoding}->decode( $reply );
        }
        undef $bulk_len;

        return 1 if $cb->( $reply );
      }
      else {
        my $eol_pos = index( $hdl->{rbuf}, EOL );
        if ( $eol_pos < 0 ) {
          return;
        }

        my $reply = substr( $hdl->{rbuf}, 0, $eol_pos, '' );
        my $type = substr( $reply, 0, 1, '' );
        substr( $hdl->{rbuf}, 0, EOL_LEN, '' );

        if ( $type eq '+' or $type eq ':' ) {
          return 1 if $cb->( $reply );
        }
        elsif ( $type eq '$' ) {
          if ( $reply >= 0 ) {
            $bulk_len = $reply;
          }
          else {
            return 1 if $cb->();
          }
        }
        elsif ( $type eq '*' ) {
          my $mbulk_len = $reply;
          if ( $mbulk_len > 0 ) {
            $self->_unshift_read( $mbulk_len, $cb );
            return 1;
          }
          elsif ( $mbulk_len < 0 ) {
            return 1 if $cb->();
          }
          else {
            return 1 if $cb->( [] );
          }
        }
        elsif ( $type eq '-' ) {
          my $err_code = E_OPRN_ERROR;
          if ( $reply =~ m/^([A-Z]{3,}) / ) {
            if ( exists $ERR_PREFIXES_MAP{ $1 } ) {
              $err_code = $ERR_PREFIXES_MAP{ $1 };
            }
          }

          return 1 if $cb->( $reply, $err_code );
        }
        else {
          $self->_disconnect( 'Unexpected data type received.',
              E_UNEXPECTED_DATA );

          return;
        }
      }
    }

    return;
  };
}

####
sub _get_reply_cb {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  return sub {
    if ( defined $_[1] ) {
      $self->_handle_error_reply( @_ );
    }
    elsif (
      %{$self->{_subs}} and ref( $_[0] ) eq 'ARRAY'
        and exists $SUB_MSG_TYPES{ $_[0][0] }
        ) {
      $self->_handle_pub_message( $_[0] );
    }
    else {
      $self->_handle_success_reply( $_[0] );
    }

    return;
  };
}

####
sub _execute_cmd {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;

  if ( exists $SPECIAL_CMDS{ $cmd->{keyword} } ) {
    if ( exists $SUB_UNSUB_CMDS{ $cmd->{keyword} } ) {
      if ( exists $SUB_CMDS{ $cmd->{keyword} } ) {
        if ( !defined $cmd->{on_message} ) {
          confess '\'on_message\' callback must be specified';
        }
      }
      if ( $self->{_sub_lock} ) {
        AE::postpone(
          sub {
            $self->_handle_cmd_error( $cmd,
                "Command '$cmd->{keyword}' not allowed after 'multi' command."
                    . ' First, the transaction must be completed.',
                E_OPRN_ERROR );
          }
        );

        return;
      }
      $cmd->{replies_left} = scalar( @{$cmd->{args}} );
    }
    elsif ( $cmd->{keyword} eq 'multi' ) {
      $self->{_sub_lock} = 1;
    }
    else { # exec
      $self->{_sub_lock} = 0;
    }
  }

  if ( $self->{_ready_to_write} ) {
    $self->_push_write( $cmd );
  }
  else {
    if ( defined $self->{_handle} ) {
      if ( $self->{_connected} ) {
        if ( $self->{_auth_st} == S_IS_DONE ) {
          if ( $self->{_db_select_st} == S_NEED_PERFORM ) {
            $self->_select_db();
          }
        }
        elsif ( $self->{_auth_st} == S_NEED_PERFORM ) {
          $self->_auth();
        }
      }
    }
    elsif ( $self->{reconnect} or $self->{_lazy_conn_st} ) {
      if ( $self->{_lazy_conn_st} ) {
        $self->{_lazy_conn_st} = 0;
      }
      $self->_connect();
    }
    else {
      AE::postpone(
        sub {
          $self->_handle_cmd_error( $cmd,
              "Can't handle the command '$cmd->{keyword}'."
                  . ' No connection to the server.',
              E_NO_CONN );
        }
      );

      return;
    }

    push( @{$self->{_input_queue}}, $cmd );
  }

  return;
}

####
sub _push_write {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;

  my $cmd_str = '';
  foreach my $token ( $cmd->{keyword}, @{$cmd->{args}} ) {
    if ( !defined $token ) {
      $token = '';
    }
    elsif ( defined $self->{encoding} and is_utf8( $token ) ) {
      $token = $self->{encoding}->encode( $token );
    }
    $cmd_str .= '$' . length( $token ) . EOL . $token . EOL;
  }
  $cmd_str = '*' . ( scalar( @{$cmd->{args}} ) + 1 ) . EOL . $cmd_str;

  my $hdl = $self->{_handle};
  if ( !@{$self->{_processing_queue}} ) {
    $hdl->rtimeout_reset();
    $hdl->rtimeout( $self->{read_timeout} );
  }
  push( @{$self->{_processing_queue}}, $cmd );
  $hdl->push_write( $cmd_str );

  return;
}

####
sub _auth {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  $self->{_auth_st} = S_IN_PROGRESS;

  $self->_push_write(
    { keyword => 'auth',
      args    => [ $self->{password} ],
      on_done => sub {
        $self->{_auth_st} = S_IS_DONE;
        if ( $self->{_db_select_st} == S_NEED_PERFORM ) {
          $self->_select_db();
        }
        else {
          $self->{_ready_to_write} = 1;
          $self->_flush_input_queue();
        }
      },
      on_error => sub {
        $self->{_auth_st} = S_NEED_PERFORM;
        $self->_abort_all( @_ );
      },
    }
  );

  return;
}

####
sub _select_db {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  $self->{_db_select_st} = S_IN_PROGRESS;
  $self->_push_write(
    { keyword => 'select',
      args    => [ $self->{database} ],
      on_done => sub {
        $self->{_db_select_st} = S_IS_DONE;
        $self->{_ready_to_write} = 1;
        $self->_flush_input_queue();
      },
      on_error => sub {
        $self->{_db_select_st} = S_NEED_PERFORM;
        $self->_abort_all( @_ );
      },
    }
  );

  return;
}

####
sub _flush_input_queue {
  my __PACKAGE__ $self = shift;

  $self->{_tmp_queue} = $self->{_input_queue};
  $self->{_input_queue} = [];
  while ( my $cmd = shift( @{$self->{_tmp_queue}} ) ) {
    $self->_push_write( $cmd );
  }

  return;
}

####
sub _unshift_read {
  my __PACKAGE__ $self = shift;
  my $replies_left = shift;
  my $cb           = shift;

  my $read_cb;
  my $reply_cb;
  my @mbulk_reply;
  my $has_err;
  {
    my $self = $self;
    weaken( $self );

    $reply_cb = sub {
      my $reply    = shift;
      my $err_code = shift;

      if ( defined $err_code ) {
        $has_err = 1;

        if ( ref( $reply ) ne 'ARRAY' ) {
          $reply = AnyEvent::Redis::RipeRedis::Error->new(
            code    => $err_code,
            message => $reply,
          );
        }
      }

      push( @mbulk_reply, $reply );

      $replies_left--;
      if ( ref( $reply ) eq 'ARRAY' and @{$reply} and $replies_left > 0 ) {
        $self->{_handle}->unshift_read( $read_cb );
      }
      elsif ( $replies_left == 0 ) {
        undef $read_cb; # Collect garbage

        if ( $has_err ) {
          $cb->( \@mbulk_reply, E_OPRN_ERROR );
        }
        else {
          $cb->( \@mbulk_reply );
        }

        return 1;
      }

      return;
    };
  }
  $read_cb = $self->_get_read_cb( $reply_cb );

  $self->{_handle}->unshift_read( $read_cb );

  return;
}

####
sub _handle_success_reply {
  my __PACKAGE__ $self = shift;

  my $cmd = $self->{_processing_queue}[0];

  if ( !defined $cmd ) {
    $self->_disconnect(
        'Don\'t known how process reply. Command queue is empty.',
        E_UNEXPECTED_DATA );

    return;
  }

  if ( exists $SUB_UNSUB_CMDS{ $cmd->{keyword} } ) {
    if ( --$cmd->{replies_left} == 0 ) {
      shift( @{$self->{_processing_queue}} );
    }

    if ( exists $SUB_CMDS{ $cmd->{keyword} } ) {
      $self->{_subs}{ $_[0][1] } = $cmd->{on_message};
    }
    else {
      delete( $self->{_subs}{ $_[0][1] } );
    }

    shift( @{$_[0]} );
    if ( defined $cmd->{on_done} ) {
      $cmd->{on_done}->( @{$_[0]} );
    }
    elsif ( defined $cmd->{on_reply} ) {
      $cmd->{on_reply}->( $_[0] );
    }

    return;
  }

  shift( @{$self->{_processing_queue}} );

  if ( $cmd->{keyword} eq 'select' ) {
    $self->{database} = $cmd->{args}[0];
  }
  elsif ( $cmd->{keyword} eq 'quit' ) {
    $self->_disconnect();
  }

  if ( defined $cmd->{on_done} ) {
    $cmd->{on_done}->( $_[0] );
  }
  elsif ( defined $cmd->{on_reply} ) {
    $cmd->{on_reply}->( $_[0] );
  }

  return;
}

####
sub _handle_error_reply {
  my __PACKAGE__ $self = shift;

  my $cmd = shift( @{$self->{_processing_queue}} );

  if ( !defined $cmd ) {
    $self->_disconnect(
        'Don\'t known how process error. Command queue is empty.',
        E_UNEXPECTED_DATA );

    return;
  }

  if ( $_[1] == E_NO_SCRIPT and exists $cmd->{script} ) {
    $cmd->{keyword} = 'eval';
    $cmd->{args}[0] = $cmd->{script};

    $self->_push_write( $cmd );

    return;
  }

  # Condition for EXEC command and for some Lua scripts
  if ( ref( $_[0] ) eq 'ARRAY' ) {
    $self->_handle_cmd_error( $cmd,
        "Operation '$cmd->{keyword}' completed with errors.",
        @_[ 1, 0 ] );
  }
  else {
    $self->_handle_cmd_error( $cmd, @_ );
  }

  return;
}

####
sub _handle_pub_message {
  my __PACKAGE__ $self = shift;

  my $msg_cb = $self->{_subs}{ $_[0][1] };

  if ( !defined $msg_cb ) {
    $self->_disconnect(
        'Don\'t known how process published message.'
            . " Unknown channel or pattern '$_[0][1]'.",
        E_UNEXPECTED_DATA );

    return;
  }

  if ( $_[0][0] eq 'message' ) {
    $msg_cb->( @{$_[0]}[ 1, 2 ] );
  }
  else {
    $msg_cb->( @{$_[0]}[ 2, 3, 1 ] );
  }

  return;
}

####
sub _handle_client_error {
  my __PACKAGE__ $self = shift;

  if ( $_[1] == E_CANT_CONN ) {
    if ( defined $self->{on_connect_error} ) {
      $self->{on_connect_error}->( $_[0] );
    }
    else {
      $self->{on_error}->( @_ );
    }
  }
  else {
    $self->{on_error}->( @_ );
  }

  return;
}

####
sub _handle_cmd_error {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;

  if ( defined $cmd->{on_error} ) {
    $cmd->{on_error}->( @_ );
  }
  elsif ( defined $cmd->{on_reply} ) {
    $cmd->{on_reply}->( @_[ 2, 0, 1 ] );
  }
  else {
    $self->{on_error}->( @_ );
  }

  return;
}

####
sub _disconnect {
  my __PACKAGE__ $self = shift;
  my $err_msg  = shift;
  my $err_code = shift;

  my $was_connected = $self->{_connected};

  if ( defined $self->{_handle} ) {
    $self->{_handle}->destroy();
    undef $self->{_handle};
  }
  $self->{_connected}      = 0;
  $self->{_auth_st}        = S_NEED_PERFORM;
  $self->{_db_select_st}   = S_NEED_PERFORM;
  $self->{_ready_to_write} = 0;
  $self->{_sub_lock}       = 0;

  $self->_abort_all( $err_msg, $err_code );

  if ( $was_connected and defined $self->{on_disconnect} ) {
    $self->{on_disconnect}->();
  }

  return;
}

####
sub _abort_all {
  my __PACKAGE__ $self = shift;
  my $err_msg  = shift;
  my $err_code = shift;

  my @cmds = (
    @{$self->{_processing_queue}},
    @{$self->{_tmp_queue}},
    @{$self->{_input_queue}},
  );

  $self->{_input_queue}      = [];
  $self->{_tmp_queue}        = [];
  $self->{_processing_queue} = [];
  $self->{_subs}             = {};

  if ( !defined $err_msg and @cmds ) {
    $err_msg = 'Connection closed by client prematurely.';
    $err_code = E_CONN_CLOSED_BY_CLIENT;
  }
  if ( defined $err_msg ) {
    $self->_handle_client_error( $err_msg, $err_code );
  }

  foreach my $cmd ( @cmds ) {
    my $cmd_err_msg = "Operation '$cmd->{keyword}' aborted: $err_msg";
    $self->_handle_cmd_error( $cmd, $cmd_err_msg, $err_code );
  }

  return;
}

####
sub AUTOLOAD {
  our $AUTOLOAD;
  my $cmd_keyword = $AUTOLOAD;
  $cmd_keyword =~ s/^.+:://;

  my $sub = sub {
    my __PACKAGE__ $self = shift;
    my @args = @_;

    my $cmd = {};
    if ( ref( $args[-1] ) eq 'CODE' ) {
      if ( exists $SUB_CMDS{ $cmd_keyword } ) {
        $cmd->{on_message} = pop( @args );
      }
      else {
        $cmd->{on_reply} = pop( @args );
      }
    }
    elsif ( ref( $args[-1] ) eq 'HASH' ) {
      $cmd = pop( @args );
    }
    $cmd->{keyword} = $cmd_keyword;
    $cmd->{args} = \@args;

    $self->_execute_cmd( $cmd );
  };

  do {
    no strict 'refs';
    *{$cmd_keyword} = $sub;
  };

  goto &{$sub};
}

####
sub DESTROY {
  my __PACKAGE__ $self = shift;

  if ( defined $self->{_handle} ) {
    my @cmds = (
      @{$self->{_processing_queue}},
      @{$self->{_tmp_queue}},
      @{$self->{_input_queue}},
    );

    foreach my $cmd ( @cmds ) {
      warn "Operation '$cmd->{keyword}' aborted:"
          . " Client object destroyed prematurely.\n";
    }
  }

  return;
}


####
package AnyEvent::Redis::RipeRedis::Error;

use fields qw(
  message
  code
);


# Constructor
sub new {
  my $proto  = shift;
  my %params = @_;

  my __PACKAGE__ $self = fields::new( $proto );

  $self->{message} = $params{message};
  $self->{code}    = $params{code};

  return $self;
}

####
sub message {
  my __PACKAGE__ $self = shift;

  return $self->{message};
}

####
sub code {
  my __PACKAGE__ $self = shift;

  return $self->{code};
}

1;
__END__

=head1 NAME

AnyEvent::Redis::RipeRedis - Flexible non-blocking Redis client with reconnect
feature

=head1 SYNOPSIS

  use AnyEvent;
  use AnyEvent::Redis::RipeRedis qw( :err_codes );

  my $cv = AE::cv();

  my $redis = AnyEvent::Redis::RipeRedis->new(
    host     => 'localhost',
    port     => '6379',
    password => 'yourpass',
  );

  $redis->set( 'foo', 'string',
    { on_error => sub {
        my $err_msg  = shift;
        my $err_code = shift;

        # error handling...

        $cv->croak( $err_msg );
      },
    }
  );

  $redis->get( 'foo',
    { on_done => sub {
        my $reply = shift;

        print "$reply\n";
      },

      on_error => sub {
        my $err_msg  = shift;
        my $err_code = shift;

        # error handling...

        $cv->croak( $err_msg );
      }
    }
  );

  $redis->incr( 'bar',
    sub {
      my $reply   = shift;
      my $err_msg = shift;

      if ( defined $err_msg ) {
        my $err_code = shift;

        # error handling...

        $cv->croak( $err_msg );

        return;
      }

      print "$reply\n";
    },
  );

  $redis->quit(
    sub {
      $cv->send();
    }
  );

  $cv->recv();

=head1 DESCRIPTION

AnyEvent::Redis::RipeRedis is the flexible non-blocking Redis client with
reconnect feature. The client supports subscriptions, transactions and connection
via UNIX-socket.

Requires Redis 1.2 or higher, and any supported event loop.

=head1 CONSTRUCTOR

=head2 new()

  my $redis = AnyEvent::Redis::RipeRedis->new(
    host               => 'localhost',
    port               => '6379',
    password           => 'yourpass',
    database           => 7,
    lazy               => 1,
    connection_timeout => 5,
    read_timeout       => 5,
    reconnect          => 1,
    encoding           => 'utf8',

    on_connect => sub {
      # handling...
    },

    on_disconnect => sub {
      # handling...
    },

    on_connect_error => sub {
      my $err_msg = shift;

      # error handling...
    },

    on_error => sub {
      my $err_msg  = shift;
      my $err_code = shift;

      # error handling...
    },
  );

=over

=item host

Server hostname (default: 127.0.0.1)

=item port

Server port (default: 6379)

=item password

If the password is specified, then the C<AUTH> command is sent to the server
after connection.

=item database

Database index. If the index is specified, then the client is switched to
the specified database after connection. You can also switch to another database
after connection by using C<SELECT> command. The client remembers last selected
database after reconnection.

The default database index is C<0>.

=item connection_timeout

Timeout, within which the client will be wait the connection establishment to
the Redis server. If the client could not connect to the server after specified
timeout, then the C<on_error> or C<on_connect_error> callback is called. In case,
when C<on_error> callback is called, C<E_CANT_CONN> error code is passed to
callback in the second argument. The timeout specifies in seconds and can
contain a fractional part.

  my $redis = AnyEvent::Redis::RipeRedis->new(
    connection_timeout => 10.5,
  );

By default the client use kernel's connection timeout.

=item read_timeout

Timeout, within which the client will be wait a response on a command from the
Redis server. If the client could not receive a response from the server after
specified timeout, then the client close connection and call C<on_error> callback
with the C<E_READ_TIMEDOUT> error code. The timeout is specifies in seconds
and can contain a fractional part.

  my $redis = AnyEvent::Redis::RipeRedis->new(
    read_timeout => 3.5,
  );

Not set by default.

=item lazy

If this parameter is set, then the connection establishes, when you will send
the first command to the server. By default the connection establishes after
calling of the C<new> method.

=item reconnect

If the connection to the Redis server was lost and the parameter C<reconnect> is
TRUE, then the client try to restore the connection on a next command executuion.
The client try to reconnect only once and if it fails, then is called the
C<on_error> callback. If you need several attempts of the reconnection, just
retry a command from the C<on_error> callback as many times, as you need. This
feature made the client more responsive.

By default is TRUE.

=item encoding

Used for encode/decode strings at time of input/output operations.

Not set by default.

=item on_connect => $cb->()

The C<on_connect> callback is called, when the connection is successfully
established.

Not set by default.

=item on_disconnect => $cb->()

The C<on_disconnect> callback is called, when the connection is closed by any
reason.

Not set by default.

=item on_connect_error => $cb->( $err_msg )

The C<on_connect_error> callback is called, when the connection could not be
established. If this collback isn't specified, then the common C<on_error>
callback is called with the C<E_CANT_CONN> error code.

Not set by default.

=item on_error => $cb->( $err_msg, $err_code )

The common C<on_error> callback is called when ocurred some error, which was
affected on entire client (e. g. connection error). Also this callback is called
on other errors if neither C<on_error> callback nor C<on_reply> callback is
specified in the command method. If common C<on_error> callback is not specified,
the client just print an error messages to C<STDERR>.

=back

=head1 COMMAND EXECUTION

=head2 <command>( [ @args, ][ $cb | \%callbacks ] )

The full list of the Redis commands can be found here: L<http://redis.io/commands>.

  $redis->set( 'foo', 'string' );

  $redis->get( 'foo',
    { on_done => sub {
        my $reply = shift;

        print "$reply\n";
      },

      on_error => sub {
        my $err_msg  = shift;
        my $err_code = shift;

        # error handling...
      }
    }
  );

  $redis->lrange( 'list', 0, -1,
    { on_done => sub {
        my $reply = shift;

        foreach my $val ( @{$reply}  ) {
          print "$val\n";
        }
      },

      on_error => sub {
        my $err_msg  = shift;
        my $err_code = shift;

        # error handling...
      },
    }
  );

  $redis->incr( 'bar',
    sub {
      my $reply   = shift;
      my $err_msg = shift;

      if ( defined $err_msg ) {
        my $err_code = shift;

        # error handling...

        return;
      }

      print "$reply\n";
    },
  );

  $redis->incr( 'bar',
    { on_reply => sub {
        my $reply   = shift;
        my $err_msg = shift;

        if ( defined $err_msg ) {
          my $err_code = shift;

          # error handling...

          return;
        }

        print "$reply\n";
      },
    }
  );

=over

=item on_done => $cb->( [ $reply ] )

The C<on_done> callback is called, when the current operation was completed
successfully.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, when some error occurred.

=item on_reply => $cb->( [ $reply, ][ $err_msg, $err_code ] )

Since version 1.300 of the client you can specify single, C<on_reply> callback,
instead of two, C<on_done> and C<on_error> callbacks. The C<on_reply> callback
is called in both cases: when operation was completed successfully and when some
error occurred. In first case to callback is passed only reply data. In second
case to callback is passed three arguments: The C<undef> value or reply data
with error objects (see below), error message and error code.

=back

=head1 TRANSACTIONS

The detailed information abount the Redis transactions can be found here:
L<http://redis.io/topics/transactions>.

=head2 multi( [ $cb | \%callbacks ] )

Marks the start of a transaction block. Subsequent commands will be queued for
atomic execution using C<EXEC>.

=head2 exec( [ $cb | \%callbacks ] )

Executes all previously queued commands in a transaction and restores the
connection state to normal. When using C<WATCH>, C<EXEC> will execute commands
only if the watched keys were not modified.

If after execution of C<EXEC> command at least one operation fails, then
either C<on_error> or C<on_reply> callback is called with C<E_OPRN_ERROR> error
code. Additionally, to callbacks is passed reply data, which contain usual data
and error objects for each failed operation. To C<on_error> callback reply data
is passed in third argument and to C<on_reply> callback in first argument.
Error object is an instance of class C<AnyEvent::Redis::RipeRedis::Error> and
has two methods: C<message()> to get error message and C<code()> to get error
code.

  $redis->multi();
  $redis->set( 'foo', 'string' );
  $redis->incr( 'foo' ); # causes an error
  $redis->exec(
    { on_error => sub {
        my $err_msg  = shift;
        my $err_code = shift;
        my $reply    = shift;

        foreach my $reply ( @{$reply} ) {
          if ( ref( $reply ) eq 'AnyEvent::Redis::RipeRedis::Error' ) {
            my $oprn_err_msg  = $reply->message();
            my $oprn_err_code = $reply->code();

            # error handling...
          }
        }
      },
    }
  );

  $redis->multi();
  $redis->set( 'foo', 'string' );
  $redis->incr( 'foo' ); # causes an error
  $redis->exec(
    sub {
      my $reply   = shift;
      my $err_msg = shift;

      if ( defined $err_msg ) {
        my $err_code = shift;

        foreach my $reply ( @{$reply} ) {
          if ( ref( $reply ) eq 'AnyEvent::Redis::RipeRedis::Error' ) {
            my $oprn_err_msg  = $reply->message();
            my $oprn_err_code = $reply->code();

            # error handling...
          }
        }
      }
    },
  );

=head2 discard( [ $cb | \%callbacks ] )

Flushes all previously queued commands in a transaction and restores the
connection state to normal.

If C<WATCH> was used, C<DISCARD> unwatches all keys.

=head2 watch( @keys, [ $cb | \%callbacks ] )

Marks the given keys to be watched for conditional execution of a transaction.

=head2 unwatch( [ $cb | \%callbacks ] )

Forget about all watched keys.

=head1 SUBSCRIPTIONS

The detailed information about Redis Pub/Sub can be found here:
L<http://redis.io/topics/pubsub>

=head2 subscribe( @channels, ( $cb | \%callbacks ) )

Subscribes the client to the specified channels.

Once the client enters the subscribed state it is not supposed to issue any
other commands, except for additional C<SUBSCRIBE>, C<PSUBSCRIBE>, C<UNSUBSCRIBE>
and C<PUNSUBSCRIBE> commands.

  $redis->subscribe( qw( ch_foo ch_bar ),
    { on_done =>  sub {
        my $ch_name  = shift;
        my $subs_num = shift;

        # handling...
      },

      on_message => sub {
        my $ch_name = shift;
        my $msg     = shift;

        # handling...
      },

      on_error => sub {
        my $err_msg  = shift;
        my $err_code = shift;

        # error handling...
      },
    }
  );

  $redis->subscribe( qw( ch_foo ch_bar ),
    sub {
      my $ch_name = shift;
      my $msg     = shift;

      # handling...
    }
  );

  $redis->subscribe( qw( ch_foo ch_bar ),
    { on_reply => sub {
        my $reply   = shift;
        my $err_msg = shift;

        if ( defined $err_msg ) {
          my $err_code = shift;

          # error handling...

          return;
        }

        my $ch_name  = $reply->[0];
        my $subs_num = $reply->[1];

        # handling...
      },

      on_message => sub {
        my $ch_name = shift;
        my $msg     = shift;

        # handling...
      },
    }
  );

=over

=item on_done => $cb->( $ch_name, $sub_num )

The C<on_done> callback is called on every specified channel, when the
subscription operation was completed successfully.

=item on_message => $cb->( $ch_name, $msg )

The C<on_message> callback is called, when a published message was received.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, if the subscription operation fails.

=item on_reply => $cb->( $reply, [ $err_msg, $err_code ] )

The C<on_reply> callback is called in both cases: when the subscription operation
was completed successfully and when subscription operation fails. In first case
C<on_reply> callback is called on every specified channel. Information about
channel name and subscription number is passed to callback in first argument as
an array reference.

=back

=head2 psubscribe( @patterns, ( $cb | \%callbacks ) )

Subscribes the client to the given patterns.

  $redis->psubscribe( qw( foo_* bar_* ),
    { on_done =>  sub {
        my $ch_pattern = shift;
        my $subs_num   = shift;

        # handling...
      },

      on_message => sub {
        my $ch_name    = shift;
        my $msg        = shift;
        my $ch_pattern = shift;

        # handling...
      },

      on_error => sub {
        my $err_msg  = shift;
        my $err_code = shift;

        # error handling...
      },
    }
  );

  $redis->psubscribe( qw( foo_* bar_* ),
    sub {
      my $ch_name    = shift;
      my $msg        = shift;
      my $ch_pattern = shift;

      # handling...
    }
  );

  $redis->psubscribe( qw( foo_* bar_* ),
    { on_reply => sub {
        my $reply   = shift;
        my $err_msg = shift;

        if ( defined $err_msg ) {
          my $err_code = shift;

          # error handling...

          return;
        }

        my $ch_pattern = $reply->[0];
        my $subs_num   = $reply->[1];

        # handling...
      },

      on_message => sub {
        my $ch_name    = shift;
        my $msg        = shift;
        my $ch_pattern = shift;

        # handling...
      },
    }
  );

=over

=item on_done => $cb->( $ch_pattern, $sub_num )

The C<on_done> callback is called on every specified pattern, when the
subscription operation was completed successfully.

=item on_message => $cb->( $ch_name, $msg, $ch_pattern )

The C<on_message> callback is called, when published message was received.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, if the subscription operation fails.

=item on_reply => $cb->( $reply, [ $err_msg, $err_code ] )

The C<on_reply> callback is called in both cases: when the subscription
operation was completed successfully and when subscription operation fails.
In first case C<on_reply> callback is called on every specified pattern.
Information about channel pattern and subscription number is passed to callback
in first argument as an array reference.

=back

=head2 publish( $channel, $message, [ $cb | \%callbacks ] )

Posts a message to the given channel.

=head2 unsubscribe( [ @channels, ][ $cb | \%callbacks ] )

Unsubscribes the client from the given channels, or from all of them if none
is given.

When no channels are specified, the client is unsubscribed from all the
previously subscribed channels. In this case, a message for every unsubscribed
channel will be sent to the client.

  $redis->unsubscribe( qw( ch_foo ch_bar ),
    { on_done => sub {
        my $ch_name  = shift;
        my $subs_num = shift;

        # handling...
      },
    }
  );

  $redis->unsubscribe( qw( ch_foo ch_bar ),
    sub {
      my $reply   = shift;
      my $err_msg = shift;

      if ( defined $err_msg ) {
        my $err_code = shift;

        # error handling...

        return;
      }

      my $ch_name  = $reply->[0];
      my $subs_num = $reply->[1];

      # handling...
    }
  );

=over

=item on_done => $cb->( $ch_name, $sub_num )

The C<on_done> callback is called on every specified channel, when the
unsubscription operation was completed successfully.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, if the unsubscription operation fails.

=item on_reply => $cb->( $reply, [ $err_msg, $err_code ] )

The C<on_reply> callback is called in both cases: when the unsubscription
operation was completed successfully and when unsubscription operation fails.
In first case C<on_reply> callback is called on every specified channel.
Information about channel name and number of remaining subscriptions is passed
to callback in first argument as an array reference.

=back

=head2 punsubscribe( [ @patterns, ][ $cb | \%callbacks ] )

Unsubscribes the client from the given patterns, or from all of them if none
is given.

When no patters are specified, the client is unsubscribed from all the
previously subscribed patterns. In this case, a message for every unsubscribed
pattern will be sent to the client.

  $redis->punsubscribe( qw( foo_* bar_* ),
    { on_done => sub {
        my $ch_pattern = shift;
        my $subs_num   = shift;

        # handling...
      },
    }
  );

  $redis->punsubscribe( qw( foo_* bar_* ),
    sub {
      my $reply   = shift;
      my $err_msg = shift;

      if ( defined $err_msg ) {
        my $err_code = shift;

        # error handling...

        return;
      }

      my $ch_pattern = $reply->[0];
      my $subs_num   = $reply->[1];

      # handling...
    }
  );

=over

=item on_done => $cb->( $ch_name, $sub_num )

The C<on_done> callback is called on every specified pattern, when the
unsubscription operation was completed successfully.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, if the unsubscription operation fails.

=item on_reply => $cb->( $reply, [ $err_msg, $err_code ] )

The C<on_reply> callback is called in both cases: when the unsubscription
operation was completed successfully and when unsubscription operation fails.
In first case C<on_reply> callback is called on every specified pattern.
Information about channel pattern and number of remaining subscriptions is
passed to callback in first argument as an array reference.

=back

=head1 CONNECTION VIA UNIX-SOCKET

Redis 2.2 and higher support connection via UNIX domain socket. To connect via
a UNIX-socket in the parameter C<host> you have to specify C<unix/>, and in
the parameter C<port> you have to specify the path to the socket.

  my $redis = AnyEvent::Redis::RipeRedis->new(
    host => 'unix/',
    port => '/tmp/redis.sock',
  );

=head1 LUA SCRIPTS EXECUTION

Redis 2.6 and higher support execution of Lua scripts on the server side.
To execute a Lua script you can use one of the commands C<EVAL> or C<EVALSHA>,
or you can use the special method C<eval_cached()>.

If Lua script returns multi-bulk reply with at least one error reply, then
either C<on_error> or C<on_reply> callback is called with C<E_OPRN_ERROR> error
code. Additionally, to callbacks is passed reply data, which contain usual data
and error objects for each error reply, as well as described for C<EXEC> command.

  $redis->eval( "return { 'foo', redis.error_reply( 'Error.' ) }", 0,
    { on_error => sub {
        my $err_msg  = shift;
        my $err_code = shift;
        my $reply    = shift;

        foreach my $reply ( @{$reply} ) {
          if ( ref( $reply ) eq 'AnyEvent::Redis::RipeRedis::Error' ) {
            my $nested_err_msg  = $reply->message();
            my $nested_err_code = $reply->code();

            # error handling...
          }
        }
      }
    }
  );

  $redis->eval( "return { 'foo', redis.error_reply( 'Error.' ) }", 0,
    sub {
      my $reply   = shift;
      my $err_msg = shift;

      if ( defined $err_msg ) {
        my $err_code = shift;

        foreach my $reply ( @{$reply} ) {
          if ( ref( $reply ) eq 'AnyEvent::Redis::RipeRedis::Error' ) {
            my $nested_err_msg  = $reply->message();
            my $nested_err_code = $reply->code();

            # error handling...
          }
        }
      }
    }
  );

=head2 eval_cached( $script, $numkeys, [ @keys, ][ @args, ][ $cb | \%callbacks ] );

When you call the C<eval_cached()> method, the client first generate a SHA1
hash for a Lua script and cache it in memory. Then the client optimistically
send the C<EVALSHA> command under the hood. If the C<E_NO_SCRIPT> error will be
returned, the client send the C<EVAL> command.

If you call the C<eval_cached()> method with the same Lua script, client don not
generate a SHA1 hash for this script repeatedly, it gets a hash from the cache
instead.

  $redis->eval_cached( 'return { KEYS[1], KEYS[2], ARGV[1], ARGV[2] }',
      2, 'key1', 'key2', 'first', 'second',
    { on_done => sub {
        my $reply = shift;

        foreach my $val ( @{$reply}  ) {
          print "$val\n";
        }
      }
    }
  );

Be care, passing a different Lua scripts to C<eval_cached()> method every time
cause memory leaks.

=head1 ERROR CODES

Every time when error occurred the error code is passed to C<on_error> or to
C<on_reply> callback. Error codes can be used for programmatic handling of errors.

AnyEvent::Redis::RipeRedis provides constants of error codes, which can be
imported and used in expressions.

  use AnyEvent::Redis::RipeRedis qw( :err_codes );

=over

=item E_CANT_CONN

Can't connect to the server. All operations were aborted.

=item E_LOADING_DATASET

Redis is loading the dataset in memory.

=item E_IO

Input/Output operation error. The connection to the Redis server was closed and
all operations were aborted.

=item E_CONN_CLOSED_BY_REMOTE_HOST

The connection closed by remote host. All operations were aborted.

=item E_CONN_CLOSED_BY_CLIENT

Connection closed by client prematurely. Uncompleted operations were aborted.

=item E_NO_CONN

No connection to the Redis server. Connection was lost by any reason on previous
operation.

=item E_OPRN_ERROR

Operation error. For example, wrong number of arguments for a command.

=item E_UNEXPECTED_DATA

The client received unexpected data from the server. The connection to the Redis
server was closed and all operations were aborted.

=item E_READ_TIMEDOUT

Read timed out. The connection to the Redis server was closed and all operations
were aborted.

=back

Error codes available since Redis 2.6.

=over

=item E_NO_SCRIPT

No matching script. Use the C<EVAL> command.

=item E_BUSY

Redis is busy running a script. You can only call C<SCRIPT KILL>
or C<SHUTDOWN NOSAVE>.

=item E_MASTER_DOWN

Link with MASTER is down and slave-serve-stale-data is set to 'no'.

=item E_MISCONF

Redis is configured to save RDB snapshots, but is currently not able to persist
on disk. Commands that may modify the data set are disabled. Please check Redis
logs for details about the error.

=item E_READONLY

You can't write against a read only slave.

=item E_OOM

Command not allowed when used memory > 'maxmemory'.

=item E_EXEC_ABORT

Transaction discarded because of previous errors.

=back

Error codes available since Redis 2.8.

=over

=item E_NO_AUTH

Authentication required.

=item E_WRONG_TYPE

Operation against a key holding the wrong kind of value.

=item E_NO_REPLICAS

Not enough good slaves to write.

=back

=head1 DISCONNECTION

When the connection to the server is no longer needed you can close it in three
ways: call the method C<disconnect()>, send the C<QUIT> command or you can just
"forget" any references to an AnyEvent::Redis::RipeRedis object, but in this
case a client object is destroyed without calling any callbacks including
the C<on_disconnect> callback to avoid an unexpected behavior.

=head2 disconnect()

The method for synchronous disconnection. All uncompleted operations will be
aborted.

  $redis->disconnect();

=head2 quit()

The method for asynchronous disconnection. Uncompleted operations which was
started before C<QUIT> will be completed correctly.

  my $cv = AE::cv();

  $redis->set( 'foo', 'string' );
  $redis->incr( 'bar' );
  $redis->quit(
    sub {
      $cv->send();
    }
  );

  $cv->recv();

=head1 OTHER METHODS

=head2 connection_timeout( [ $seconds ] )

Get or set the C<connection_timeout> of the client. The C<undef> value resets
the C<connection_timeout> to default value.

=head2 read_timeout( [ $seconds ] )

Get or set the C<read_timeout> of the client.

=head2 reconnect( [ $boolean ] )

Enable or disable reconnection mode of the client.

=head2 encoding( [ $enc_name ] )

Get or set the current C<encoding> of the client.

=head2 on_connect( [ $callback ] )

Get or set the C<on_connect> callback.

=head2 on_disconnect( [ $callback ] )

Get or set the C<on_disconnect> callback.

=head2 on_connect_error( [ $callback ] )

Get or set the C<on_connect_error> callback.

=head2 on_error( [ $callback ] )

Get or set the C<on_error> callback.

=head2 selected_database()

Get currently selected database index.

=head1 SEE ALSO

L<AnyEvent>, L<AnyEvent::Redis>, L<Redis>, L<Redis::hiredis>, L<RedisDB>

=head1 AUTHOR

Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>

=head2 Special thanks

=over

=item *

Alexey Shrub

=item *

Vadim Vlasov

=item *

Konstantin Uvarin

=back

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2012-2013, Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>.
All rights reserved.

This module is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut
