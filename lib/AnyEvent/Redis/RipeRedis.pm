use 5.008000;
use strict;
use warnings;

####
package AnyEvent::Redis::RipeRedis;

use base qw( Exporter );

our $VERSION = '1.41_01';

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

my %SUBUNSUB_CMDS = (
  %SUB_CMDS,
  unsubscribe  => 1,
  punsubscribe => 1,
);

my %MSG_TYPES = (
  message  => 1,
  pmessage => 1,
);

my %NEED_POSTPROC = (
  %SUBUNSUB_CMDS,
  select => 1,
  quit   => 1,
);

my %ERR_PREFS_MAP = (
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
  my $proto  = shift;
  my $params = { @_ };

  my $self = ref( $proto ) ? $proto : bless {}, $proto;

  $self->{host}     = $params->{host} || D_HOST;
  $self->{port}     = $params->{port} || D_PORT;
  $self->{password} = $params->{password};
  unless ( defined $params->{database} ) {
    $params->{database} = D_DB_INDEX;
  }
  $self->{database} = $params->{database};
  $self->encoding( $params->{encoding} );
  $self->connection_timeout( $params->{connection_timeout} );
  $self->read_timeout( $params->{read_timeout} );
  unless ( exists $params->{reconnect} ) {
    $params->{reconnect} = 1;
  }
  $self->{reconnect} = $params->{reconnect};

  my $handle_params = $params->{handle_params};
  if ( !defined $params->{handle_params} ) {
    $handle_params = {};
  }
  foreach my $p_name ( qw( linger autocork ) ) {
    if ( !exists $handle_params->{ $p_name } and defined $params->{ $p_name } ) {
      $handle_params->{ $p_name } = $params->{ $p_name };
    }
  }
  $self->{handle_params} = $handle_params;

  $self->{on_connect}       = $params->{on_connect};
  $self->{on_disconnect}    = $params->{on_disconnect};
  $self->{on_connect_error} = $params->{on_connect_error};
  $self->on_error( $params->{on_error} );

  $self->{_handle}         = undef;
  $self->{_connected}      = 0;
  $self->{_lazy_conn_st}   = $params->{lazy};
  $self->{_auth_st}        = S_NEED_PERFORM;
  $self->{_select_db_st}   = S_NEED_PERFORM;
  $self->{_ready_to_write} = 0;
  $self->{_input_queue}    = [];
  $self->{_tmp_queue}      = [];
  $self->{_process_queue}  = [];
  $self->{_multi_lock}     = 0;
  $self->{_subs}           = {};
  $self->{_subs_num}       = 0;

  unless ( $self->{_lazy_conn_st} ) {
    $self->_connect();
  }

  return $self;
}

####
sub multi {
  my $self = shift;
  my $cmd  = $self->_parse_cmd_args( [ @_ ] );
  $cmd->{keyword} = 'multi';

  $self->{_multi_lock} = 1;
  $self->_execute_cmd( $cmd );

  return;
}

####
sub exec {
  my $self = shift;
  my $cmd  = $self->_parse_cmd_args( [ @_ ] );
  $cmd->{keyword} = 'exec';

  $self->{_multi_lock} = 0;
  $self->_execute_cmd( $cmd );

  return;
}

####
sub eval_cached {
  my $self = shift;
  my $cmd  = $self->_parse_cmd_args( [ @_ ] );
  $cmd->{keyword} = 'evalsha';

  $cmd->{script} = $cmd->{args}[0];
  unless ( exists $EVAL_CACHE{ $cmd->{script} } ) {
    $EVAL_CACHE{ $cmd->{script} } = sha1_hex( $cmd->{script} );
  }
  $cmd->{args}[0] = $EVAL_CACHE{ $cmd->{script} };

  $self->_execute_cmd( $cmd );

  return;
}

####
sub subscribe {
  my $self = shift;
  my $cmd  = $self->_parse_sub_args( [ @_ ] );
  $cmd->{keyword} = 'subscribe';

  $self->_subunsub( $cmd );

  return;
}

####
sub psubscribe {
  my $self = shift;
  my $cmd  = $self->_parse_sub_args( [ @_ ] );
  $cmd->{keyword} = 'psubscribe';

  $self->_subunsub( $cmd );

  return;
}

####
sub unsubscribe {
  my $self = shift;
  my $cmd  = $self->_parse_cmd_args( [ @_ ] );
  $cmd->{keyword} = 'unsubscribe';

  $self->_subunsub( $cmd );

  return;
}

####
sub punsubscribe {
  my $self = shift;
  my $cmd  = $self->_parse_cmd_args( [ @_ ] );
  $cmd->{keyword} = 'punsubscribe';

  $self->_subunsub( $cmd );

  return;
}

####
sub disconnect {
  my $self = shift;

  $self->_disconnect();

  return;
}

####
sub encoding {
  my $self = shift;

  if ( @_ ) {
    my $enc = shift;

    if ( defined $enc ) {
      $self->{encoding} = find_encoding( $enc );
      unless ( defined $self->{encoding} ) {
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
  my $self = shift;

  if ( @_ ) {
    my $on_error = shift;

    if ( defined $on_error ) {
      $self->{on_error} = $on_error;
    }
    else {
      $self->{on_error} = sub {
        my $err_msg = shift;

        warn "$err_msg\n";
      };
    }
  }

  return $self->{on_error};
}

####
sub selected_database {
  my $self = shift;

  return $self->{database};
}

# Create more accessors
{
  no strict 'refs';

  foreach my $attr_pref ( qw( connection read ) ) {
    my $attr_name = $attr_pref . '_timeout';

    *{$attr_name} = sub {
      my $self = shift;

      if ( @_ ) {
        my $timeout = shift;

        if (
          defined $timeout
            and ( !looks_like_number( $timeout ) or $timeout < 0 )
            ) {
          confess ucfirst( $attr_pref ) . ' timeout must be a positive number';
        }
        $self->{ $attr_name } = $timeout;
      }

      return $self->{ $attr_name };
    }
  }

  foreach my $attr_name (
    qw( reconnect on_connect on_disconnect on_connect_error )
      ) {
    *{$attr_name} = sub {
      my $self = shift;

      if ( @_ ) {
        $self->{ $attr_name } = shift;
      }

      return $self->{ $attr_name };
    }
  }
}

####
sub _connect {
  my $self = shift;

  $self->{_handle} = AnyEvent::Handle->new(
    %{ $self->{handle_params} },
    connect          => [ $self->{host}, $self->{port} ],
    on_prepare       => $self->_get_on_prepare(),
    on_connect       => $self->_get_on_connect(),
    on_connect_error => $self->_get_on_connect_error(),
    on_rtimeout      => $self->_get_on_rtimeout(),
    on_eof           => $self->_get_on_eof(),
    on_error         => $self->_get_on_handle_error(),
    on_read          => $self->_get_on_read(),
  );

  return;
}

####
sub _get_on_prepare {
  my $self = shift;

  weaken( $self );

  return sub {
    if ( defined $self->{connection_timeout} ) {
      return $self->{connection_timeout};
    }

    return;
  };
}

####
sub _get_on_connect {
  my $self = shift;

  weaken( $self );

  return sub {
    $self->{_connected} = 1;
    unless ( defined $self->{password} ) {
      $self->{_auth_st} = S_IS_DONE;
    }
    if ( $self->{database} == 0 ) {
      $self->{_select_db_st} = S_IS_DONE;
    }

    if ( $self->{_auth_st} == S_NEED_PERFORM ) {
      $self->_auth();
    }
    elsif ( $self->{_select_db_st} == S_NEED_PERFORM ) {
      $self->_select;
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
sub _get_on_connect_error {
  my $self = shift;

  weaken( $self );

  return sub {
    my $err_msg = pop;

    $self->_disconnect( "Can't connect to $self->{host}:$self->{port}: $err_msg",
        E_CANT_CONN );
  };
}

####
sub _get_on_rtimeout {
  my $self = shift;

  weaken( $self );

  return sub {
    if ( @{$self->{_process_queue}} ) {
      $self->_disconnect( 'Read timed out.', E_READ_TIMEDOUT );
    }
    else {
      $self->{_handle}->rtimeout( undef );
    }
  };
}

####
sub _get_on_eof {
  my $self = shift;

  weaken( $self );

  return sub {
    $self->_disconnect( 'Connection closed by remote host.',
        E_CONN_CLOSED_BY_REMOTE_HOST );
  };
}

####
sub _get_on_handle_error {
  my $self = shift;

  weaken( $self );

  return sub {
    my $err_msg = pop;

    $self->_disconnect( $err_msg, E_IO );
  };
}

####
sub _get_on_read {
  my $self = shift;

  weaken( $self );

  my $str_len;
  my @bufs;
  my $bufs_num = 0;

  return sub {
    my $handle = shift;

    MAIN: while ( 1 ) {
      if ( $handle->destroyed() ) {
        return;
      }

      my $data;
      my $err_code;

      if ( defined $str_len ) {
        if ( length( $handle->{rbuf} ) < $str_len + EOL_LEN ) {
          return;
        }
        $data = substr( $handle->{rbuf}, 0, $str_len, '' );
        substr( $handle->{rbuf}, 0, EOL_LEN, '' );
        if ( defined $self->{encoding} ) {
          $data = $self->{encoding}->decode( $data );
        }
        undef $str_len;
      }
      else {
        my $eol_pos = index( $handle->{rbuf}, EOL );

        if ( $eol_pos < 0 ) {
          return;
        }
        $data = substr( $handle->{rbuf}, 0, $eol_pos, '' );
        my $type = substr( $data, 0, 1, '' );
        substr( $handle->{rbuf}, 0, EOL_LEN, '' );

        if ( $type ne '+' and $type ne ':' ) {
          if ( $type eq '$' ) {
            if ( $data >= 0 ) {
              $str_len = $data;

              next;
            }

            undef $data;
          }
          elsif ( $type eq '*' ) {
            if ( $data > 0 ) {
              push( @bufs,
                { data        => [],
                  err_code    => undef,
                  chunks_left => $data,
                }
              );
              $bufs_num++;

              next;
            }
            elsif ( $data == 0 ) {
              $data = [];
            }
            else {
              undef $data;
            }
          }
          elsif ( $type eq '-' ) {
            $err_code = E_OPRN_ERROR;
            if ( $data =~ m/^([A-Z]{3,}) / ) {
              if ( exists $ERR_PREFS_MAP{ $1 } ) {
                $err_code = $ERR_PREFS_MAP{ $1 };
              }
            }
          }
          else {
            $self->_disconnect( 'Unexpected reply received.',
                E_UNEXPECTED_DATA );

            return;
          }
        }
      }

      while ( $bufs_num > 0 ) {
        my $curr_buf = $bufs[-1];
        if ( defined $err_code ) {
          unless ( ref( $data ) ) {
            $data = AnyEvent::Redis::RipeRedis::Error->new( $data,
                $err_code );
          }
          $curr_buf->{err_code} = E_OPRN_ERROR;
        }
        push( @{$curr_buf->{data}}, $data );

        if ( --$curr_buf->{chunks_left} > 0 ) {
          next MAIN;
        }
        $data     = $curr_buf->{data};
        $err_code = $curr_buf->{err_code};
        pop @bufs;
        $bufs_num--;
      }

      $self->_process_reply( $data, $err_code );
    }

    return;
  };
}

####
sub _parse_cmd_args {
  my $self = shift;
  my $args = shift;

  my $params = {};
  if ( ref( $args->[-1] ) eq 'CODE' ) {
    $params->{on_reply} = pop( @{$args} );
  }
  elsif ( ref( $args->[-1] ) eq 'HASH' ) {
    $params = pop( @{$args} );
  }
  $params->{args} = $args;

  return $params;
}

####
sub _parse_sub_args {
  my $self = shift;
  my $args = shift;

  my $params = {};
  if ( ref( $args->[-1] ) eq 'CODE' ) {
    $params->{on_message} = pop( @{$args} );
  }
  elsif ( ref( $args->[-1] ) eq 'HASH' ) {
    $params = pop( @{$args} );
  }
  $params->{args} = $args;

  unless ( defined $params->{on_message} ) {
    confess '\'on_message\' callback must be specified';
  }

  return $params;
}

####
sub _subunsub {
  my $self = shift;
  my $cmd  = shift;

  if ( $self->{_multi_lock} ) {
    AE::postpone(
      sub {
        $self->_call_on_cmd_error( $cmd,
            "Command '$cmd->{keyword}' not allowed after 'multi' command."
            . ' First, the transaction must be finalized.', E_OPRN_ERROR );
      }
    );

    return;
  }

  if ( exists $cmd->{on_done} ) {
    my $on_done = $cmd->{on_done};
    $cmd->{on_done} = sub {
      $on_done->( @{$_[0]} );
    }
  }
  $cmd->{replies_left} = scalar( @{$cmd->{args}} );
  $self->_execute_cmd( $cmd );

  return;
}

####
sub _execute_cmd {
  my $self = shift;
  my $cmd  = shift;

  unless ( $self->{_ready_to_write} ) {
    if ( defined $self->{_handle} ) {
      if ( $self->{_connected} ) {
        if ( $self->{_auth_st} == S_IS_DONE ) {
          if ( $self->{_select_db_st} == S_NEED_PERFORM ) {
            $self->_select;
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
          $self->_call_on_cmd_error( $cmd, "Operation '$cmd->{keyword}' aborted:"
              . ' No connection to the server.', E_NO_CONN );
        }
      );

      return;
    }

    push( @{$self->{_input_queue}}, $cmd );

    return;
  }

  $self->_push_write( $cmd );

  return;
}

####
sub _push_write {
  my $self = shift;
  my $cmd  = shift;

  my $cmd_str = '';
  foreach my $token ( $cmd->{keyword}, @{$cmd->{args}} ) {
    unless ( defined $token ) {
      $token = '';
    }
    elsif ( defined $self->{encoding} and is_utf8( $token ) ) {
      $token = $self->{encoding}->encode( $token );
    }
    $cmd_str .= '$' . length( $token ) . EOL . $token . EOL;
  }
  $cmd_str = '*' . ( scalar( @{$cmd->{args}} ) + 1 ) . EOL . $cmd_str;

  if ( defined $self->{read_timeout} and !@{$self->{_process_queue}} ) {
    $self->{_handle}->rtimeout_reset();
    $self->{_handle}->rtimeout( $self->{read_timeout} );
  }

  push( @{$self->{_process_queue}}, $cmd );
  $self->{_handle}->push_write( $cmd_str );

  return;
}

####
sub _auth {
  my $self = shift;

  weaken( $self );

  $self->{_auth_st} = S_IN_PROGRESS;

  $self->_push_write(
    { keyword => 'auth',
      args    => [ $self->{password} ],
      on_done => sub {
        $self->{_auth_st} = S_IS_DONE;
        if ( $self->{_select_db_st} == S_NEED_PERFORM ) {
          $self->_select;
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
sub _select {
  my $self = shift;

  weaken( $self );

  $self->{_select_db_st} = S_IN_PROGRESS;
  $self->_push_write(
    { keyword => 'select',
      args    => [ $self->{database} ],
      on_done => sub {
        $self->{_select_db_st} = S_IS_DONE;
        $self->{_ready_to_write} = 1;
        $self->_flush_input_queue();
      },
      on_error => sub {
        $self->{_select_db_st} = S_NEED_PERFORM;
        $self->_abort_all( @_ );
      },
    }
  );

  return;
}

####
sub _flush_input_queue {
  my $self = shift;

  $self->{_tmp_queue} = $self->{_input_queue};
  $self->{_input_queue} = [];
  while ( my $cmd = shift( @{$self->{_tmp_queue}} ) ) {
    $self->_push_write( $cmd );
  }

  return;
}

####
sub _process_reply {
  my $self = shift;
  my $data     = shift;
  my $err_code = shift;

  if ( defined $err_code ) {
    my $cmd = shift( @{$self->{_process_queue}} );

    unless ( defined $cmd ) {
      $self->_disconnect( 'Don\'t known how process error.'
          . ' Command queue is empty.', E_UNEXPECTED_DATA );
      return;
    }

    if ( $err_code == E_NO_SCRIPT and exists $cmd->{script} ) {
      $cmd->{keyword} = 'eval';
      $cmd->{args}[0] = $cmd->{script};
      $self->_push_write( $cmd );

      return;
    }

    $self->_call_on_cmd_error( $cmd, ref( $data )
        ? ( "Operation '$cmd->{keyword}' completed with errors.", $err_code,
        $data ) : ( $data, $err_code ) );
  }
  elsif (
    $self->{_subs_num} > 0 and ref( $data )
      and exists $MSG_TYPES{ $data->[0] }
      ) {
    my $on_msg = $self->{_subs}{ $data->[1] };

    unless ( defined $on_msg ) {
      $self->_disconnect( 'Don\'t known how process published message.'
          ." Unknown channel or pattern '$data->[1]'.", E_UNEXPECTED_DATA );
      return;
    }

    $self->_call_on_message( $on_msg, $data );
  }
  else {
    my $cmd = $self->{_process_queue}[0];

    unless ( defined $cmd ) {
      $self->_disconnect( 'Don\'t known how process reply.'
          .' Command queue is empty.', E_UNEXPECTED_DATA );
      return;
    }

    if ( !defined $cmd->{replies_left} or --$cmd->{replies_left} == 0 ) {
      shift( @{$self->{_process_queue}} );
    }

    if ( exists $NEED_POSTPROC{ $cmd->{keyword} } ) {
      if ( exists $SUBUNSUB_CMDS{ $cmd->{keyword} } ) {
        shift( @{$data} );

        if ( exists $SUB_CMDS{ $cmd->{keyword} } ) {
          $self->{_subs}{ $data->[0] } = $cmd->{on_message};
        }
        else {
          delete( $self->{_subs}{ $data->[0] } );
        }
        $self->{_subs_num} = $data->[1];
      }
      elsif ( $cmd->{keyword} eq 'select' ) {
        $self->{database} = $cmd->{args}[0];
      }
      else { # quit
        $self->_disconnect();
      }
    }

    $self->_call_on_cmd_done( $cmd, $data );
  }

  return;
}

####
sub _call_on_error {
  my $self = shift;

  if ( $_[1] == E_CANT_CONN and defined $self->{on_connect_error} ) {
    $self->{on_connect_error}->( $_[0] );
  }
  else {
    $self->{on_error}->( @_ );
  }

  return;
}

####
sub _call_on_cmd_error {
  my $self = shift;
  my $cmd  = shift;

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
sub _call_on_message {
  my $on_msg = $_[1];

  $on_msg->( $_[2][0] eq 'pmessage' ? @{$_[2]}[ 2, 3, 1 ]
      : @{$_[2]}[ 1, 2 ] );

  return;
}

####
sub _call_on_cmd_done {
  my $cmd = $_[1];

  if ( defined $cmd->{on_done} ) {
    $cmd->{on_done}->( $_[2] );
  }
  elsif ( defined $cmd->{on_reply} ) {
    $cmd->{on_reply}->( $_[2] );
  }

  return;
}

####
sub _disconnect {
  my $self     = shift;
  my $err_msg  = shift;
  my $err_code = shift;

  my $was_connected = $self->{_connected};

  if ( defined $self->{_handle} ) {
    $self->{_handle}->destroy();
    undef $self->{_handle};
  }
  $self->{_connected}      = 0;
  $self->{_auth_st}        = S_NEED_PERFORM;
  $self->{_select_db_st}   = S_NEED_PERFORM;
  $self->{_ready_to_write} = 0;
  $self->{_multi_lock}     = 0;

  $self->_abort_all( $err_msg, $err_code );

  if ( $was_connected and defined $self->{on_disconnect} ) {
    $self->{on_disconnect}->();
  }

  return;
}

####
sub _abort_all {
  my $self     = shift;
  my $err_msg  = shift;
  my $err_code = shift;

  my @unfin_cmds = (
    @{$self->{_process_queue}},
    @{$self->{_tmp_queue}},
    @{$self->{_input_queue}},
  );

  $self->{_input_queue}   = [];
  $self->{_tmp_queue}     = [];
  $self->{_process_queue} = [];
  $self->{_subs}          = {};
  $self->{_subs_num}      = 0;

  if ( !defined $err_msg and @unfin_cmds ) {
    $err_msg  = 'Connection closed by client prematurely.';
    $err_code = E_CONN_CLOSED_BY_CLIENT;
  }
  if ( defined $err_msg ) {
    $self->_call_on_error( $err_msg, $err_code );

    foreach my $cmd ( @unfin_cmds ) {
      $self->_call_on_cmd_error( $cmd,
          "Operation '$cmd->{keyword}' aborted: $err_msg", $err_code );
    }
  }

  return;
}

####
sub AUTOLOAD {
  our $AUTOLOAD;
  my $method = $AUTOLOAD;
  $method =~ s/^.+:://;
  my $keyword = lc( $method );

  my $sub = sub {
    my $self = shift;
    my $cmd = $self->_parse_cmd_args( [ @_ ] );
    $cmd->{keyword} = $keyword;

    $self->_execute_cmd( $cmd );

    return;
  };

  do {
    no strict 'refs';
    *{$method} = $sub;
  };

  goto &{$sub};
}

####
sub DESTROY {
  my $self = shift;

  if ( defined $self->{_handle} ) {
    my @unfin_cmds = (
      @{$self->{_process_queue}},
      @{$self->{_tmp_queue}},
      @{$self->{_input_queue}},
    );

    foreach my $cmd ( @unfin_cmds ) {
      warn "Operation '$cmd->{keyword}' aborted:"
          . " Client object destroyed prematurely.\n";
    }
  }

  return;
}


####
package AnyEvent::Redis::RipeRedis::Error;

# Constructor
sub new {
  my $class    = shift;
  my $err_msg  = shift;
  my $err_code = shift;

  my $self = bless {}, $class;

  $self->{message} = $err_msg;
  $self->{code}    = $err_code;

  return $self;
}

####
sub message {
  my $self = shift;

  return $self->{message};
}

####
sub code {
  my $self = shift;

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
        my $data = shift;

        print "$data\n";
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
      my $data    = shift;
      my $err_msg = shift;

      if ( defined $err_msg ) {
        my $err_code = shift;

        # error handling...

        $cv->croak( $err_msg );

        return;
      }

      print "$data\n";
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

=item encoding

Used for encode/decode strings at time of input/output operations.

Not set by default.

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

If enabled, the connection establishes at time, when you will send the first
command to the server. By default the connection establishes after calling of
the C<new> method.

Disabled by default.

=item reconnect

If the connection to the Redis server was lost and the parameter C<reconnect> is
TRUE, then the client try to restore the connection on a next command executuion.
The client try to reconnect only once and if it fails, then is called the
C<on_error> callback. If you need several attempts of the reconnection, just
retry a command from the C<on_error> callback as many times, as you need. This
feature made the client more responsive.

Enabled by default.

=item handle_params

The hash reference with parameters, that will be passed to L<AnyEvent::Handle>
constructor.

  handle_params => {
    linger   => 60,
    autocork => 1,
  },

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
        my $data = shift;

        print "$data\n";
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
        my $data = shift;

        foreach my $val ( @{$data}  ) {
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
      my $data    = shift;
      my $err_msg = shift;

      if ( defined $err_msg ) {
        my $err_code = shift;

        # error handling...

        return;
      }

      print "$data\n";
    },
  );

  $redis->incr( 'bar',
    { on_reply => sub {
        my $data    = shift;
        my $err_msg = shift;

        if ( defined $err_msg ) {
          my $err_code = shift;

          # error handling...

          return;
        }

        print "$data\n";
      },
    }
  );

=over

=item on_done => $cb->( [ $data ] )

The C<on_done> callback is called, when the current operation was completed
successfully.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, when some error occurred.

=item on_reply => $cb->( [ $data, ][ $err_msg, $err_code ] )

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
        my $data     = shift;

        foreach my $chunk ( @{$data} ) {
          if ( ref( $chunk ) eq 'AnyEvent::Redis::RipeRedis::Error' ) {
            my $oprn_err_msg  = $chunk->message();
            my $oprn_err_code = $chunk->code();

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
      my $data   = shift;
      my $err_msg = shift;

      if ( defined $err_msg ) {
        my $err_code = shift;

        foreach my $chunk ( @{$data} ) {
          if ( ref( $chunk ) eq 'AnyEvent::Redis::RipeRedis::Error' ) {
            my $oprn_err_msg  = $chunk->message();
            my $oprn_err_code = $chunk->code();

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
        my $data    = shift;
        my $err_msg = shift;

        if ( defined $err_msg ) {
          my $err_code = shift;

          # error handling...

          return;
        }

        my $ch_name  = $data->[0];
        my $subs_num = $data->[1];

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

=item on_reply => $cb->( [ $data, ][ $err_msg, $err_code ] )

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
        my $data    = shift;
        my $err_msg = shift;

        if ( defined $err_msg ) {
          my $err_code = shift;

          # error handling...

          return;
        }

        my $ch_pattern = $data->[0];
        my $subs_num   = $data->[1];

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

=item on_reply => $cb->( [ $data, ][ $err_msg, $err_code ] )

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
      my $data    = shift;
      my $err_msg = shift;

      if ( defined $err_msg ) {
        my $err_code = shift;

        # error handling...

        return;
      }

      my $ch_name  = $data->[0];
      my $subs_num = $data->[1];

      # handling...
    }
  );

=over

=item on_done => $cb->( $ch_name, $sub_num )

The C<on_done> callback is called on every specified channel, when the
unsubscription operation was completed successfully.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, if the unsubscription operation fails.

=item on_reply => $cb->( [ $data, ][ $err_msg, $err_code ] )

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
      my $data    = shift;
      my $err_msg = shift;

      if ( defined $err_msg ) {
        my $err_code = shift;

        # error handling...

        return;
      }

      my $ch_pattern = $data->[0];
      my $subs_num   = $data->[1];

      # handling...
    }
  );

=over

=item on_done => $cb->( $ch_name, $sub_num )

The C<on_done> callback is called on every specified pattern, when the
unsubscription operation was completed successfully.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, if the unsubscription operation fails.

=item on_reply => $cb->( [ $data, ][ $err_msg, $err_code ] )

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
        my $data     = shift;

        foreach my $chunk ( @{$data} ) {
          if ( ref( $chunk ) eq 'AnyEvent::Redis::RipeRedis::Error' ) {
            my $nested_err_msg  = $chunk->message();
            my $nested_err_code = $chunk->code();

            # error handling...
          }
        }
      }
    }
  );

  $redis->eval( "return { 'foo', redis.error_reply( 'Error.' ) }", 0,
    sub {
      my $data    = shift;
      my $err_msg = shift;

      if ( defined $err_msg ) {
        my $err_code = shift;

        foreach my $chunk ( @{$data} ) {
          if ( ref( $chunk ) eq 'AnyEvent::Redis::RipeRedis::Error' ) {
            my $nested_err_msg  = $chunk->message();
            my $nested_err_code = $chunk->code();

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
        my $data = shift;

        foreach my $val ( @{$data}  ) {
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
case a client object is destroyed without calling any callbacks, including
the C<on_disconnect> callback, to avoid an unexpected behavior.

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

Enables or disables reconnection mode of the client.

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

=item *

Ivan Kruglov

=back

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2012-2014, Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>.
All rights reserved.

This module is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut
