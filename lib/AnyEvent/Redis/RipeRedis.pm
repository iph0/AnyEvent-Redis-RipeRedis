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
  _input_buf
  _tmp_buf
  _processing_queue
  _sub_lock
  _subs
);

our $VERSION = '1.254';

use AnyEvent;
use AnyEvent::Handle;
use Encode qw( find_encoding is_utf8 );
use Scalar::Util qw( looks_like_number weaken );
use Digest::SHA qw( sha1_hex );
use Carp qw( confess );

BEGIN {
  our @EXPORT_OK = qw( E_CANT_CONN E_LOADING_DATASET E_IO
      E_CONN_CLOSED_BY_REMOTE_HOST E_CONN_CLOSED_BY_CLIENT E_NO_CONN
      E_OPRN_ERROR E_UNEXPECTED_DATA E_NO_SCRIPT E_READ_TIMEDOUT );

  our %EXPORT_TAGS = (
    err_codes => \@EXPORT_OK,
  );
}

use constant {
  # Default values
  D_HOST => 'localhost',
  D_PORT => 6379,

  # Error codes
  E_CANT_CONN => 1,
  E_LOADING_DATASET => 2,
  E_IO => 3,
  E_CONN_CLOSED_BY_REMOTE_HOST => 4,
  E_CONN_CLOSED_BY_CLIENT => 5,
  E_NO_CONN => 6,
  E_OPRN_ERROR => 9,
  E_UNEXPECTED_DATA => 10,
  E_NO_SCRIPT => 11,
  E_READ_TIMEDOUT => 12,

  # Command status
  S_NEED_PERFORM => 1,
  S_IN_PROGRESS => 2,
  S_IS_DONE => 3,

  # Flags
  F_ERROR_REPLY => 1,
  F_SAFE_DISCONN => 1,

  # String terminator
  EOL => "\r\n",
  EOL_LEN => 2,
};

my %SUB_CMDS = (
  subscribe => 1,
  psubscribe => 1,
);
my %SUB_UNSUB_CMDS = (
  %SUB_CMDS,
  unsubscribe => 1,
  punsubscribe => 1,
);

my %SPEC_CMDS = (
  exec => 1,
  multi => 1,
  %SUB_UNSUB_CMDS,
);

my %EVAL_CACHE;


# Constructor
sub new {
  my $proto = shift;
  my $params = { @_ };

  my __PACKAGE__ $self = fields::new( $proto );

  $self->{host} = $params->{host} || D_HOST;
  $self->{port} = $params->{port} || D_PORT;
  $self->{password} = $params->{password};
  $self->{database} = $params->{database};
  $self->connection_timeout( $params->{connection_timeout} );
  $self->read_timeout( $params->{read_timeout} );
  if ( !exists( $params->{reconnect} ) ) {
    $params->{reconnect} = 1;
  }
  $self->{reconnect} = $params->{reconnect};
  $self->encoding( $params->{encoding} );
  $self->{on_connect} = $params->{on_connect};
  $self->{on_disconnect} = $params->{on_disconnect};
  $self->{on_connect_error} = $params->{on_connect_error};
  $self->on_error( $params->{on_error} );

  $self->{_handle} = undef;
  $self->{_connected} = 0;
  $self->{_lazy_conn_st} = $params->{lazy};
  $self->{_auth_st} = S_NEED_PERFORM;
  $self->{_db_select_st} = S_NEED_PERFORM;
  $self->{_ready_to_write} = 0;
  $self->{_input_buf} = [];
  $self->{_tmp_buf} = [];
  $self->{_processing_queue} = [];
  $self->{_sub_lock} = 0;
  $self->{_subs} = {};

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
  if ( ref( $args[-1] ) eq 'HASH' ) {
    $cmd = pop( @args );
  }
  $cmd->{name} = 'evalsha';
  $cmd->{args} = \@args;

  $cmd->{script} = $args[0];
  if ( !exists( $EVAL_CACHE{$cmd->{script}} ) ) {
    $EVAL_CACHE{$cmd->{script}} = sha1_hex( $cmd->{script} );
  }
  $args[0] = $EVAL_CACHE{$cmd->{script}};

  if ( !defined( $cmd->{on_error} ) ) {
    $cmd->{on_error} = $self->{on_error};
  }

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
sub connection_timeout {
  my __PACKAGE__ $self = shift;

  if ( @_ ) {
    my $conn_timeout = shift;
    if (
      defined( $conn_timeout )
        and ( !looks_like_number( $conn_timeout ) or $conn_timeout < 0 )
        ) {
      confess 'Connection timeout must be a positive number.';
    }
    $self->{connection_timeout} = $conn_timeout;
  }

  return $self->{connection_timeout};
}

####
sub read_timeout {
  my __PACKAGE__ $self = shift;

  if ( @_ ) {
    my $read_timeout = shift;
    if (
      defined( $read_timeout )
        and ( !looks_like_number( $read_timeout ) or $read_timeout < 0 )
        ) {
      confess 'Read timeout must be a positive number.';
    }
    $self->{read_timeout} = $read_timeout;
  }

  return $self->{read_timeout};
}

####
sub reconnect {
  my __PACKAGE__ $self = shift;

  if ( @_ ) {
    $self->{reconnect} = shift;
  }

  return $self->{reconnect};
}

####
sub encoding {
  my __PACKAGE__ $self = shift;

  if ( @_ ) {
    my $enc = shift;

    if ( defined( $enc ) ) {
      $self->{encoding} = find_encoding( $enc );
      if ( !defined( $self->{encoding} ) ) {
        confess "Encoding '$enc' not found.";
      }
    }
    else {
      undef( $self->{encoding} );
    }
  }

  return $self->{encoding};
}

####
sub on_connect {
  my __PACKAGE__ $self = shift;

  if ( @_ ) {
    $self->{on_connect} = shift;
  }

  return $self->{on_connect};
}

####
sub on_disconnect {
  my __PACKAGE__ $self = shift;

  if ( @_ ) {
    $self->{on_disconnect} = shift;
  }

  return $self->{on_disconnect};
}

####
sub on_connect_error {
  my __PACKAGE__ $self = shift;

  if ( @_ ) {
    $self->{on_connect_error} = shift;
  }

  return $self->{on_connect_error};
}

####
sub on_error {
  my __PACKAGE__ $self = shift;

  if ( @_ ) {
    my $on_error = shift;
    if ( !defined( $on_error ) ) {
      $on_error = sub {
        my $err_msg = shift;
        warn "$err_msg\n";
      };
    }
    $self->{on_error} = $on_error;
  }

  return $self->{on_error};
}

####
sub _connect {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  $self->{_handle} = AnyEvent::Handle->new(
    connect => [ $self->{host}, $self->{port} ],
    on_prepare => $self->_on_prepare(),
    on_connect => $self->_on_connect(),
    on_connect_error => $self->_on_connect_error(),
    on_rtimeout => $self->_on_rtimeout(),
    on_eof => $self->_on_eof(),
    on_error => $self->_on_error(),
    on_read => $self->_on_read(
      sub {
        return $self->_process_response( @_ );
      }
    ),
  );

  return;
}

####
sub _on_prepare {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  return sub {
    if ( defined( $self->{connection_timeout} ) ) {
      return $self->{connection_timeout};
    }

    return;
  };
}

####
sub _on_connect {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  return sub {
    $self->{_connected} = 1;
    if ( !defined( $self->{password} ) ) {
      $self->{_auth_st} = S_IS_DONE;
    }
    if ( !defined( $self->{database} ) ) {
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
      $self->_flush_input_buf();
    }
    if ( defined( $self->{on_connect} ) ) {
      $self->{on_connect}->();
    }
  };
}

####
sub _on_connect_error {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  return sub {
    my $err_msg = pop;

    $self->_process_crit_error( "Can't connect to $self->{host}:$self->{port}: "
        . $err_msg, E_CANT_CONN );
  };
}

####
sub _on_rtimeout {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  return sub {
    if ( @{$self->{_processing_queue}} ) {
      $self->_process_crit_error( 'Read timed out.', E_READ_TIMEDOUT );
    }
    else {
      $self->{_handle}->rtimeout( undef );
    }
  };
}

####
sub _on_eof {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  return sub {
    $self->_process_crit_error( 'Connection closed by remote host.',
        E_CONN_CLOSED_BY_REMOTE_HOST );
  };
}

####
sub _on_error {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  return sub {
    my $err_msg = pop;

    $self->_process_crit_error( $err_msg, E_IO );
  };
}

####
sub _process_crit_error {
  my __PACKAGE__ $self = shift;
  my $err_msg = shift;
  my $err_code = shift;

  $self->_reset_state();
  $self->_abort_cmds( $err_msg, $err_code );

  if ( $err_code == E_CANT_CONN ) {
    if ( defined( $self->{on_connect_error} ) ) {
      $self->{on_connect_error}->( $err_msg );
    }
    else {
      $self->{on_error}->( $err_msg, $err_code );
    }
  }
  else {
    $self->{on_error}->( $err_msg, $err_code );
    if ( defined( $self->{on_disconnect} ) ) {
      $self->{on_disconnect}->();
    }
  }

  return;
}

####
sub _execute_cmd {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;

  if ( !exists( $cmd->{args} ) ) {
    $cmd->{args} = [];
  }
  if ( !defined( $cmd->{on_error} ) ) {
    $cmd->{on_error} = $self->{on_error};
  }

  if ( exists( $SPEC_CMDS{$cmd->{name}} ) ) {
    if ( exists( $SUB_UNSUB_CMDS{$cmd->{name}} ) ) {
      if ( exists( $SUB_CMDS{$cmd->{name}} ) ) {
        if ( !defined( $cmd->{on_message} ) ) {
          confess "'on_message' callback must be specified.";
        }
      }
      if ( $self->{_sub_lock} ) {
        AE::postpone(
          sub {
            $cmd->{on_error}->( "Command '$cmd->{name}' not allowed after 'multi'"
                . ' command. First, the transaction must be completed.',
                E_OPRN_ERROR );
          }
        );

        return;
      }
      $cmd->{resp_remaining} = scalar( @{$cmd->{args}} );
    }
    elsif ( $cmd->{name} eq 'multi' ) {
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
    if ( defined( $self->{_handle} ) ) {
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
          $cmd->{on_error}->( "Can't handle the command '$cmd->{name}'."
              . ' No connection to the server.', E_NO_CONN );
        }
      );

      return;
    }

    push( @{$self->{_input_buf}}, $cmd );
  }

  return;
}

####
sub _auth {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  $self->{_auth_st} = S_IN_PROGRESS;

  $self->_push_write( {
    name => 'auth',
    args => [ $self->{password} ],
    on_done => sub {
      $self->{_auth_st} = S_IS_DONE;
      if ( $self->{_db_select_st} == S_NEED_PERFORM ) {
        $self->_select_db();
      }
      else {
        $self->{_ready_to_write} = 1;
        $self->_flush_input_buf();
      }
    },

    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      $self->{_auth_st} = S_NEED_PERFORM;
      $self->_abort_cmds( $err_msg, $err_code );
      $self->{on_error}->( $err_msg, $err_code );
    },
  } );

  return;
}

####
sub _select_db {
  my __PACKAGE__ $self = shift;

  weaken( $self );

  $self->{_db_select_st} = S_IN_PROGRESS;
  $self->_push_write( {
    name => 'select',
    args => [ $self->{database} ],
    on_done => sub {
      $self->{_db_select_st} = S_IS_DONE;
      $self->{_ready_to_write} = 1;
      $self->_flush_input_buf();
    },

    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      $self->{_db_select_st} = S_NEED_PERFORM;
      $self->_abort_cmds( $err_msg, $err_code );
      $self->{on_error}->( $err_msg, $err_code );
    },
  } );

  return;
}

####
sub _push_write {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;

  my $cmd_str = '';
  my $mbulk_len = 0;
  foreach my $token ( $cmd->{name}, @{$cmd->{args}} ) {
    if ( !defined( $token ) ) {
      $token = '';
    }
    if ( defined( $self->{encoding} ) and is_utf8( $token ) ) {
      $token = $self->{encoding}->encode( $token );
    }
    my $token_len = length( $token );
    $cmd_str .= "\$$token_len" . EOL . $token . EOL;
    $mbulk_len++;
  }
  $cmd_str = "*$mbulk_len" . EOL . $cmd_str;

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
sub _on_read {
  my __PACKAGE__ $self = shift;
  my $cb = shift;

  my $bulk_len;

  weaken( $self );

  return sub {
    while ( 1 ) {
      my $hdl = $self->{_handle};
      # check handle and read buffer for a case of disconnect
      if ( !defined( $hdl ) or !defined( $hdl->{rbuf} ) ) {
        return;
      }

      if ( defined( $bulk_len ) ) {
        my $bulk_eol_len = $bulk_len + EOL_LEN;
        if ( length( $hdl->{rbuf} ) < $bulk_eol_len ) {
          return;
        }

        my $data = substr( $hdl->{rbuf}, 0, $bulk_len, '' );
        substr( $hdl->{rbuf}, 0, EOL_LEN, '' );
        if ( defined( $self->{encoding} ) ) {
          $data = $self->{encoding}->decode( $data );
        }
        undef( $bulk_len );

        return 1 if $cb->( $data );
      }
      else {
        my $eol_pos = index( $hdl->{rbuf}, EOL );
        if ( $eol_pos < 0 ) {
          return;
        }

        my $data = substr( $hdl->{rbuf}, 0, $eol_pos, '' );
        my $type = substr( $data, 0, 1, '' );
        substr( $hdl->{rbuf}, 0, EOL_LEN, '' );

        if ( $type eq '+' or $type eq ':' ) {
          return 1 if $cb->( $data );
        }
        elsif ( $type eq '-' ) {
          return 1 if $cb->( $data, F_ERROR_REPLY );
        }
        elsif ( $type eq '$' ) {
          if ( $data >= 0 ) {
            $bulk_len = $data;
          }
          else {
            return 1 if $cb->();
          }
        }
        elsif ( $type eq '*' ) {
          my $mbulk_len = $data;
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
        else {
          $self->_process_crit_error( 'Unexpected data type received.',
              E_UNEXPECTED_DATA );

          return;
        }
      }
    }

    return;
  };
}

####
sub _unshift_read {
  my __PACKAGE__ $self = shift;
  my $mbulk_len = shift;
  my $cb = shift;

  my $read_cb;
  my $resp_cb;
  my @data;
  my $err_cnt = 0;
  my $remaining = $mbulk_len;

  {
    my $self = $self;
    weaken( $self );

    $resp_cb = sub {
      my $data_chunk = shift;
      my $is_err = shift;

      if ( $is_err ) {
        $err_cnt++;
        if ( ref( $data_chunk ) ne 'ARRAY' ) {
          my $err_code = E_OPRN_ERROR;
          if ( index( $data_chunk, 'NOSCRIPT' ) == 0 ) {
            $err_code = E_NO_SCRIPT;
          }
          $data_chunk = AnyEvent::Redis::RipeRedis::Error->new(
            code => $err_code,
            message => $data_chunk,
          );
        }
      }

      push( @data, $data_chunk );

      $remaining--;
      if (
        ref( $data_chunk ) eq 'ARRAY' and @{$data_chunk}
          and $remaining > 0
          ) {
        $self->{_handle}->unshift_read( $read_cb );
      }
      elsif ( $remaining == 0 ) {
        undef( $read_cb ); # Collect garbage

        if ( $err_cnt > 0 ) {
          $cb->( \@data, F_ERROR_REPLY );
        }
        else {
          $cb->( \@data );
        }

        return 1;
      }
    };
  }
  $read_cb = $self->_on_read( $resp_cb );

  $self->{_handle}->unshift_read( $read_cb );

  return;
}

####
sub _process_response {
  my __PACKAGE__ $self = shift;
  my $data = shift;
  my $is_err = shift;

  if ( $is_err ) {
    $self->_process_cmd_error( $data );
  }
  elsif ( %{$self->{_subs}} and $self->_is_pub_message( $data ) ) {
    $self->_process_pub_message( $data );
  }
  else {
    $self->_process_data( $data );
  }

  return;
}

####
sub _process_data {
  my __PACKAGE__ $self = shift;
  my $data = shift;

  my $cmd = $self->{_processing_queue}[0];

  if ( !defined( $cmd ) ) {
    $self->_process_crit_error( "Don't known how process response data."
        . ' Command queue is empty.', E_UNEXPECTED_DATA );

    return;
  }

  if ( exists( $SUB_UNSUB_CMDS{$cmd->{name}} ) ) {
    if ( --$cmd->{resp_remaining} == 0 ) {
      shift( @{$self->{_processing_queue}} );
    }

    if ( exists( $SUB_CMDS{$cmd->{name}} ) ) {
      $self->{_subs}{$data->[1]} = $cmd->{on_message};
    }
    elsif ( exists( $self->{_subs}{$data->[1]} ) ) {
      delete( $self->{_subs}{$data->[1]} );
    }
    if ( defined( $cmd->{on_done} ) ) {
      $cmd->{on_done}->( $data->[1], $data->[2] );
    }
  }
  else {
    shift( @{$self->{_processing_queue}} );

    if ( $cmd->{name} eq 'quit' ) {
      $self->_disconnect();
    }
    if ( defined( $cmd->{on_done} ) ) {
      $cmd->{on_done}->( $data );
    }
  }

  return;
}

####
sub _process_pub_message {
  my __PACKAGE__ $self = shift;
  my $data = shift;

  if ( !exists( $self->{_subs}{$data->[1]} ) ) {
    $self->_process_crit_error( "Don't known how process published message."
        . " Unknown channel or pattern '$data->[1]'.", E_UNEXPECTED_DATA );

    return;
  }

  my $msg_cb = $self->{_subs}{$data->[1]};
  if ( $data->[0] eq 'message' ) {
    $msg_cb->( $data->[1], $data->[2] );
  }
  else {
    $msg_cb->( $data->[2], $data->[3], $data->[1] );
  }

  return;
}

####
sub _process_cmd_error {
  my __PACKAGE__ $self = shift;
  my $data = shift;

  my $cmd = shift( @{$self->{_processing_queue}} );

  if ( !defined( $cmd ) ) {
    $self->_process_crit_error( "Don't known how process error."
        . " Command queue is empty.", E_UNEXPECTED_DATA );

    return;
  }

  if ( ref( $data ) eq 'ARRAY' ) {
    $cmd->{on_error}->( "Operation '$cmd->{name}' completed with errors.",
        E_OPRN_ERROR, $data );

    return;
  }

  my $err_code = E_OPRN_ERROR;
  if ( index( $data, 'NOSCRIPT' ) == 0 ) {
    $err_code = E_NO_SCRIPT;
    if ( exists( $cmd->{script} ) ) {
      $cmd->{name} = 'eval';
      $cmd->{args}[0] = $cmd->{script};
      $self->_push_write( $cmd );

      return;
    }
  }
  elsif ( index( $data, 'LOADING' ) == 0 ) {
    $err_code = E_LOADING_DATASET;
  }

  $cmd->{on_error}->( $data, $err_code );

  return;
}

####
sub _is_pub_message {
  my $data = pop;

  return ( ref( $data ) eq 'ARRAY' and ( $data->[0] eq 'message'
      or $data->[0] eq 'pmessage' ) );
}

####
sub _flush_input_buf {
  my __PACKAGE__ $self = shift;

  $self->{_tmp_buf} = $self->{_input_buf};
  $self->{_input_buf} = [];
  while ( my $cmd = shift( @{$self->{_tmp_buf}} ) ) {
    $self->_push_write( $cmd );
  }

  return;
}

####
sub _disconnect {
  my __PACKAGE__ $self = shift;
  my $safe_disconn = shift;

  my $was_connected = $self->{_connected};
  $self->_reset_state();
  $self->_abort_cmds( 'Connection closed by client.', E_CONN_CLOSED_BY_CLIENT,
      $safe_disconn );
  if (
    $was_connected and !$safe_disconn
      and defined( $self->{on_disconnect} )
      ) {
    $self->{on_disconnect}->();
  }

  return;
}

####
sub _reset_state {
  my __PACKAGE__ $self = shift;

  if ( defined( $self->{_handle} ) ) {
    $self->{_handle}->destroy();
    undef( $self->{_handle} );
  }
  $self->{_connected} = 0;
  $self->{_auth_st} = S_NEED_PERFORM;
  $self->{_db_select_st} = S_NEED_PERFORM;
  $self->{_ready_to_write} = 0;
  $self->{_sub_lock} = 0;
  $self->{_subs} = {};

  return;
}

####
sub _abort_cmds {
  my __PACKAGE__ $self = shift;
  my $err_msg = shift;
  my $err_code = shift;
  my $safe_abort = shift;

  my @cmds = (
    @{$self->{_processing_queue}},
    @{$self->{_tmp_buf}},
    @{$self->{_input_buf}},
  );
  $self->{_input_buf} = [];
  $self->{_tmp_buf} = [],
  $self->{_processing_queue} = [];
  foreach my $cmd ( @cmds ) {
    my $full_err_msg = "Operation '$cmd->{name}' aborted: $err_msg";
    if ( !$safe_abort ) {
      $cmd->{on_error}->( $full_err_msg, $err_code );
    }
    else {
      warn "$full_err_msg\n";
    }
  }

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
    my @args = @_;

    my $cmd = {};
    if ( ref( $args[-1] ) eq 'HASH' ) {
      $cmd = pop( @args );
    }
    $cmd->{name} = $cmd_name;
    $cmd->{args} = \@args;

    $self->_execute_cmd( $cmd );

    return;
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

  # Check whether the object was created entirely
  if ( defined( $self->{_subs} ) ) {
    # Disconnect without calling any callbacks
    $self->_disconnect( F_SAFE_DISCONN );
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
  my $proto = shift;
  my %params = @_;

  my __PACKAGE__ $self = fields::new( $proto );

  $self->{message} = $params{message};
  $self->{code} = $params{code};

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
    host => 'localhost',
    port => '6379',
    password => 'yourpass',
    encoding => 'utf8',
    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      # handle the error
    },
  );

  # Set value
  $redis->set( 'foo', 'string', {
    on_done => sub {
      print "SET is done\n";
      $cv->send();
    },
    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      # handle the error
    }
  } );

  $cv->recv();

  $redis->disconnect();

=head1 DESCRIPTION

AnyEvent::Redis::RipeRedis is the flexible non-blocking Redis client with
reconnect feature. The client supports subscriptions, transactions and connection
via UNIX-socket.

Requires Redis 1.2 or higher, and any supported event loop.

=head1 CONSTRUCTOR

=head2 new()

  my $redis = AnyEvent::Redis::RipeRedis->new(
    host => 'localhost',
    port => '6379',
    password => 'yourpass',
    database => 7,
    lazy => 1,
    connection_timeout => 5,
    read_timeout => 5,
    reconnect => 1,
    encoding => 'utf8',
    on_connect => sub {
      print "Connected to Redis server\n";
    },
    on_disconnect => sub {
      print "Disconnected from Redis server\n";
    },
    on_connect_error => sub {
      my $err_msg = shift;

      # handle the error
    },
    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      # handle the error
    },
  );

=over

=item host

Server hostname (default: 127.0.0.1)

=item port

Server port (default: 6379)

=item password

If the password is specified, then the C<AUTH> command is sent immediately to
the server after every connection.

=item database

Database index. If the index is specified, then the client is switched to
the specified database immediately after every connection.

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
established. If this collback isn't specified, then the C<on_error> callback is
called with the C<E_CANT_CONN> error code.

Not set by default.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, when any error occurred. If the callback is
not set, the client just print an error message to C<STDERR>.

=back

=head1 COMMAND EXECUTION

=head2 <command>( [ @args[, \%callbacks ] ] )

The full list of the Redis commands can be found here: L<http://redis.io/commands>.

  # Set value
  $redis->set( 'foo', 'string' );

  # Increment
  $redis->incr( 'bar', {
    on_done => sub {
      my $data = shift;
      print "$data\n";
    },
  } );

  # Get list of values
  $redis->lrange( 'list', 0, -1, {
    on_done => sub {
      my $data = shift;
      foreach my $val ( @{$data}  ) {
        print "$val\n";
      }
    },
    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      # handle the error
    },
  } );

=over

=item on_done => $cb->( [ $data ] )

The C<on_done> callback is called, when the current operation is done.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, if any error occurred. If the C<on_error>
callback is not specified here, then the C<on_error> callback
specified in constructor is called.

=back

=head1 TRANSACTIONS

The detailed information abount the Redis transactions can be found here:
L<http://redis.io/topics/transactions>.

=head2 multi( [ \%callbacks ] )

Marks the start of a transaction block. Subsequent commands will be queued for
atomic execution using C<EXEC>.

=head2 exec( [ \%callbacks ] )

Executes all previously queued commands in a transaction and restores the
connection state to normal. When using C<WATCH>, C<EXEC> will execute commands
only if the watched keys were not modified.

If after C<EXEC> command at least one operation fails, then C<on_error> callback
is called with C<E_OPRN_ERROR> error code and with reply data. Reply data is
passed in third argument of C<on_error> callback and will contain errors of
failed operations and replies of successful operations. Errors will be
represented as objects of the class C<AnyEvent::Redis::RipeRedis::Error>. Each
error object has two methods: C<message()> to get error message and C<code()> to
get error code.

  $redis->multi();
  $redis->set( 'foo', 'string' );
  $redis->incr( 'foo' ); # causes an error
  $redis->exec( {
    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;
      my $data = shift;

      warn "$err_msg\n";
      foreach my $reply ( @{$data}  ) {
        if ( ref( $reply ) eq 'AnyEvent::Redis::RipeRedis::Error' ) {
          my $oprn_err_msg = $reply->message();
          my $oprn_err_code = $reply->code();

          # handle the error
        }
      }

      $cv->send();
    },
  } );


=head2 discard( [ \%callbacks ] )

Flushes all previously queued commands in a transaction and restores the
connection state to normal.

If C<WATCH> was used, C<DISCARD> unwatches all keys.

=head2 watch( @keys[, \%callbacks ] )

Marks the given keys to be watched for conditional execution of a transaction.

=head2 unwatch( [ \%callbacks ] )

Forget about all watched keys.

=head1 SUBSCRIPTIONS

The detailed information about Redis Pub/Sub can be found here:
L<http://redis.io/topics/pubsub>

=head2 subscribe( @channels[, \%callbacks ] )

Subscribes the client to the specified channels.

Once the client enters the subscribed state it is not supposed to issue any
other commands, except for additional C<SUBSCRIBE>, C<PSUBSCRIBE>, C<UNSUBSCRIBE>
and C<PUNSUBSCRIBE> commands.

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
    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      # handle the error
    },
  } );

=over

=item on_done => $cb->( $ch_name, $sub_num )

The C<on_done> callback is called on every specified channel, when the current
subscription operation is done.

=item on_message => $cb->( $ch_name, $msg )

The C<on_message> callback is called, when a published message is received.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, if the subscription operation fails. If the
C<on_error> callback is not specified here, then the C<on_error> callback
specified in constructor is called.

=back

=head2 psubscribe( @patterns[, \%callbacks ] )

Subscribes the client to the given patterns.

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
      my $err_msg = shift;
      my $err_code = shift;

      # handle the error
    },
  } );

=over

=item on_done => $cb->( $ch_pattern, $sub_num )

The C<on_done> callback is called on every specified pattern, when the
current subscription operation is done.

=item on_message => $cb->( $ch_name, $msg, $ch_pattern )

The C<on_message> callback is called, when published message is received.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, if the subscription operation fails. If the
C<on_error> callback is not specified here, then the C<on_error> callback
specified in constructor is called.

=back

=head2 publish( $channel, $message[, \%callbacks ] )

Posts a message to the given channel.

=head2 unsubscribe( [ @channels ][, \%callbacks ] )

Unsubscribes the client from the given channels, or from all of them if none
is given.

When no channels are specified, the client is unsubscribed from all the
previously subscribed channels. In this case, a message for every unsubscribed
channel will be sent to the client.

=over

=item on_done => $cb->( $ch_name, $sub_num )

The C<on_done> callback is called on every specified channel, when the
current unsubscription operation is done.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, if the unsubscription operation fails. If
the C<on_error> callback is not specified here, then the C<on_error> callback
specified in constructor is called.

=back

=head2 punsubscribe( [ @patterns ][, \%callbacks ] )

Unsubscribes the client from the given patterns, or from all of them if none
is given.

When no patters are specified, the client is unsubscribed from all the
previously subscribed patterns. In this case, a message for every unsubscribed
pattern will be sent to the client.

=over

=item on_done => $cb->( $ch_name, $sub_num )

The C<on_done> callback is called on every specified pattern, when the
current unsubscription operation is done.

=item on_error => $cb->( $err_msg, $err_code )

The C<on_error> callback is called, if the unsubscription operation fails. If
the C<on_error> callback is not specified here, then the C<on_error> callback
specified in constructor is called.

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
C<on_error> callback is called with C<E_OPRN_ERROR> error code and with reply
data. Reply data is passed in third argument of C<on_error> callback and will
contain returned errors and other data. Errors will be represented as objects of
the class C<AnyEvent::Redis::RipeRedis::Error>. Each error object has two
methods: C<message()> to get error message and C<code()> to get error code.

  $redis->eval( "return { 'foo', redis.error_reply( 'Error.' ) }", 0, {
    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;
      my $data = shift;

      warn "$err_msg\n";
      foreach my $reply ( @{$data}  ) {
        if ( ref( $reply ) eq 'AnyEvent::Redis::RipeRedis::Error' ) {
          my $oprn_err_msg = $reply->message();
          my $oprn_err_code = $reply->code();

          # handle the error
        }
      }
      $cv->send();
    }
  } );

=head2 eval_cached( $script, $numkeys[, [ @keys, ][ @args, ]\%callbacks ] );

When you call the C<eval_cached()> method, the client first generate a SHA1
hash for a Lua script and cache it in memory. Then the client optimistically
send the C<EVALSHA> command under the hood. If the C<NO_SCRIPT> error will be
returned, the client send the C<EVAL> command.

If you call the C<eval_cached()> method with the same Lua script, client don not
generate a SHA1 hash for this script repeatedly, it gets a hash from the cache
instead.

  $redis->eval_cached( 'return { KEYS[1], KEYS[2], ARGV[1], ARGV[2] }',
      2, 'key1', 'key2', 'first', 'second', {
    on_done => sub {
      my $data = shift;
      foreach my $val ( @{$data}  ) {
        print "$val\n";
      }
    }
  } );

Be care, passing a different Lua scripts to C<eval_cached()> method every time
cause memory leaks.

=head1 ERROR CODES

Every time when the calback C<on_error> is called, the current error code is
passed to it in the second argument. Error codes can be used for programmatic
handling of errors.

AnyEvent::Redis::RipeRedis provides constants of error codes, which can be
imported and used in expressions.

  use AnyEvent::Redis::RipeRedis qw( :err_codes );

Error codes and constants corresponding to them:

  1  - E_CANT_CONN
  2  - E_LOADING_DATASET
  3  - E_IO
  4  - E_CONN_CLOSED_BY_REMOTE_HOST
  5  - E_CONN_CLOSED_BY_CLIENT
  6  - E_NO_CONN
  9  - E_OPRN_ERROR
  10 - E_UNEXPECTED_DATA
  11 - E_NO_SCRIPT
  12 - E_READ_TIMEDOUT

=over

=item E_CANT_CONN

Can not connect to the server. All operations were aborted.

=item E_LOADING_DATASET

Redis is loading the dataset in memory.

=item E_IO

Input/Output operation error. The connection to the Redis server was closed and
all operations were aborted.

=item E_CONN_CLOSED_BY_REMOTE_HOST

The connection closed by remote host. All operations were aborted.

=item E_CONN_CLOSED_BY_CLIENT

Uncompleted operations were aborted at time of calling C<disconnect()> method
or after executing C<QUIT> command.

=item E_NO_CONN

No connection to the Redis server. Connection was lost by any reason on previous
operation.

=item E_OPRN_ERROR

Operation error. For example, wrong number of arguments for a command.

=item E_UNEXPECTED_DATA

The client received unexpected data from the server. The connection to the Redis
server was closed and all operations were aborted.

=item E_NO_SCRIPT

No matching script. Use the C<EVAL> command.

=item E_READ_TIMEDOUT

Read timed out. The connection to the Redis server was closed and all operations
were aborted.

=back

=head1 DISCONNECTION

When the connection to the server is no longer needed you can close it in three
ways: call the method C<disconnect()>, send the C<QUIT> command or you can just
"forget" any references to an AnyEvent::Redis::RipeRedis object, but in this
case a client object is destroyed without calling any callbacks including
the C<on_disconnect> callback to avoid an unexpected behavior.

=head2 disconnect()

The method for synchronous disconnection.

  $redis->disconnect();

=head1 OTHER METHODS

=head2 connection_timeout( $seconds )

Get, set or reset to default the C<connection_timeout> of the client.

=head2 read_timeout( $seconds )

Get, set or disable the C<read_timeout> of the client.

=head2 reconnect( $boolean )

Enable or disable reconnection mode of the client.

=head2 encoding( $enc_name )

Get, set or disable the C<encoding>.

=head2 on_connect( $callback )

Get, set or disable the C<on_connect> callback.

=head2 on_disconnect( $callback )

Get, set or disable the C<on_disconnect> callback.

=head2 on_connect_error( $callback )

Get, set or disable the C<on_connect_error> callback.

=head2 on_error( $callback )

Get, set or disable the C<on_error> callback.

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
