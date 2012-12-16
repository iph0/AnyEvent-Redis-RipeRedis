use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 12;
use Test::AnyEvent::RedisHandle;
use Test::AnyEvent::RedisEmulator;
use Test::AnyEvent::EVLoop;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
use Scalar::Util qw( weaken );

my $T_CLASS = 'AnyEvent::Redis::RipeRedis';

my %GENERIC_PARAMS = (
  host => 'localhost',
  port => '6379',
);

t_no_connection();
t_reconnect();
t_broken_connection();
t_cmd_on_error();
t_invalid_password();
t_oprn_not_permitted();
t_sub_after_multi();
t_conn_closed_by_client();
t_loading_dataset();
t_invalid_db_index();
t_read_timeout();


# Subroutines

####
sub t_no_connection {
  Test::AnyEvent::RedisHandle->down_connection();

  my $redis;
  my @t_errors;

  ev_loop(
    sub {
      my $cv = shift;

      $redis = $T_CLASS->new(
        %GENERIC_PARAMS,
        reconnect => 0,

        on_connect_error => sub {
          my $err_msg = shift;
          push( @t_errors, $err_msg );
        },

        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_errors, [ $err_msg, $err_code ] );
        },
      );

      $redis->ping( {
        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_errors, [ $err_msg, $err_code ] );
          $cv->send();
        }
      } );
    },
  );

  ev_loop(
    sub {
      my $cv = shift;

      $redis->ping( {
        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_errors, [ $err_msg, $err_code ] );
          $cv->send();
        }
      } );
    },
  );

  Test::AnyEvent::RedisHandle->up_connection();

  is_deeply( \@t_errors, [
    [ "Command 'ping' aborted: Can't connect to localhost:6379:"
        . " Connection timed out", E_CANT_CONN ],
    "Can't connect to localhost:6379: Connection timed out",
    [ "Can't handle the command 'ping'. No connection to the server", E_NO_CONN ],
  ], "Can't connect" );

  return;
}

####
sub t_reconnect {
  my @t_data;
  my $redis;

  ev_loop(
    sub {
      my $cv = shift;

      $redis = $T_CLASS->new(
        %GENERIC_PARAMS,
        password => 'test',

        on_connect => sub {
          push( @t_data, 'Connected' );
        },

        on_disconnect => sub {
          push( @t_data, 'Disconnected' );
          Test::AnyEvent::RedisHandle->up_connection();
          $cv->send();
        },

        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_data, [ $err_msg, $err_code ] );
        },
      );

      $redis->ping( {
        on_done => sub {
          Test::AnyEvent::RedisHandle->down_connection();
        }
      } );
    },
  );

  ev_loop(
    sub {
      my $cv = shift;

      $redis->ping( {
        on_done => sub {
          my $resp = shift;
          push( @t_data, $resp );
          $cv->send();
        }
      } );
    },
  );

  is_deeply( \@t_data, [
    'Connected',
    [ 'Connection closed by remote host', E_CONN_CLOSED_BY_REMOTE_HOST ],
    'Disconnected',
    'Connected',
    'PONG',
  ], 'Reconnect' );

  return;
}

####
sub t_broken_connection {
  my @t_data;
  my $redis;
  ev_loop(
    sub {
      my $cv = shift;

      $redis = $T_CLASS->new(
        %GENERIC_PARAMS,
        password => 'test',

        on_connect => sub {
          push( @t_data, 'Connected' );
        },

        on_disconnect => sub {
          push( @t_data, 'Disconnected' );
        },

        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_data, [ $err_msg, $err_code ] );
          $cv->send();
        },
      );

      {
        my $redis = $redis;
        weaken( $redis );
        $redis->ping( {
          on_done => sub {
            Test::AnyEvent::RedisHandle->down_connection();
            $redis->ping();
          },
        } );
      }
    },
  );

  Test::AnyEvent::RedisHandle->up_connection();

  is_deeply( \@t_data, [
    'Connected',
    [ "Command 'ping' aborted: Broken pipe", E_IO ],
    [ "Broken pipe", E_IO ],
    'Disconnected',
  ], 'Broken connection' );

  return;
}

####
sub t_cmd_on_error {
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    password => 'test',
  );

  local %SIG;
  my $t_err;
  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'bar', 'Some string' );

      $SIG{__WARN__} = sub {
        $t_err = shift;
        chomp( $t_err );
        $cv->send();
      };
      $redis->incr( 'bar' );
    },
  );

  is( $t_err, 'ERR value is not an integer or out of range',
      "Default 'on_error' callback" );

  my @t_errors;
  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi();
      $redis->set( '', undef, {
        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_errors, [ $err_msg, $err_code ] );
        },
      } );
      $redis->incr( 'bar' );
      $redis->exec( {
        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_errors, [ $err_msg, $err_code ] );
          $cv->send();
        },
      } );
    },
  );

  is_deeply( \@t_errors, [
    [ "ERR wrong number of arguments for 'set' command", E_OPRN_ERROR ],
    [ 'ERR value is not an integer or out of range', E_OPRN_ERROR ],
  ], "'on_error' callback in the method of the command" );

  return;
}

####
sub t_invalid_password {
  my $redis;
  my @t_errors;
  ev_loop(
    sub {
      my $cv = shift;

      $redis = $T_CLASS->new(
        %GENERIC_PARAMS,
        password => 'invalid',
        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_errors, [ $err_msg, $err_code ] );
        },
      );

      $redis->ping( {
        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_errors, [ $err_msg, $err_code ] );
          $cv->send();
        }
      } );
    },
  );

  is_deeply( \@t_errors, [
    [ "Command 'ping' aborted: ERR invalid password", E_INVALID_PASS ],
    [ 'ERR invalid password', E_INVALID_PASS ],
  ], 'Invalid password' );

  return;
}

####
sub t_oprn_not_permitted {
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
  );

  my $t_err_msg;
  my $t_err_code;
  ev_loop(
    sub {
      my $cv = shift;

      $redis->ping( {
        on_error => sub {
          $t_err_msg = shift;
          $t_err_code = shift;

          $cv->send();
        }
      } );
    },
  );

  is_deeply( [ $t_err_msg, $t_err_code ], [ 'ERR operation not permitted',
      E_OPRN_NOT_PERMITTED ], 'Operation not permitted' );

  return;
}

####
sub t_sub_after_multi {
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    password => 'test',
  );

  my $t_err_msg;
  my $t_err_code;
  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi();
      $redis->subscribe( 'channel', {
        on_message => sub {
          my $msg = shift;
        },
        on_error => sub {
          $t_err_msg = shift;
          $t_err_code = shift;

          $cv->send();
        }
      } );
    },
  );

  is_deeply( [ $t_err_msg, $t_err_code ], [ "Command 'subscribe' not allowed"
      . " after 'multi' command. First, the transaction must be completed",
      E_OPRN_ERROR ],
      'Invalid context for subscribtion' );

  return;
}

####
sub t_conn_closed_by_client {
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    password => 'test',
  );
  my $t_err_msg;
  my $t_err_code;
  $redis->ping( {
    on_error => sub {
      $t_err_msg = shift;
      $t_err_code = shift;
    },
  } );
  $redis->disconnect();

  is_deeply( [ $t_err_msg, $t_err_code ], [ "Command 'ping' aborted: Connection"
      . " closed by client", E_CONN_CLOSED_BY_CLIENT ],
      'Connection closed by client' );

  return;
}

####
sub t_loading_dataset {
  Test::AnyEvent::RedisEmulator->loading_dataset( 1 );

  my $redis;
  my @t_errors;
  ev_loop(
    sub {
      my $cv = shift;

      $redis = $T_CLASS->new(
        %GENERIC_PARAMS,
        password => 'test',

        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_errors, [ $err_msg, $err_code ] );
          $cv->send();
        },
      );

      $redis->ping( {
        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_errors, [ $err_msg, $err_code ] );
        },
      } );
    },
  );

  Test::AnyEvent::RedisEmulator->loading_dataset( 0 );

  is_deeply( \@t_errors, [
    [ "Command 'ping' aborted: LOADING Redis is loading the dataset in memory",
        E_LOADING_DATASET ],
    [ "LOADING Redis is loading the dataset in memory",
        E_LOADING_DATASET ],
  ], 'Loading dataset' );

  return;
}

####
sub t_invalid_db_index {
  my $redis;
  my @t_errors;
  ev_loop(
    sub {
      my $cv = shift;

      $redis = $T_CLASS->new(
        %GENERIC_PARAMS,
        password => 'test',
        database => 16,

        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_errors, [ $err_msg, $err_code ] );
          $cv->send();
        },
      );

      $redis->ping( {
        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_errors, [ $err_msg, $err_code ] );
        },
      } );
    },
  );

  is_deeply( \@t_errors, [
    [ "Command 'ping' aborted: ERR invalid DB index", E_OPRN_ERROR ],
    [ "ERR invalid DB index", E_OPRN_ERROR ],
  ], 'Invalid DB index' );

  return;
}

####
sub t_read_timeout {
  my $redis;
  my @t_errors;

  ev_loop(
    sub {
      my $cv = shift;

      $redis = $T_CLASS->new(
        %GENERIC_PARAMS,
        password => 'test',
        reconnect => 0,
        read_timeout => 5,

        on_connect => sub {
          Test::AnyEvent::RedisHandle->freeze_connection();
        },

        on_error => sub {
          my $err_msg = shift;
          my $err_code = shift;

          push( @t_errors, [ $err_msg, $err_code ] );
          $cv->send();
        },
      );
    },
  );

  Test::AnyEvent::RedisHandle->thaw_connection();

  is_deeply( \@t_errors, [
    [ "Command 'auth' aborted: Read timed out", E_READ_TIMEDOUT ],
    [ 'Read timed out', E_READ_TIMEDOUT ],
  ], 'Read timed out' );

  return;
}
