use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 10;
use Test::AnyEvent::RedisHandle;
use Test::AnyEvent::RedisEmulator;
use Test::AnyEvent::EVLoop;
use AnyEvent;
use AnyEvent::Redis::RipeRedis qw( :err_codes );

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
t_auth_required();
t_sub_after_multi();
t_conn_closed_on_demand();
t_loading_dataset();


# Subroutines

####
sub t_no_connection {
  Test::AnyEvent::RedisHandle->connection_down( 1 );

  my @t_data;
  my $cv = AnyEvent->condvar();
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    reconnect => 0,

    on_connect_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      push( @t_data, [ $err_msg, $err_code ] );
    },

    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      push( @t_data, [ $err_msg, $err_code ] );
    },
  );
  $redis->ping( {
    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      push( @t_data, [ $err_msg, $err_code ] );
      $cv->send();
    }
  } );
  ev_loop( $cv );

  $redis->ping( {
    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      push( @t_data, [ $err_msg, $err_code ] );
    }
  } );

  Test::AnyEvent::RedisHandle->connection_down( 0 );

  is_deeply( \@t_data, [
    [ "Command 'ping' aborted: Can't connect to localhost:6379:"
        . " Server not responding", E_CANT_CONN ],
    [ "Can't connect to localhost:6379: Server not responding", E_CANT_CONN ],
    [ "Can't handle the command 'ping'. No connection to the server", E_NO_CONN ],
  ], "Can't connect" );

  return;
}

####
sub t_reconnect {
  my @t_data;
  my $cv = AnyEvent->condvar();
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    password => 'test',

    on_connect => sub {
      push( @t_data, 'Connected' );
    },

    on_disconnect => sub {
      push( @t_data, 'Disconnected' );
      Test::AnyEvent::RedisHandle->connection_down( 0 );
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
      Test::AnyEvent::RedisHandle->connection_down( 1 );
    }
  } );
  ev_loop( $cv );

  $cv = AnyEvent->condvar();
  $redis->ping( {
    on_done => sub {
      my $resp = shift;
      push( @t_data, $resp );
      $cv->send();
    }
  } );
  ev_loop( $cv );

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
  my $cv = AnyEvent->condvar();
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    password => 'test',

    on_connect => sub {
      push( @t_data, 'Connected' );
    },

    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      push( @t_data, [ $err_msg, $err_code ] );
      $cv->send();
    },
  );
  $redis->ping( {
    on_done => sub {
      Test::AnyEvent::RedisHandle->broken_connection( 1 );
      $redis->ping();
    },
  } );
  ev_loop( $cv );

  Test::AnyEvent::RedisHandle->broken_connection( 0 );

  is_deeply( \@t_data, [
    'Connected',
    [ "Command 'ping' aborted: Can't write to socket", E_IO_OPERATION ],
    [ "Can't write to socket", E_IO_OPERATION ],
  ], 'Broken connection' );

  return;
}

####
sub t_cmd_on_error {
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    password => 'test',
  );
  $redis->set( 'bar', 'Some string' );

  my $t_err;
  my $cv = AnyEvent->condvar();
  local $SIG{__WARN__} = sub {
    $t_err = shift;
    chomp( $t_err );
    $cv->send();
  };
  $redis->incr( 'bar' );
  ev_loop( $cv );

  is( $t_err, 'ERR value is not an integer or out of range',
      "Default 'on_error' callback" );

  my @t_errors;
  $cv = AnyEvent->condvar();
  $redis->multi();
  $redis->set( {
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
  ev_loop( $cv );

  is_deeply( \@t_errors, [
    [ "ERR wrong number of arguments for 'set' command", E_COMMAND_EXEC ],
    [ 'ERR value is not an integer or out of range', E_COMMAND_EXEC ],
  ], "'on_error' callback in the method of the command" );

  return;
}

####
sub t_invalid_password {
  my @t_errors;
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    password => 'invalid',
    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      push( @t_errors, [ $err_msg, $err_code ] );
    },
  );

  my $cv = AnyEvent->condvar();
  $redis->ping( {
    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      push( @t_errors, [ $err_msg, $err_code ] );
      $cv->send();
    }
  } );
  ev_loop( $cv );

  is_deeply( \@t_errors, [
    [ "Command 'ping' aborted: ERR invalid password", E_INVALID_PASS ],
    [ 'ERR invalid password', E_INVALID_PASS ],
  ], 'Invalid password' );

  return;
}

####
sub t_auth_required {
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
  );

  my $t_err_msg;
  my $t_err_code;
  my $cv = AnyEvent->condvar();
  $redis->ping( {
    on_error => sub {
      $t_err_msg = shift;
      $t_err_code = shift;

      $cv->send();
    }
  } );
  ev_loop( $cv );

  is_deeply( [ $t_err_msg, $t_err_code ], [ 'ERR operation not permitted',
      E_AUTH_REQUIRED ], 'Authentication required' );

  return;
}

####
sub t_sub_after_multi {
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    password => 'test',
  );

  my $cv = AnyEvent->condvar();
  $redis->multi( {
    on_done => sub {
      $cv->send();
    }
  } );
  my $t_err_msg;
  my $t_err_code;
  $redis->subscribe( 'channel', {
    on_error => sub {
      $t_err_msg = shift;
      $t_err_code = shift;

      $cv->send();
    }
  } );
  ev_loop( $cv );

  is_deeply( [ $t_err_msg, $t_err_code ], [ "Command 'subscribe' not allowed"
      . " after 'multi' command. First, the transaction must be completed",
      E_COMMAND_EXEC ],
      'Invalid context for subscribtion' );

  return;
}

####
sub t_conn_closed_on_demand {
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
      . " closed on demand", E_CONN_CLOSED_ON_DEMAND ],
      'Connection closed on demand' );

  return;
}

####
sub t_loading_dataset {
  Test::AnyEvent::RedisEmulator->loading_dataset( 1 );

  my $cv = AnyEvent->condvar();
  my @t_errors;
  my $redis = $T_CLASS->new(
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
  ev_loop( $cv );

  Test::AnyEvent::RedisEmulator->loading_dataset( 0 );

  is_deeply( \@t_errors, [
    [ "Command 'ping' aborted: LOADING Redis is loading the dataset in memory",
        E_LOADING_DATASET ],
    [ "LOADING Redis is loading the dataset in memory",
        E_LOADING_DATASET ],
  ], 'Loading dataset' );

  return;
}
