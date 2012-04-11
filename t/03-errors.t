use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 6;
use Test::AnyEvent::RedisHandle;
use Test::AnyEvent::EVLoop;
use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $T_CLASS = 'AnyEvent::Redis::RipeRedis';

my %GENERIC_PARAMS = (
  host => 'localhost',
  port => '6379',
);

t_no_connection();
t_reconnect();
t_broken_connection();
t_cmd_on_error();
t_empty_password();


# Subroutines

####
sub t_no_connection {
  Test::AnyEvent::RedisHandle->redis_down();

  my @t_data;

  my $cv = AnyEvent->condvar();
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    reconnect => 0,

    on_error => sub {
      my $err = shift;
      push( @t_data, $err );
    },
  );
  $redis->ping( {
    on_error => sub {
      my $err = shift;
      push( @t_data, $err );
      $cv->send();
    }
  } );
  ev_loop( $cv );

  $redis->ping( {
    on_error => sub {
      my $err = shift;
      push( @t_data, $err );
    }
  } );

  Test::AnyEvent::RedisHandle->redis_up();

  is_deeply( \@t_data, [
    "Can't connect to localhost:6379. Server not responding",
    "Can't connect to localhost:6379. Server not responding. Command 'ping' failed",
    "Can't execute command 'ping'. Connection not established"
  ], "Can't connect" );

  return;
}

####
sub t_reconnect {
  Test::AnyEvent::RedisHandle->redis_up();

  my $cv;
  my @t_data;

  $cv = AnyEvent->condvar();
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    reconnect => 1,
    password => 'test',

    on_connect => sub {
      push( @t_data, 'Connected' );
    },

    on_disconnect => sub {
      push( @t_data, 'Disconnected' );
      Test::AnyEvent::RedisHandle->redis_up();
      $cv->send();
    },
  );
  $redis->ping( {
    on_done => sub {
      Test::AnyEvent::RedisHandle->redis_down();
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
    'Disconnected',
    'Connected',
    'PONG',
  ], 'Reconnect' );

  return;
}

####
sub t_broken_connection {
  Test::AnyEvent::RedisHandle->redis_up();

  my @t_data;

  my $cv = AnyEvent->condvar();
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    password => 'test',

    on_connect => sub {
      push( @t_data, 'Connected' );
    },

    on_error => sub {
      my $err = shift;
      push( @t_data, $err );
      $cv->send();
    },
  );
  $redis->ping( {
    on_done => sub {
      Test::AnyEvent::RedisHandle->break_connection();
      $redis->ping();
    },
  } );
  ev_loop( $cv );

  Test::AnyEvent::RedisHandle->fix_connection();

  is_deeply( \@t_data, [
    'Connected',
    "Can't write to socket",
    "Can't write to socket. Command 'ping' failed",
  ], 'Broken connection' );

  return;
}

####
sub t_cmd_on_error {
  my $cv;
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    password => 'test',
  );
  $redis->set( 'bar', 'Some string' );

  my $t_err;
  $cv = AnyEvent->condvar();
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
      my $err = shift;
      push( @t_errors, $err );
    },
  } );
  $redis->incr( 'bar' );
  $redis->exec( {
    on_error => sub {
      my $err = shift;
      push( @t_errors, $err );
      $cv->send();
    },
  } );
  ev_loop( $cv );

  is_deeply( \@t_errors, [
    "ERR wrong number of arguments for 'set' command",
    'ERR value is not an integer or out of range',
  ], "'on_error' callback in the method of the command" );

  return;
}

####
sub t_empty_password {
  my $t_err;
  my $cv = AnyEvent->condvar();
  my $redis = $T_CLASS->new(
    %GENERIC_PARAMS,
    password => '',
  );
  $redis->ping( {
    on_error => sub {
      $t_err = shift;
      $cv->send();
    }
  } );
  ev_loop( $cv );

  is( $t_err, 'ERR operation not permitted', 'Empty password' );

  return;
}
