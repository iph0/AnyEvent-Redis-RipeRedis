use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 10;
use Test::AnyEvent::RedisHandle;
use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $t_class = 'AnyEvent::Redis::RipeRedis';

my %GENERIC_PARAMS = (
  host => 'localhost',
  port => '6379',
);
my $PASSWORD = 'test';

my $timeout;
$timeout = AnyEvent->timer(
  after => 5,
  cb => sub {
    undef( $timeout );
    exit 0; # Emergency exit
  },
);

can_not_connect_t();
reconnect_n_times_t();
reconnect_until_success_t();
connection_lost_t();
connection_lost_reconnect_t();
broken_connection_t();
cmd_on_error_t();

# Subroutines

####
sub can_not_connect_t {
  my @data;

  Test::AnyEvent::RedisHandle->redis_down();

  my $cv = AnyEvent->condvar();

  my $redis = $t_class->new(
    %GENERIC_PARAMS,

    on_connect_error => sub {
      my $err = shift;
      my $attempt = shift;
      push( @data, [ $err, $attempt ] );
    },

    on_error => sub {
      my $err = shift;
      push( @data, $err );

      $cv->send();
    },
  );

  $redis->auth( $PASSWORD );

  $cv->recv();

  Test::AnyEvent::RedisHandle->redis_up();

  my @exp_data = (
    [ "Can't connect to localhost:6379; Connection error", 1 ],
    "Command 'auth' failed",
  );
  is_deeply( \@data, \@exp_data, "Can't connect" );

  return;
}

####
sub reconnect_n_times_t {
  my @data;

  Test::AnyEvent::RedisHandle->redis_down();

  my $cv = AnyEvent->condvar();

  my $redis = $t_class->new(
    %GENERIC_PARAMS,
    reconnect => 1,
    reconnect_after => 0.001,
    max_connect_attempts => 2,

    on_stop_reconnect => sub {
      push( @data, 'stopped' );
      $cv->send();
    },

    on_error => sub {
      my $err = shift;
      push( @data, $err );
    },
  );

  $redis->auth( $PASSWORD );

  $cv->recv();

  Test::AnyEvent::RedisHandle->redis_up();

  my @exp_data = (
    "Can't connect to localhost:6379; Connection error",
    "Command 'auth' failed",
    "Can't connect to localhost:6379; Connection error",
    'stopped',
  );
  is_deeply( \@data, \@exp_data, 'Reconnect N times' );

  return;
}

####
sub reconnect_until_success_t {
  my @data;

  Test::AnyEvent::RedisHandle->redis_down();

  my $cv = AnyEvent->condvar();

  my $redis = $t_class->new(
    %GENERIC_PARAMS,
    reconnect => 1,
    reconnect_after => 0.001,

    on_connect => sub {
      my $attempt = shift;
      push( @data, $attempt );

      $cv->send();
    },

    on_connect_error => sub {
      my $err = shift;
      my $attempt = shift;
      push( @data, [ $err, $attempt ] );

      if ( $attempt == 2 ) {
        Test::AnyEvent::RedisHandle->redis_up();
      }
    },

    on_error => sub {
      my $err = shift;
      push( @data, $err );
    },
  );

  $redis->auth( $PASSWORD );

  $cv->recv();

  Test::AnyEvent::RedisHandle->redis_down();

  my @exp_data = (
    [ "Can't connect to localhost:6379; Connection error", 1 ],
    "Command 'auth' failed",
    [ "Can't connect to localhost:6379; Connection error", 2 ],
    3,
  );
  is_deeply( \@data, \@exp_data, 'Reconnect until success' );

  return;
}

####
sub connection_lost_t {
  my @data;

  Test::AnyEvent::RedisHandle->redis_up();

  my $cv = AnyEvent->condvar();

  my $redis = $t_class->new(
    %GENERIC_PARAMS,

    on_connect => sub {
      my $attempt = shift;
      push( @data, $attempt );
    },

    on_error => sub {
      my $msg = shift;
      push( @data, $msg );

      $cv->send();
    },
  );

  $redis->auth( $PASSWORD, {
    on_done => sub {
      Test::AnyEvent::RedisHandle->redis_down();
    },
  } );

  $cv->recv();

  my @exp_data = (
    1,
    'Connection lost',
  );
  is_deeply( \@data, \@exp_data, 'Connection lost' );

  $redis->ping( {
    on_error => sub {
      my $err = shift;
      my $exp_err = "Can't execute command 'ping'. Connection not established";
      is( $err, $exp_err, 'Execute command when connection not established' );
    }
  } );

  return;
}

####
sub connection_lost_reconnect_t {
  my @data;

  Test::AnyEvent::RedisHandle->redis_up();

  my $cv = AnyEvent->condvar();

  my $sw;
  my $redis = $t_class->new(
    %GENERIC_PARAMS,
    reconnect => 1,

    on_connect => sub {
      my $attempt = shift;
      push( @data, $attempt );
      
      if ( $sw ) {
        $cv->send();
      }
    },

    on_error => sub {
      my $msg = shift;
      push( @data, $msg );
      $sw = 1;
      
      Test::AnyEvent::RedisHandle->redis_up();
    },
  );

  $redis->auth( $PASSWORD, {
    on_done => sub {
      Test::AnyEvent::RedisHandle->redis_down();
    },
  } );

  $cv->recv();

  my @exp_data = (
    1,
    'Connection lost',
    1,
  );
  is_deeply( \@data, \@exp_data, 'Connection lost (reconnect)' );

  return;
}

####
sub broken_connection_t {
  my @data;

  Test::AnyEvent::RedisHandle->redis_up();

  my $cv = AnyEvent->condvar();

  my $redis = $t_class->new(
    %GENERIC_PARAMS,

    on_connect => sub {
      my $attempt = shift;
      push( @data, $attempt );
    },

    on_error => sub {
      my $msg = shift;
      push( @data, $msg );

      $cv->send();
    },
  );

  $redis->auth( $PASSWORD, {
    on_done => sub {
      Test::AnyEvent::RedisHandle->break_connection();
    },
  } );
  $redis->ping();

  $cv->recv();

  Test::AnyEvent::RedisHandle->fix_connection();

  my @exp_data = (
    1,
    'Error writing to socket',
    "Command 'ping' failed",
  );
  is_deeply( \@data, \@exp_data, 'Broken connection' );

  return;
}

####
sub cmd_on_error_t {
  my $cv = AnyEvent->condvar();

  my $redis = $t_class->new( %GENERIC_PARAMS );

  $redis->auth( $PASSWORD );
  $redis->set( 'bar', 'Some string' );

  local $SIG{__WARN__} = sub {
    my $err = shift;
    chomp( $err );
    my $exp_err = 'ERR value is not an integer or out of range';
    is( $err, $exp_err, 'Default on_error callback' );
  };
  $redis->incr( 'bar' );

  $redis->multi();
  $redis->set( {
    on_error => sub {
      my $err = shift;
      my $exp_err = "ERR wrong number of arguments for 'set' command";
      is( $err, $exp_err, 'Local on_error callback (validate time)' );
    },
  } );
  $redis->incr( 'bar' );
  $redis->exec( {
    on_error => sub {
      my $err = shift;
      my $exp_err = 'ERR value is not an integer or out of range';
      is( $err, $exp_err, 'Local on_error callback (execute time)' );

      $cv->send();
    },
  } );

  $cv->recv();

  return;
}
