use 5.010000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 5;
use Test::AnyEvent::RedisHandle;
use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $t_class = 'AnyEvent::Redis::RipeRedis';

my %GENERIC_PARAMS = (
  host => 'localhost',
  port => '6379',
  encoding => 'utf8',
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
# TODO connection_lost_reconnect_t();
broken_connection_t();
# TODO on_error_t();
# TODO executong command on closed connection

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
    "Command \"auth\" failed",
  );
  is_deeply( \@data, \@exp_data, 'No connection, reconnection off' );

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
    "Command \"auth\" failed",
    "Can't connect to localhost:6379; Connection error",
    'stopped',
  );
  is_deeply( \@data, \@exp_data, 'No connection, reconnect <N> times' );

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
    "Command \"auth\" failed",
    [ "Can't connect to localhost:6379; Connection error", 2 ],
    3,
  );
  is_deeply( \@data, \@exp_data, 'No connection, reconnect until success' );

  return;
}

####
sub connection_lost_t {
  my @data;
  my $send_sw;

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
    'Command "ping" failed',
  );
  is_deeply( \@data, \@exp_data, 'Broken connection' );

  return;
}

####
#sub on_error_t {
#  my $cv = AnyEvent->condvar();
#
#  my $redis = $t_class->new(
#    %GENERIC_PARAMS,
#
#    on_error => sub {
#      my $msg = shift;
#      diag( $msg );
#    },
#  );
#
#  # Authenticate
#  $redis->auth( 'invalid_password', {
#    on_error => sub {
#      my $resp = shift;
#      is( $resp, 'ERR invalid password', 'on_error (parameter of the method)' );
#
#      $cv->send();
#    },
#  } );
#
#  $cv->recv();
#
#  return;
#}
