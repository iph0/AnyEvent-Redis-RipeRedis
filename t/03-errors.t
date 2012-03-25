use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 7;
use Test::AnyEvent::RedisHandle;
use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $t_class = 'AnyEvent::Redis::RipeRedis';

my %GENERIC_PARAMS = (
  host => 'localhost',
  port => '6379',
);

my $timer;
$timer = AnyEvent->timer(
  after => 5,
  cb => sub {
    undef( $timer );
    exit 0; # Emergency exit
  },
);

no_connection_t();
reconnect_t();
broken_connection_t();
cmd_on_error_t();
empty_password_t();

# Subroutines

####
sub no_connection_t {
  my @data;

  Test::AnyEvent::RedisHandle->redis_down();

  my $cv = AnyEvent->condvar();

  my $cnt = 0;
  my $redis = $t_class->new(
    %GENERIC_PARAMS,

    on_error => sub {
      my $err = shift;
      push( @data, $err );
    },
  );

  $redis->ping( {
    on_error => sub {
      my $err = shift;
      push( @data, $err );
      $cv->send();
    }
  } );

  $cv->recv();

  Test::AnyEvent::RedisHandle->redis_up();

  my @exp_data = (
    "Can't connect to localhost:6379; Connection error",
    "Command 'ping' failed",
  );
  is_deeply( \@data, \@exp_data, "Can't connect" );

  return;
}

####
sub reconnect_t {
  my @data;

  Test::AnyEvent::RedisHandle->redis_up();

  my $cv = AnyEvent->condvar();

  my $redis = $t_class->new(
    %GENERIC_PARAMS,
    password => 'test',

    on_connect => sub {
      push( @data, 'Connected' );
    },

    on_disconnect => sub {
      push( @data, 'Disconnected' );
      Test::AnyEvent::RedisHandle->redis_up();

      $cv->send();
    },
  );

  $redis->ping( {
    on_done => sub {
      Test::AnyEvent::RedisHandle->redis_down();
    }
  } );

  $cv->recv();

  $cv = AnyEvent->condvar();

  $redis->ping( {
    on_done => sub {
      my $resp = shift;
      push( @data, $resp );

      $cv->send();
    }
  } );

  $cv->recv();

  my @exp_data = (
    'Connected',
    'Disconnected',
    'Connected',
    'PONG',
  );
  is_deeply( \@data, \@exp_data, 'Reconnect' );

  return;
}

####
sub broken_connection_t {
  my @data;

  Test::AnyEvent::RedisHandle->redis_up();

  my $cv = AnyEvent->condvar();

  my $redis = $t_class->new(
    %GENERIC_PARAMS,
    password => 'test',

    on_connect => sub {
      push( @data, 'Connected' );
    },

    on_error => sub {
      my $err = shift;
      push( @data, $err );

      $cv->send();
    },
  );

  $redis->ping( {
    on_done => sub {
      Test::AnyEvent::RedisHandle->break_connection();
      $redis->ping();
    },
  } );

  $cv->recv();

  Test::AnyEvent::RedisHandle->fix_connection();

  my @exp_data = (
    'Connected',
    'Error writing to socket',
    "Command 'ping' failed",
  );
  is_deeply( \@data, \@exp_data, 'Broken connection' );

  return;
}

####
sub cmd_on_error_t {
  my $cv = AnyEvent->condvar();

  my $redis = $t_class->new(
    %GENERIC_PARAMS,
    password => 'test',
  );

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

####
sub empty_password_t {
  my $cv = AnyEvent->condvar();

  my $redis = $t_class->new(
    %GENERIC_PARAMS,
    password => '',
  );
  
  $redis->ping( { 
    on_error => sub {
      my $err = shift;
      my $exp_err = 'ERR operation not permitted';
      is( $err, $exp_err, 'Empty password' );

      $cv->send();
    }
  } );

  $cv->recv();

  return; 
}
