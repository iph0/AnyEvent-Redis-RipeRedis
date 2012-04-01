use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 6;
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
    diag( 'Emergency exit from event loop. Test failed' );
    exit 0; # Emergency exit
  },
);

t_no_connection();
t_reconnect();
t_broken_connection();
t_cmd_on_error();
t_empty_password();

# Subroutines

####
sub t_no_connection {
  my @t_data;

  Test::AnyEvent::RedisHandle->redis_down();

  my $cv = AnyEvent->condvar();

  my $cnt = 0;
  my $redis = $t_class->new(
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

  $cv->recv();

  $redis->ping( {
    on_error => sub {
      my $err = shift;
      push( @t_data, $err );
    }
  } );

  Test::AnyEvent::RedisHandle->redis_up();

  my $t_exp_data = [
    "Can't connect to localhost:6379; Connection error",
    "Command 'ping' failed",
    "Can't execute command 'ping'. Connection not established"
  ];
  is_deeply( \@t_data, $t_exp_data, "Can't connect" );

  return;
}

####
sub t_reconnect {
  my @t_data;

  Test::AnyEvent::RedisHandle->redis_up();

  my $cv = AnyEvent->condvar();

  my $redis = $t_class->new(
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

  $cv->recv();

  $cv = AnyEvent->condvar();

  $redis->ping( {
    on_done => sub {
      my $resp = shift;
      push( @t_data, $resp );
      $cv->send();
    }
  } );

  $cv->recv();

  my $t_exp_data = [
    'Connected',
    'Disconnected',
    'Connected',
    'PONG',
  ];
  is_deeply( \@t_data, $t_exp_data, 'Reconnect' );

  return;
}

####
sub t_broken_connection {
  my @t_data;

  Test::AnyEvent::RedisHandle->redis_up();

  my $cv = AnyEvent->condvar();

  my $redis = $t_class->new(
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

  $cv->recv();

  Test::AnyEvent::RedisHandle->fix_connection();

  my $t_exp_data = [
    'Connected',
    'Error writing to socket',
    "Command 'ping' failed",
  ];
  is_deeply( \@t_data, $t_exp_data, 'Broken connection' );

  return;
}

####
sub t_cmd_on_error {
  my $cv;

  my $redis = $t_class->new(
    %GENERIC_PARAMS,
    password => 'test',
  );

  $redis->set( 'bar', 'Some string' );

  local $SIG{__WARN__} = sub {
    my $t_err = shift;
    chomp( $t_err );
    is( $t_err, 'ERR value is not an integer or out of range',
        "Default 'on_error' callback" );
    $cv->send();
  };
  $cv = AnyEvent->condvar();
  $redis->incr( 'bar' );
  $cv->recv();

  $cv = AnyEvent->condvar();
  my @t_errors;
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
  $cv->recv();
  my $t_exp_errors = [
    "ERR wrong number of arguments for 'set' command",
    'ERR value is not an integer or out of range',
  ];
  is_deeply( \@t_errors, $t_exp_errors, "Local 'on_error' callback" );

  return;
}

####
sub t_empty_password {
  my $cv = AnyEvent->condvar();
  
  my $redis = $t_class->new(
    %GENERIC_PARAMS,
    password => '',
  );

  $redis->ping( {
    on_error => sub {
      my $t_err = shift;
      is( $t_err, 'ERR operation not permitted', 'Empty password' );
      $cv->send();
    }
  } );
  $cv->recv();

  return;
}
