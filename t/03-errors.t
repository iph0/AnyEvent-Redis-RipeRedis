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
  password => 'test',
  encoding => 'utf8',

  on_redis_error => sub {
    my $msg = shift;

    diag( $msg );
  }
);

my $timeout;

$timeout = AnyEvent->timer(
  after => 5,
  cb => sub {
    undef( $timeout );

    exit 0; # Emergency exit
  }
);

# TODO
# on_redis_error (t_on_redis_error)

t_can_not_connect();
t_reconnect_n_times();
t_reconnect_until_success();
t_connection_lost();
t_broken_connection();

# Subroutines

####
sub t_can_not_connect {
  my $cv;
  my $redis;
  my @data;

  Test::AnyEvent::RedisHandle->redis_down();

  $cv = AnyEvent->condvar();
  
  $redis = $t_class->new(
    %GENERIC_PARAMS,

    on_connect_error => sub {
      my $msg = shift;
      my $attempt = shift;

      push( @data, [ $msg, $attempt ] );

      $cv->send();
    },

    on_error => sub {
      my $msg = shift;

      diag( $msg );
    }
  );

  $cv->recv();

  $cv = AnyEvent->condvar();

  $redis = $t_class->new(
    %GENERIC_PARAMS,

    on_error => sub {
      my $msg = shift;

      push( @data, $msg );
      
      $cv->send();
    }
  );

  $cv->recv();

  $redis->ping( sub { 
    my $resp;

    diag( $resp );
  } );

  Test::AnyEvent::RedisHandle->redis_up();

  my $exp_msg_conn_err = "Can't connect to localhost:6379; Connection error";
  my $exp_msg_cmd_err = "Can't execute command \"ping\". Connection not established";

  my @exp_data = ( 
    [ $exp_msg_conn_err, 1 ],
    $exp_msg_conn_err,
    $exp_msg_cmd_err
  );

  is_deeply( \@data, \@exp_data, 'No connection, reconnection off' );

  return;
}

####
sub t_reconnect_n_times {
  my $cv;
  my $redis;
  my @data;

  Test::AnyEvent::RedisHandle->redis_down();

  $cv = AnyEvent->condvar();

  $redis = $t_class->new(
    %GENERIC_PARAMS,

    reconnect => 1,
    reconnect_after => 0.001,
    max_connect_attempts => 3,

    on_stop_reconnect => sub {
      push( @data, 'stopped' );

      $cv->send();
    },

    on_connect_error => sub {
      my $msg = shift;
      my $attempt = shift;

      push( @data, [ $msg, $attempt ] );
    },

    on_error => sub {
      my $msg = shift;

      diag( $msg );
    }
  );

  $cv->recv();

  $cv = AnyEvent->condvar();

  $redis = $t_class->new(
    %GENERIC_PARAMS,

    reconnect => 1,
    reconnect_after => 0.001,
    max_connect_attempts => 3,

    on_stop_reconnect => sub {
      push( @data, 'stopped' );

      $cv->send();
    },

    on_error => sub {
      my $msg = shift;

      push( @data, $msg );
    },
  );

  $cv->recv();

  Test::AnyEvent::RedisHandle->redis_up();
  
  my $exp_msg = "Can't connect to localhost:6379; Connection error";

  my @exp_data = (
    [ $exp_msg, 1 ],
    [ $exp_msg, 2 ],
    [ $exp_msg, 3 ],
    'stopped',
    $exp_msg,
    $exp_msg,
    $exp_msg,
    'stopped'
  );

  is_deeply( \@data, \@exp_data, 'No connection, reconnect <N> times' );

  return;
}

####
sub t_reconnect_until_success {
  my $cv;
  my $redis;
  my @data;

  $cv = AnyEvent->condvar();

  Test::AnyEvent::RedisHandle->redis_down();

  $redis = $t_class->new(
    %GENERIC_PARAMS,

    reconnect => 1,
    reconnect_after => 0.001,
    max_connect_attempts => 4,

    on_connect => sub {
      my $attempt = shift;

      push( @data, $attempt );

      $cv->send();
    },

    on_connect_error => sub {
      my $msg = shift;
      my $attempt = shift;

      push( @data, [ $msg, $attempt ] );

      if ( $attempt == 3 ) {
        Test::AnyEvent::RedisHandle->redis_up();
      }
    },

    on_error => sub {
      my $msg = shift;

      diag( $msg );
    }
  );

  $cv->recv();

  $cv = AnyEvent->condvar();

  Test::AnyEvent::RedisHandle->redis_down();

  my $attempt = 0;

  $redis = $t_class->new(
    %GENERIC_PARAMS,

    reconnect => 1,
    reconnect_after => 0.001,
    max_connect_attempts => 4,

    on_connect => sub {
      my $attempt = shift;

      push( @data, $attempt );

      $cv->send();
    },

    on_error => sub {
      my $msg = shift;

      push( @data, $msg );

      if ( ++$attempt == 3 ) {
        Test::AnyEvent::RedisHandle->redis_up();
      }
    }
  );

  $cv->recv();

  my $exp_msg = "Can't connect to localhost:6379; Connection error";

  my @exp_data = (
    [ $exp_msg, 1 ],
    [ $exp_msg, 2 ],
    [ $exp_msg, 3 ],
    4,
    $exp_msg,
    $exp_msg,
    $exp_msg,
    '4'
  );

  is_deeply( \@data, \@exp_data, 'No connection, reconnect until success' );

  return;
}

####
sub t_connection_lost {
  my @data;
  my $send_sw;

  Test::AnyEvent::RedisHandle->redis_up();

  my $cv = AnyEvent->condvar();

  my $redis = $t_class->new(
    %GENERIC_PARAMS,

    reconnect => 1,
    reconnect_after => 0.001,
    max_connect_attempts => 4,

    on_connect => sub {
      my $attempt = shift;

      push( @data, $attempt );

      if ( $send_sw ) {
        $cv->send();
      }
    },

    on_connect_error => sub {
      my $msg = shift;
      my $attempt = shift;

      push( @data, [ $msg, $attempt ] );

      if ( $attempt == 3 ) {
        $send_sw = 1;
        
        Test::AnyEvent::RedisHandle->redis_up();
      }
    },

    on_error => sub {
      my $msg = shift;

      push( @data, $msg );
    }
  );

  $redis->ping( sub {
    Test::AnyEvent::RedisHandle->redis_down();
  } );

  $cv->recv();

  my $exp_msg = "Can't connect to localhost:6379; Connection error";

  my @exp_data = (
    1,
    'Connection lost',
    [ $exp_msg, 1 ],
    [ $exp_msg, 2 ],
    [ $exp_msg, 3 ],
    4
  );

  is_deeply( \@data, \@exp_data, 'Connection lost, reconnect until success' );

  return;
}

####
sub t_broken_connection {
  my @data;
  my $send_sw;

  my $cv = AnyEvent->condvar();

  Test::AnyEvent::RedisHandle->redis_up();

  my $redis = $t_class->new(
    %GENERIC_PARAMS,

    reconnect => 1,
    reconnect_after => 0.001,
    max_connect_attempts => 4,

    on_connect => sub {
      my $attempt = shift;

      push( @data, $attempt );

      if ( $send_sw ) {
        $cv->send();
      }
    },

    on_connect_error => sub {
      my $msg = shift;
      my $attempt = shift;

      push( @data, [ $msg, $attempt ] );

      if ( $attempt == 3 ) {
        $send_sw = 1;
        
        Test::AnyEvent::RedisHandle->redis_up();
      }
    },

    on_error => sub {
      my $msg = shift;

      push( @data, $msg );
    }
  );

  $redis->ping( sub {
    Test::AnyEvent::RedisHandle->break_connection();
  } );

  $redis->incr( 'foo', sub { 
    my $val;

    diag( $val );
  } );

  $cv->recv();

  my $exp_msg = "Can't connect to localhost:6379; Connection error";

  my @exp_data = (
    1,
    'Error writing to socket',
    [ $exp_msg, 1 ],
    [ $exp_msg, 2 ],
    [ $exp_msg, 3 ],
    4
  );

  is_deeply( \@data, \@exp_data, 'Broken connection, reconnect until success' );

  return;
}
