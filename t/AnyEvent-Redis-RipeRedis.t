# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl AnyEvent-Redis-RipeRedis.t'

use 5.010000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 22;
use Test::AnyEvent::RedisHandle;
use AnyEvent;

my $t_class;

BEGIN {
  $t_class = 'AnyEvent::Redis::RipeRedis';

  use_ok( $t_class );
}

can_ok( $t_class, 'new' );
can_ok( $t_class, 'AUTOLOAD' );
can_ok( $t_class, 'DESTROY' );

my $cv = AnyEvent->condvar();

my $timeout;

$timeout = AnyEvent->timer(
  after => 3,
  cb => sub {
    undef( $timeout );

    $cv->send();
  }
);

my $redis;

$redis = $t_class->new( {
  host => 'localhost',
  port => '6379',
  password => 'test',
  encoding => 'utf8',
  reconnect => 1,
  reconnect_after => 1,
  max_connect_attempts => 10,

  on_connect => sub {
    my $attempt = shift;

    is( $attempt, 1, 'on_connect' );
  },

  on_stop_reconnect => sub {
    # TODO
  },

  on_redis_error => sub {
    my $msg = shift;

    ok( $msg =~ m/^ERR/o, $msg );
  },

  on_error => sub {
    my $msg = shift;

    # TODO
  }
} );

isa_ok( $redis, $t_class );

# Ping
$redis->ping( sub {
  my $resp = shift;

  is( $resp, 'PONG', 'ping (status reply)' );
} );

# Increment
$redis->incr( 'foo', sub {
  my $val = shift;

  is( $val, 1, 'incr (numeric reply)' );
} );

# Set value
$redis->set( 'bar', 'Some string', sub {
  my $resp = shift;

  is( $resp, 'OK', 'set (status reply)' );
} );

# Get value
$redis->get( 'bar', sub {
  my $val = shift;

  is( $val, 'Some string', 'get (bulk reply)' );
} );

# Get non existent key
$redis->get( 'non_existent', sub {
  my $val = shift;

  is( $val, undef, 'get (non existent key)' );
} );


# Push values

for ( my $i = 2; $i <= 3; $i++ ) {
  $redis->rpush( 'list', "element_$i", sub {
    my $resp = shift;

    is( $resp, 'OK', 'rpush (status reply)' );
  } );
}

$redis->lpush( 'list', "element_1", sub {
  my $resp = shift;

  is( $resp, 'OK', 'rpush (status reply)' );
} );


# Get list of values
$redis->lrange( 'list', 0, -1, sub {
  my $list = shift;

  my $expected = [ qw(
    element_1
    element_2
    element_3
  ) ];

  is_deeply( $list, $expected, 'lrange (multi-bulk reply)' );
} );

# Get non existent list
$redis->lrange( 'non_existent', 0, -1, sub {
  my $list = shift;

  is_deeply( $list, [], 'lrange (non existent key)' );
} );


# Transaction

$redis->multi( sub {
  my $resp = shift;

  is( $resp, 'OK', 'multi (status reply)' );
} );

$redis->incr( 'foo', sub {
  my $val = shift;

  is( $val, 'QUEUED', 'incr (queued)' );
} );

$redis->lrange( 'list', 0, -1, sub {
  my $resp = shift;

  is( $resp, 'QUEUED', 'lrange (queued)' );
} );

$redis->get( 'bar', sub {
  my $val = shift;

  is( $val, 'QUEUED', 'get (queued)' );
} );

$redis->exec( sub {
  my $data_list = shift;

  my $expected = [
    2,
    [ qw(
      element_1
      element_2
      element_3
    ) ],
    'Some string'
  ];

  is_deeply( $data_list, $expected, 'exec (nested multi-bulk reply)' );

  $redis->quit( sub {
    my $resp = shift;

    is( $resp, 'OK', 'quit (status reply)' );

    $cv->send();
  } );
} );

$cv->recv();
