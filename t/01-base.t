use 5.006000;
use strict;
use warnings;
use utf8;

use lib 't/tlib';
use Test::More tests => 16;
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

my $timer;
$timer = AnyEvent->timer(
  after => 5,
  cb => sub {
    undef( $timer );
    diag( 'Emergency exit from event loop. Test failed' );
    exit 0; # Emergency exit
  },
);

my $cv;

$cv = AnyEvent->condvar();
my $redis = new_ok( $t_class, [
  host => 'localhost',
  port => '6379',
  password => 'test',
  connection_timeout => 5,
  encoding => 'utf8',

  on_connect => sub {
    ok( 1, 'Connected', );
    $cv->send();
  },
] );
$cv->recv();

# Ping (status reply)
$cv = AnyEvent->condvar();
$redis->ping( {
  on_done => sub {
    my $t_resp = shift;
    is( $t_resp, 'PONG', 'ping (status reply)' );
    $cv->send();
  },
} );
$cv->recv();

# Increment (numeric reply)
$cv = AnyEvent->condvar();
$redis->incr( 'foo', {
  on_done => sub {
    my $t_val = shift;
    is( $t_val, 1, 'incr (numeric reply)' );
    $cv->send();
  },
} );
$cv->recv();

# Set value
$redis->set( 'bar', 'Some string' );

# Get value (bulk reply)
$cv = AnyEvent->condvar();
$redis->get( 'bar', {
  on_done => sub {
    my $t_str = shift;
    is( $t_str, 'Some string', 'get (bulk reply)' );
    $cv->send();
  },
} );
$cv->recv();

# Set/Get UTF-8 string
$cv = AnyEvent->condvar();
$redis->set( 'ключ', 'Значение' );
$redis->get( 'ключ', {
  on_done => sub {
    my $t_str = shift;
    is( $t_str, 'Значение', 'set/get UTF-8 string' );
    $cv->send();
  },
} );
$cv->recv();

# Get non existent key
$cv = AnyEvent->condvar();
$redis->get( 'non_existent', {
  on_done => sub {
    my $t_str = 'not_undef';
    $t_str = shift;
    is( $t_str, undef, 'get (non existent key)' );
    $cv->send();
  },
} );
$cv->recv();

# Get list of values
$cv = AnyEvent->condvar();
for ( my $i = 2; $i <= 3; $i++ ) {
  $redis->rpush( 'list', "element_$i" );
}
$redis->lpush( 'list', 'element_1' );
$redis->lrange( 'list', 0, -1, {
  on_done => sub {
    my $t_data = shift;
    my $t_exp_data = [ qw( element_1 element_2 element_3 ) ];
    is_deeply( $t_data, $t_exp_data, 'lrange (multi-bulk reply)' );
    $cv->send();
  },
} );
$cv->recv();

# Fetch from empty list
$cv = AnyEvent->condvar();
$redis->lrange( 'non_existent', 0, -1, {
  on_done => sub {
    my $t_data = shift;
    is_deeply( $t_data, [], 'lrange (empty list)' );
    $cv->send();
  },
} );
$cv->recv();

# Get from empty list
$cv = AnyEvent->condvar();
$redis->brpop( 'non_existent', '5', {
  on_done => sub {
    my $t_data = 'not_undef';
    $t_data = shift;
    is( $t_data, undef, 'brpop (empty list)' );
    $cv->send();
  },
} );
$cv->recv();

# Transaction
$cv = AnyEvent->condvar();
$redis->multi();
$redis->incr( 'foo' );
$redis->lrange( 'list', 0, -1 );
$redis->lrange( 'non_existent', 0, -1 );
$redis->get( 'bar' );
$redis->lrange( 'list', 0, -1 );
$redis->exec( {
  on_done => sub {
    my $t_data = shift;
    my $t_exp_data = [
      2,
      [ qw( element_1 element_2 element_3 ) ],
      [],
      'Some string',
      [ qw( element_1 element_2 element_3 ) ],
    ];
    is_deeply( $t_data, $t_exp_data, 'exec (nested multi-bulk reply)' );
    $cv->send();
  },
} );
$cv->recv();

# Quit
$cv = AnyEvent->condvar();
$redis->quit( {
  on_done => sub {
    my $t_resp = shift;
    is( $t_resp, 'OK', 'quit (status reply)' );
    $cv->send();
  },
} );
$cv->recv();
