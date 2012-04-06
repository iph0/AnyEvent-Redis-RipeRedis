use 5.006000;
use strict;
use warnings;
use utf8;

# TODO make AE wrapper function for testing

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

my $cv = AnyEvent->condvar();

my $redis = new_ok( $t_class, [
  host => 'localhost',
  port => '6379',
  password => 'test',
  connection_timeout => 5,
  encoding => 'utf8',

  on_connect => sub {
    ok( 1, 'Connected', );
  },
] );

# Ping (status reply)
$redis->ping( {
  on_done => sub {
    my $t_resp = shift;
    is( $t_resp, 'PONG', 'ping (status reply)' );
  },
} );

# Increment (numeric reply)
$redis->incr( 'foo', {
  on_done => sub {
    my $t_val = shift;
    is( $t_val, 1, 'incr (numeric reply)' );
  },
} );

# Set value
$redis->set( 'bar', 'Some string' );

# Get value (bulk reply)
$redis->get( 'bar', {
  on_done => sub {
    my $t_str = shift;
    is( $t_str, 'Some string', 'get (bulk reply)' );
  },
} );

# Set/Get UTF-8 string
$redis->set( 'ключ', 'Значение' );
$redis->get( 'ключ', {
  on_done => sub {
    my $t_str = shift;
    is( $t_str, 'Значение', 'set/get UTF-8 string' );
  },
} );

# Get non existent key
$redis->get( 'non_existent', {
  on_done => sub {
    my $t_str = 'not_undef';
    $t_str = shift;
    is( $t_str, undef, 'get (non existent key)' );
  },
} );

# Get list of values
for ( my $i = 2; $i <= 3; $i++ ) {
  $redis->rpush( 'list', "element_$i" );
}
$redis->lpush( 'list', 'element_1' );
$redis->lrange( 'list', 0, -1, {
  on_done => sub {
    my $t_data = shift;
    my $t_exp_data = [ qw( element_1 element_2 element_3 ) ];
    is_deeply( $t_data, $t_exp_data, 'lrange (multi-bulk reply)' );
  },
} );

# Fetch from empty list
$redis->lrange( 'non_existent', 0, -1, {
  on_done => sub {
    my $t_data = shift;
    is_deeply( $t_data, [], 'lrange (empty list)' );
  },
} );

# Get from empty list
$redis->brpop( 'non_existent', '5', {
  on_done => sub {
    my $t_data = 'not_undef';
    $t_data = shift;
    is( $t_data, undef, 'brpop (empty list)' );
  },
} );

# Transaction
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
  },
} );

# Quit
$redis->quit( {
  on_done => sub {
    my $t_resp = shift;
    is( $t_resp, 'OK', 'quit (status reply)' );
    $cv->send();
  },
} );

$cv->recv();
