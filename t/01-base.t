use 5.006000;
use strict;
use warnings;
use utf8;

use lib 't/tlib';
use Test::More tests => 19;
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

my $redis = new_ok( $t_class, [
  host => 'localhost',
  port => '6379',
  connection_timeout => 5,
  password => 'test',
  encoding => 'utf8',

  on_connect => sub {
    ok( 1, 'on_connect' );
  },
] );

$redis->ping( {
  on_done => sub {
    my $resp = shift;
    is( $resp, 'PONG', 'ping (status reply)' )
  },
} );

# Increment
$redis->incr( 'foo', {
  on_done => sub {
    my $val = shift;
    is( $val, 1, 'incr (numeric reply)' );
  },
} );

# Set value
$redis->set( 'bar', 'Some string' );

# Get value
$redis->get( 'bar', {
  on_done => sub {
    my $val = shift;
    is( $val, 'Some string', 'get (bulk reply)' );
  },
} );

# Set/Get UTF-8 string
$redis->set( 'ключ', 'Значение' );
$redis->get( 'ключ', {
  on_done => sub {
    my $val = shift;
    is( $val, 'Значение', 'set/get UTF-8 string' );
  },
} );

# Get non existent key
$redis->get( 'non_existent', {
  on_done => sub {
    my $val = shift;
    is( $val, undef, 'get (non existent key)' );
  },
} );


# Push values

for ( my $i = 2; $i <= 3; $i++ ) {
  $redis->rpush( 'list', "element_$i", {
    on_done => sub {
      my $resp = shift;
      is( $resp, 'OK', 'rpush (status reply)' );
    },
  } );
}

$redis->lpush( 'list', "element_1", {
  on_done => sub {
    my $resp = shift;
    is( $resp, 'OK', 'rpush (status reply)' );
  },
} );

# Get list of values
$redis->lrange( 'list', 0, -1, {
  on_done => sub {
    my $list = shift;

    my @exp_list = qw( element_1 element_2 element_3 );
    is_deeply( $list, \@exp_list, 'lrange (multi-bulk reply)' );
  },
} );

# Fetch from empty list
$redis->lrange( 'non_existent', 0, -1, {
  on_done => sub {
    my $list = shift;
    is_deeply( $list, [], 'lrange (empty list)' );
  },
} );

# Get from empty list
$redis->brpop( 'non_existent', '5', {
  on_done => sub {
    my $val = shift;
    is( $val, undef, 'brpop (empty list)' );
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
    my $data = shift;

    my $exp_data = [
      2,
      [ qw( element_1 element_2 element_3 ) ],
      [],
      'Some string',
      [ qw( element_1 element_2 element_3 ) ],
    ];
    is_deeply( $data, $exp_data, 'exec (nested multi-bulk reply)' );
  },
} );

# Quit
$redis->quit( {
  on_done => sub {
    my $resp = shift;
    is( $resp, 'OK', 'quit (status reply)' );

    $cv->send();
  },
} );

my $timer;
$timer = AnyEvent->timer(
  after => 5,
  cb => sub {
    undef( $timer );
    exit 0; # Emergency exit
  },
);

$cv->recv();
