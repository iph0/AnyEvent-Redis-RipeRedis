use 5.010000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 23;
use Test::AnyEvent::RedisHandle;
use AnyEvent;

my $t_class;

BEGIN {
  $t_class = 'AnyEvent::Redis::RipeRedis';

  use_ok( $t_class );
}

can_ok( $t_class, 'new' );
can_ok( $t_class, 'AUTOLOAD' );

my %GENERIC_PARAMS = (
  host => 'localhost',
  port => '6379',
  encoding => 'utf8',
);

my $cv = AnyEvent->condvar();

# Parameters pass to constructor as hash reference
new_ok( $t_class, [ \%GENERIC_PARAMS ] );

# Parameters pass to constructor as hash
my $redis = new_ok( $t_class, [
  %GENERIC_PARAMS,

  on_connect => sub {
    my $attempt = shift;

    is( $attempt, 1, 'on_connect' );
  },

  on_error => sub {
    my $msg = shift;

    diag( $msg );
  }
] );

# Authenticate
$redis->auth( 'test', {
  on_done => sub {
    my $resp = shift;

    is( $resp, 'OK', 'auth (on_done; status reply)' )
  }
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

  my $exp = [ qw(
    element_1
    element_2
    element_3
  ) ];

  is_deeply( $list, $exp, 'lrange (multi-bulk reply)' );
} );

# Get non existent list
$redis->lrange( 'non_existent', 0, -1, sub {
  my $list = shift;

  is_deeply( $list, [], 'lrange (non existent key)' );
} );

# Get
$redis->brpop( 'non_existent', '3', sub {
  my $val = shift;

  is( $val, undef, 'brpop (non existent key)' );
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

  my $exp = [
    2,
    [ qw(
      element_1
      element_2
      element_3
    ) ],
    'Some string'
  ];

  is_deeply( $data_list, $exp, 'exec (nested multi-bulk reply)' );
} );

$redis->quit( sub {
  my $resp = shift;

  is( $resp, 'OK', 'quit (status reply)' );

  $cv->send();
} );

my $timeout;

$timeout = AnyEvent->timer(
  after => 5,
  cb => sub {
    undef( $timeout );

    exit 0; # Emergency exit
  }
);

$cv->recv();
