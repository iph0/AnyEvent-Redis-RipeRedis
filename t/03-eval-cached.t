use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 1;
use Test::AnyEvent::RedisHandle;
use Test::AnyEvent::EVLoop;
use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $T_CLASS = 'AnyEvent::Redis::RipeRedis';

my $redis = $T_CLASS->new(
  password => 'test',
  lazy => 1,
);

my $cv = AnyEvent->condvar();
# Really Lua script not executed
my $script = <<LUA
return redis.status_reply( 'OK' )
LUA
;
my @t_data;
$redis->eval_cached( $script, 0, {
  on_done => sub {
    my $data = shift;
    push( @t_data, $data );

    $redis->eval_cached( $script, 0, {
      on_done => sub {
        my $data = shift;
        push( @t_data, $data );
        $cv->send();
      }
    } );
  },
} );
ev_loop( $cv );

is_deeply( \@t_data, [ qw( OK OK ) ], 'eval_cached' );
