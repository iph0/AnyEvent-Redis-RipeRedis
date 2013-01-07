use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 5;
use Test::AnyEvent::RedisHandle;
use Test::AnyEvent::EVLoop;
use Scalar::Util qw( weaken );

my $T_CLASS;

BEGIN {
  $T_CLASS = 'AnyEvent::Redis::RipeRedis';
  use_ok( $T_CLASS );
}

can_ok( $T_CLASS, 'new' );
can_ok( $T_CLASS, 'eval_cached' );

my $t_redis = new_ok( $T_CLASS, [
  password => 'test',
] );

my @t_data;

ev_loop(
  sub {
    my $cv = shift;

    my $script = <<LUA
return redis.status_reply( 'OK' )
LUA
;
    my $t_redis = $t_redis;
    weaken( $t_redis );
    $t_redis->eval_cached( $script, 0, {
      on_done => sub {
        my $data = shift;
        push( @t_data, $data );

        $t_redis->eval_cached( $script, 0, {
          on_done => sub {
            my $data = shift;
            push( @t_data, $data );
            $cv->send();
          }
        } );
      },
    } );
  }
);

is_deeply( \@t_data, [ qw( OK OK ) ], 'Script execution' );

$t_redis->disconnect();
