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

my $t_redis;

my $t_connected = 0;

ev_loop(
  sub {
    my $cv = shift;

    $t_redis = new_ok( $T_CLASS, [
      password => 'test',
      lazy => 1,
      reconnect => 0,

      on_connect => sub {
        $t_connected = 1;
      },
    ] );

    my $timer;
    $timer = AnyEvent->timer(
      after => 0.001,
      cb => sub {
        undef( $timer );

        ok( !$t_connected, 'Lazy connection (yet no connected)' );

        $t_redis->ping( {
          on_done => sub {
            $cv->send();
          },
        } );
      },
    );
  }
);

ok( $t_connected, 'Lazy connection (connected)' );

$t_redis->disconnect();
