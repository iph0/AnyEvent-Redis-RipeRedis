use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 2;
use Test::AnyEvent::RedisHandle;
use Test::AnyEvent::EVLoop;
use AnyEvent::Redis::RipeRedis;
use Scalar::Util qw( weaken );

my $T_CLASS = 'AnyEvent::Redis::RipeRedis';

my $redis;

my $t_connected = 0;

ev_loop(
  sub {
    my $cv = shift;

    $redis = $T_CLASS->new(
      password => 'test',
      lazy => 1,
      reconnect => 0,

      on_connect => sub {
        $t_connected = 1;
      },
    );

    my $timer;
    $timer = AnyEvent->timer(
      after => 0.1,
      cb => sub {
        undef( $timer );
        $cv->send();
      },
    );
  }
);

ok( !$t_connected, 'Lazy connection (yet no connected)' );

ev_loop(
  sub {
    my $cv = shift;

    $redis->ping( {
      on_done => sub {
        $cv->send();
      },
    } );
  }
);

ok( $t_connected, 'Lazy connection (connected)' );

$redis->disconnect();
