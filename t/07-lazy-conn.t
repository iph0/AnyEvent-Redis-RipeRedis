use 5.008000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis;
require 't/test_helper.pl';

my $server_info = run_redis_instance();
if ( !defined( $server_info ) ) {
  plan skip_all => 'redis-server is required to this test';
}
plan tests => 2;

my $redis;
my $t_is_conn = 0;

ev_loop(
  sub {
    my $cv = shift;

    $redis = AnyEvent::Redis::RipeRedis->new(
      host => $server_info->{host},
      port => $server_info->{port},
      lazy => 1,
      reconnect => 0,
      on_connect => sub {
        $t_is_conn = 1;
      },
    );

    my $timer;
    $timer = AnyEvent->timer(
      after => 1,
      cb => sub {
        undef( $timer );

        ok( !$t_is_conn, 'lazy connection (no connected yet)' );

        $redis->ping( {
          on_done => sub {
            $cv->send();
          },
        } );
      },
    );
  }
);

$redis->disconnect();

ok( $t_is_conn, 'lazy connection (connected)' );
