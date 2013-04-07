use 5.006000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis;
use Scalar::Util qw( weaken );
require 't/test_helper.pl';

my $server_info = run_redis_instance();

plan tests => 2;

my $t_is_conn = 0;

ev_loop(
  sub {
    my $cv = shift;

    my $redis = AnyEvent::Redis::RipeRedis->new(
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

        ok( !$t_is_conn, 'Lazy connection (yet no connected)' );

        $redis->ping( {
          on_done => sub {
            $cv->send();
          },
        } );
      },
    );

    $redis->disconnect();
  }
);

ok( $t_is_conn, 'Lazy connection (connected)' );

