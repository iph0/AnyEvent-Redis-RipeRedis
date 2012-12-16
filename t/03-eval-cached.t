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

can_ok( $T_CLASS, 'eval_cached' );

my $redis = $T_CLASS->new(
  password => 'test',
);

my @t_data;

ev_loop(
  sub {
    my $cv = shift;

    my $script = <<LUA
return redis.status_reply( 'OK' )
LUA
;
    my $redis = $redis;
    weaken( $redis );
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
  }
);

is_deeply( \@t_data, [ qw( OK OK ) ], 'Script execution' );

$redis->disconnect();
