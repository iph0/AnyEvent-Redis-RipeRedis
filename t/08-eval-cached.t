use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
use Scalar::Util qw( weaken );
require 't/test_helper.pl';

my $server_info = run_redis_instance();

my $redis = AnyEvent::Redis::RipeRedis->new(
  host => $server_info->{host},
  port => $server_info->{port},
);

my $ver = get_redis_version( $redis );
if ( $ver < 2.00600 ) {
  plan skip_all => 'redis-server 2.6 or higher is required to this test';
}

plan tests => 1;

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
