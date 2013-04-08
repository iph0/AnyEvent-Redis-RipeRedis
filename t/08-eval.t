use 5.006000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
use Digest::SHA qw( sha1_hex );
use Scalar::Util qw( weaken );
require 't/test_helper.pl';

my $server_info = run_redis_instance();
if ( !defined( $server_info ) ) {
  plan skip_all => 'redis-server is required to this test';
}
my $redis = AnyEvent::Redis::RipeRedis->new(
  host => $server_info->{host},
  port => $server_info->{port},
);
my $ver = get_redis_version( $redis );
if ( $ver < 2.00600 ) {
  plan skip_all => 'redis-server 2.6 or higher is required to this test';
}
plan tests => 3;

t_no_script( $redis );
t_eval_cached( $redis );
t_on_error_in_eval_cached( $redis );

$redis->disconnect();


####
sub t_no_script {
  my $redis = shift;

  my $t_err_code;
  my $script = <<LUA
return redis.status_reply( 'OK' )
LUA
;
  my $script_sha1 = sha1_hex( $script );

  ev_loop(
    sub {
      my $cv = shift;

      $redis->evalsha( $script_sha1, 0, {
        on_error => sub {
          $t_err_code = pop;
          $cv->send();
        }
      } );
    },
  );

  is( $t_err_code, E_NO_SCRIPT, 'no script' );

  return;
}

####
sub t_eval_cached {
  my $redis = shift;

  my @t_data;

  my $script = <<LUA
return ARGV[1]
LUA
;
  ev_loop(
    sub {
      my $cv = shift;

      my $redis = $redis;
      weaken( $redis );

      $redis->eval_cached( $script, 0, 42, {
        on_done => sub {
          my $data = shift;
          push( @t_data, $data );

          $redis->eval_cached( $script, 0, 42 );

          $redis->eval_cached( $script, 0, 42, {
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

  is_deeply( \@t_data, [ qw( 42 42 ) ], 'eval_cached' );

  return;
}

####
sub t_on_error_in_eval_cached {
  my $redis = shift;

  my $t_err_code;

  my $script = <<LUA
return ARGV[1]
LUA
;
  ev_loop(
    sub {
      my $cv = shift;

      $redis->eval_cached( $script, 1, {
        on_error => sub {
          $t_err_code = pop;
          $cv->send();
        },
      } );
    }
  );

  is( $t_err_code, E_OPRN_ERROR, "'on_error' in eval_cached" );

  return;
}
