use 5.008000;
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
plan tests => 13;

t_no_script( $redis );
t_eval_cached( $redis );
t_on_error_in_eval_cached( $redis );
t_errors_in_mbulk_reply( $redis );

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
    }
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

  my $t_err_msg;
  my $t_err_code;

  my $script = <<LUA
return redis.error_reply( "Something wrong." )
LUA
;
  ev_loop(
    sub {
      my $cv = shift;

      $redis->eval_cached( $script, 0, {
        on_error => sub {
          $t_err_msg = shift;
          $t_err_code = shift;
          $cv->send();
        },
      } );
    }
  );

  my $t_name = "'on_error' in eval_cached;";
  is( $t_err_msg, 'Something wrong.', "$t_name; error message" );
  is( $t_err_code, E_OPRN_ERROR, "$t_name; error code" );

  return;
}

####
sub t_errors_in_mbulk_reply {
  my $redis = shift;

  my $t_err_msg;
  my $t_err_code;
  my $t_err_data;

  my $script = <<LUA
return {
  42,
  redis.error_reply( "Something wrong lv0." ),
  { redis.error_reply( "Something wrong lv1." ) }
}
LUA
;
  ev_loop(
    sub {
      my $cv = shift;

      $redis->eval( $script, 0, {
        on_error => sub {
          $t_err_msg = shift;
          $t_err_code = shift;
          $t_err_data = shift;
          $cv->send();
        },
      } );
    }
  );

  my $t_name = 'errors in multi-bulk reply';
  is( $t_err_msg, "Operation 'eval' completed with errors.",
      "$t_name; error message" );
  is( $t_err_code, E_OPRN_ERROR, "$t_name; error code" );
  is( $t_err_data->[0], 42, "$t_name; numeric reply" );
  isa_ok( $t_err_data->[1], 'AnyEvent::Redis::RipeRedis::Error',
      "$t_name; lv0" );
  is( $t_err_data->[1]->message(), 'Something wrong lv0.',
      "$t_name; lv0 error message" );
  is( $t_err_data->[1]->code(), E_OPRN_ERROR, "$t_name; lv0 error code" );
  isa_ok( $t_err_data->[2][0], 'AnyEvent::Redis::RipeRedis::Error',
      "$t_name; lv1" );
  is( $t_err_data->[2][0]->message(), 'Something wrong lv1.',
      "$t_name; lv1 error message" );
  is( $t_err_data->[2][0]->code(), E_OPRN_ERROR, "$t_name; lv1 error code" );

  return;
}
