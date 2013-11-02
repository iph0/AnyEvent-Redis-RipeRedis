use 5.008000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
use Digest::SHA qw( sha1_hex );
use Scalar::Util qw( weaken );
require 't/test_helper.pl';

my $SERVER_INFO = run_redis_instance();
if ( !defined( $SERVER_INFO ) ) {
  plan skip_all => 'redis-server is required for this test';
}
my $REDIS = AnyEvent::Redis::RipeRedis->new(
  host => $SERVER_INFO->{host},
  port => $SERVER_INFO->{port},
);
my $ver = get_redis_version( $REDIS );
if ( $ver < 2.00600 ) {
  plan skip_all => 'redis-server 2.6 or higher is required for this test';
}
plan tests => 25;

t_no_script( $REDIS );
t_eval_cached( $REDIS );
t_on_error_in_eval_cached( $REDIS );
t_errors_in_mbulk_reply( $REDIS );

$REDIS->disconnect();


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

      $redis->evalsha( $script_sha1, 0,
        { on_error => sub {
            $t_err_code = pop;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_err_code, E_NO_SCRIPT, 'no script' );

  return;
}

####
sub t_eval_cached {
  my $redis = shift;

  my $script = <<LUA
return ARGV[1]
LUA
;

  my @t_data1;

  ev_loop(
    sub {
      my $cv = shift;

      my $redis = $redis;
      weaken( $redis );

      $redis->eval_cached( $script, 0, 42,
        { on_done => sub {
            my $data = shift;
            push( @t_data1, $data );

            $redis->eval_cached( $script, 0, 15 );

            $redis->eval_cached( $script, 0, 57,
              {
                on_done => sub {
                  my $data = shift;
                  push( @t_data1, $data );
                  $cv->send();
                },
              }
            );
          },
        }
      );
    }
  );

  is_deeply( \@t_data1, [ qw( 42 57 ) ], 'eval_cached; on_done' );

  my @t_data2;

  ev_loop(
    sub {
      my $cv = shift;

      my $redis = $redis;
      weaken( $redis );

      $redis->eval_cached( $script, 0, 9,
        sub {
          my $data = shift;
          push( @t_data2, $data );

          $redis->eval_cached( $script, 0, 5 );

          $redis->eval_cached( $script, 0, 17,
            sub {
              my $data = shift;
              push( @t_data2, $data );
              $cv->send();
            }
          );
        }
      );
    }
  );

  is_deeply( \@t_data2, [ qw( 9 17 ) ], 'eval_cached; on_reply' );

  return;
}

####
sub t_on_error_in_eval_cached {
  my $redis = shift;


  my $script = <<LUA
return redis.error_reply( "ERR Something wrong." )
LUA
;

  my $t_err_msg1;
  my $t_err_code1;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->eval_cached( $script, 0,
        { on_error => sub {
            $t_err_msg1 = shift;
            $t_err_code1 = shift;

            $cv->send();
          },
        }
      );
    }
  );

  is( $t_err_msg1, 'ERR Something wrong.',
      "'on_error' in eval_cached; on_error; error message" );
  is( $t_err_code1, E_OPRN_ERROR,
      "'on_error' in eval_cached; on_error; error code" );

  my $t_err_msg2;
  my $t_err_code2;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->eval_cached( $script, 0,
        sub {
          my $data = shift;

          if ( defined( $_[0] ) ) {
            $t_err_msg2 = shift;
            $t_err_code2 = shift;
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_err_msg2, 'ERR Something wrong.',
      "'on_error' in eval_cached; on_reply; error message" );
  is( $t_err_code2, E_OPRN_ERROR,
      "'on_error' in eval_cached; on_reply; error code" );

  return;
}

####
sub t_errors_in_mbulk_reply {
  my $redis = shift;

  my $script = <<LUA
return { ARGV[1], redis.error_reply( "Something wrong." ),
    { redis.error_reply( "NOSCRIPT No matching script." ) } }
LUA
;

  my $t_err_msg1;
  my $t_err_code1;
  my $t_data1;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->eval( $script, 0, 42,
        { on_error => sub {
            $t_err_msg1 = shift;
            $t_err_code1 = shift;
            $t_data1 = shift;

            $cv->send();
          },
        }
      );
    }
  );

  is( $t_err_msg1, "Operation 'eval' completed with errors.",
      "errors in multi-bulk reply; on_error; error message" );
  is( $t_err_code1, E_OPRN_ERROR,
      "errors in multi-bulk reply; on_error; error code" );
  is( $t_data1->[0], 42, "errors in multi-bulk reply; on_error; numeric reply" );
  isa_ok( $t_data1->[1], 'AnyEvent::Redis::RipeRedis::Error',
      "errors in multi-bulk reply; level_0; on_error;" );
  is( $t_data1->[1]->message(), 'Something wrong.',
      "errors in multi-bulk reply; level_0; on_error; error message" );
  is( $t_data1->[1]->code(), E_OPRN_ERROR,
      "errors in multi-bulk reply; level_0; on_error; error code" );
  isa_ok( $t_data1->[2][0], 'AnyEvent::Redis::RipeRedis::Error',
      "errors in multi-bulk reply; level_1; on_error;" );
  is( $t_data1->[2][0]->message(), 'NOSCRIPT No matching script.',
      "errors in multi-bulk reply; level_1; on_error; error message" );
  is( $t_data1->[2][0]->code(), E_NO_SCRIPT,
      "errors in multi-bulk reply; level_1; on_error; error code" );

  my $t_err_msg2;
  my $t_err_code2;
  my $t_data2;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->eval( $script, 0, 9,
        sub {
          $t_data2 = shift;

          if ( defined( $_[0] ) ) {
            $t_err_msg2 = shift;
            $t_err_code2 = shift;
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_err_msg2, "Operation 'eval' completed with errors.",
      "errors in multi-bulk reply; on_reply; error message" );
  is( $t_err_code2, E_OPRN_ERROR,
      "errors in multi-bulk reply; on_reply; error code" );
  is( $t_data2->[0], 9, "errors in multi-bulk reply; on_reply; numeric reply" );
  isa_ok( $t_data2->[1], 'AnyEvent::Redis::RipeRedis::Error',
      "errors in multi-bulk reply; level_0; on_reply;" );
  is( $t_data2->[1]->message(), 'Something wrong.',
      "errors in multi-bulk reply; level_0; on_reply; error message" );
  is( $t_data2->[1]->code(), E_OPRN_ERROR,
      "errors in multi-bulk reply; level_0; on_reply; error code" );
  isa_ok( $t_data2->[2][0], 'AnyEvent::Redis::RipeRedis::Error',
      "errors in multi-bulk reply; level_1; on_reply;" );
  is( $t_data2->[2][0]->message(), 'NOSCRIPT No matching script.',
      "errors in multi-bulk reply; level_1; on_reply; error message" );
  is( $t_data2->[2][0]->code(), E_NO_SCRIPT,
      "errors in multi-bulk reply; level_1; on_reply; error code" );

  return;
}
