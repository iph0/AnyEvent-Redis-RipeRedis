use 5.008000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
use Digest::SHA qw( sha1_hex );
use Scalar::Util qw( weaken );
require 't/test_helper.pl';

my $SERVER_INFO = run_redis_instance();
if ( !defined $SERVER_INFO ) {
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
plan tests => 27;

can_ok( $REDIS, 'eval_cached' );

t_no_script( $REDIS );

t_eval_cached_mth1( $REDIS );
t_eval_cached_mth2( $REDIS );

t_error_reply_mth1( $REDIS );
t_error_reply_mth2( $REDIS );

t_errors_in_mbulk_reply_mth1( $REDIS );
t_errors_in_mbulk_reply_mth2( $REDIS );

$REDIS->disconnect();


####
sub t_no_script {
  my $redis = shift;

  my $t_err_msg;
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
            $t_err_msg  = shift;
            $t_err_code = shift;

            $cv->send();
          },
        }
      );
    }
  );

  like( $t_err_msg, qr/^NOSCRIPT/, 'no script; \'on_error\' used; error message' );
  is( $t_err_code, E_NO_SCRIPT, 'no script; \'on_error\' used; error code' );

  return;
}

####
sub t_eval_cached_mth1 {
  my $redis = shift;

  my $script = <<LUA
return ARGV[1]
LUA
;
  my @t_data;

  ev_loop(
    sub {
      my $cv = shift;

      my $redis = $redis;
      weaken( $redis );

      $redis->eval_cached( $script, 0, 42,
        { on_done => sub {
            my $reply = shift;

            push( @t_data, $reply );

            $redis->eval_cached( $script, 0, 15 );

            $redis->eval_cached( $script, 0, 57,
              {
                on_done => sub {
                  my $reply = shift;
                  push( @t_data, $reply );
                  $cv->send();
                },
              }
            );
          },
        }
      );
    }
  );

  is_deeply( \@t_data, [ qw( 42 57 ) ], 'eval_cached; \'on_done\' used' );

  return;
}

####
sub t_eval_cached_mth2 {
  my $redis = shift;

  my $script = <<LUA
return ARGV[1]
LUA
;
  my @t_data;

  ev_loop(
    sub {
      my $cv = shift;

      my $redis = $redis;
      weaken( $redis );

      $redis->eval_cached( $script, 0, 42,
        sub {
          my $reply   = shift;
          my $err_msg = shift;

          if ( defined $err_msg ) {
            diag( $err_msg );
            return;
          }

          push( @t_data, $reply );

          $redis->eval_cached( $script, 0, 15 );

          $redis->eval_cached( $script, 0, 57,
            sub {
              my $reply   = shift;
              my $err_msg = shift;

              if ( defined $err_msg ) {
                diag( $err_msg );
                return;
              }

              push( @t_data, $reply );

              $cv->send();
            }
          );
        }
      );
    }
  );

  is_deeply( \@t_data, [ qw( 42 57 ) ], 'eval_cached; \'on_reply\' used' );

  return;
}

####
sub t_error_reply_mth1 {
  my $redis = shift;

  my $script = <<LUA
return redis.error_reply( "ERR Something wrong." )
LUA
;
  my $t_err_msg;
  my $t_err_code;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->eval_cached( $script, 0,
        { on_error => sub {
            $t_err_msg  = shift;
            $t_err_code = shift;

            $cv->send();
          },
        }
      );
    }
  );

  is( $t_err_msg, 'ERR Something wrong.',
      'eval_cached; \'on_error\' used; error reply; error message' );
  is( $t_err_code, E_OPRN_ERROR,
      'eval_cached; \'on_error\' used; error reply; error code' );

  return;
}

####
sub t_error_reply_mth2 {
  my $redis = shift;

  my $script = <<LUA
return redis.error_reply( "ERR Something wrong." )
LUA
;
  my $t_err_msg;
  my $t_err_code;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->eval_cached( $script, 0,
        sub {
          my $reply  = shift;
          $t_err_msg = shift;

          if ( defined $t_err_msg ) {
            $t_err_code = shift;
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_err_msg, 'ERR Something wrong.',
      'eval_cached; \'on_reply\' used; error reply; error message' );
  is( $t_err_code, E_OPRN_ERROR,
      'eval_cached; \'on_reply\' used; error reply; error code' );

  return;
}

####
sub t_errors_in_mbulk_reply_mth1 {
  my $redis = shift;

  my $script = <<LUA
return { ARGV[1], redis.error_reply( "Something wrong." ),
    { redis.error_reply( "NOSCRIPT No matching script." ) } }
LUA
;
  my $t_err_msg;
  my $t_err_code;
  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->eval( $script, 0, 42,
        { on_error => sub {
            $t_err_msg  = shift;
            $t_err_code = shift;
            $t_reply    = shift;

            $cv->send();
          },
        }
      );
    }
  );

  is( $t_err_msg, 'Operation \'eval\' completed with errors.',
      'errors in multi-bulk reply; \'on_error\' used; error message' );
  is( $t_err_code, E_OPRN_ERROR,
      'errors in multi-bulk reply; \'on_error\' used; error code' );
  is( $t_reply->[0], 42,
      'errors in multi-bulk reply; \'on_error\' used; numeric reply' );
  isa_ok( $t_reply->[1], 'AnyEvent::Redis::RipeRedis::Error',
      'errors in multi-bulk reply; \'on_error\' used; level_0;' );
  is( $t_reply->[1]->message(), 'Something wrong.',
      'errors in multi-bulk reply; \'on_error\' used; level_0; error message' );
  is( $t_reply->[1]->code(), E_OPRN_ERROR,
      'errors in multi-bulk reply; \'on_error\' used; level_0; error code' );
  isa_ok( $t_reply->[2][0], 'AnyEvent::Redis::RipeRedis::Error',
      'errors in multi-bulk reply; \'on_error\' used; level_1;' );
  is( $t_reply->[2][0]->message(), 'NOSCRIPT No matching script.',
      'errors in multi-bulk reply; \'on_error\' used; level_1; error message' );
  is( $t_reply->[2][0]->code(), E_NO_SCRIPT,
      'errors in multi-bulk reply; \'on_error\' used; level_1; error code' );

  return;
}

####
sub t_errors_in_mbulk_reply_mth2 {
  my $redis = shift;

  my $script = <<LUA
return { ARGV[1], redis.error_reply( "Something wrong." ),
    { redis.error_reply( "NOSCRIPT No matching script." ) } }
LUA
;
  my $t_err_msg;
  my $t_err_code;
  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->eval( $script, 0, 42,
        sub {
          $t_reply   = shift;
          $t_err_msg = shift;

          if ( defined $t_err_msg ) {
            $t_err_code = shift;
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_err_msg, 'Operation \'eval\' completed with errors.',
      'errors in multi-bulk reply; \'on_reply\' used; error message' );
  is( $t_err_code, E_OPRN_ERROR,
      'errors in multi-bulk reply; \'on_reply\' used; error code' );
  is( $t_reply->[0], 42,
      'errors in multi-bulk reply; \'on_reply\' used; numeric reply' );
  isa_ok( $t_reply->[1], 'AnyEvent::Redis::RipeRedis::Error',
      'errors in multi-bulk reply; \'on_reply\' used; level_0;' );
  is( $t_reply->[1]->message(), 'Something wrong.',
      'errors in multi-bulk reply; \'on_reply\' used; level_0; error message' );
  is( $t_reply->[1]->code(), E_OPRN_ERROR,
      'errors in multi-bulk reply; \'on_reply\' used; level_0; error code' );
  isa_ok( $t_reply->[2][0], 'AnyEvent::Redis::RipeRedis::Error',
      'errors in multi-bulk reply; \'on_reply\' used; level_1;' );
  is( $t_reply->[2][0]->message(), 'NOSCRIPT No matching script.',
      'errors in multi-bulk reply; \'on_reply\' used; level_1; error message' );
  is( $t_reply->[2][0]->code(), E_NO_SCRIPT,
      'errors in multi-bulk reply; \'on_reply\' used; level_1; error code' );

  return;
}
