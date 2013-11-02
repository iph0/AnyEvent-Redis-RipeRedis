use 5.008000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
require 't/test_helper.pl';

BEGIN {
  eval "use Test::LeakTrace 0.14";
  if ( $@ ) {
    plan skip_all => "Test::LeakTrace 0.14 required for this test";
  }
}

my $SERVER_INFO = run_redis_instance();
if ( !defined( $SERVER_INFO ) ) {
  plan skip_all => 'redis-server is required for this test';
}
plan tests => 4;

my $REDIS = AnyEvent::Redis::RipeRedis->new(
  host => $SERVER_INFO->{host},
  port => $SERVER_INFO->{port},
);

t_no_leaks_status_reply( $REDIS );
t_no_leaks_mbulk_reply( $REDIS );
t_no_leaks_transaction( $REDIS );
t_no_leaks_eval_cached( $REDIS );

$REDIS->disconnect();


####
sub t_no_leaks_status_reply {
  my $redis = shift;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'foo', 'string',
        { on_done => sub {
            $cv->send();
          },
        }
      );
    }
  );

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        $redis->get( 'bar',
          { on_done => sub {
              my $data = shift;
            },
          }
        );

        $redis->get( 'bar',
          sub {
            my $data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
            }

            $cv->send();
          }
        );
      }
    );
  } 'status reply';

  return;
}

####
sub t_no_leaks_mbulk_reply {
  my $redis = shift;

  ev_loop(
    sub {
      my $cv = shift;

      my $oprn_cnt = 0;
      for ( my $i = 1; $i <= 3; $i++ ) {
        $redis->rpush( 'list', "element_$i", {
          on_done => sub {
            $oprn_cnt++;
            if ( $oprn_cnt == 3 ) {
              $cv->send();
            }
          }
        } );
      }
    }
  );

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        $redis->lrange( 'list', 0, -1,
          { on_done => sub {
              my $data = shift;
            },
          }
        );

        $redis->lrange( 'list', 0, -1,
          sub {
            my $data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
            }

            $cv->send();
          }
        );
      }
    );
  } 'multi-bulk reply';

  return;
}

####
sub t_no_leaks_transaction {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        $redis->multi();
        $redis->get( 'foo' );
        $redis->lrange( 'list', 0, -1 );
        $redis->get( 'foo' );
        $redis->lrange( 'list', 0, -1 );
        $redis->exec(
          { on_done => sub {
              my $data = shift;
            },
          }
        );

        $redis->multi();
        $redis->get( 'foo' );
        $redis->lrange( 'list', 0, -1 );
        $redis->get( 'foo' );
        $redis->lrange( 'list', 0, -1 );
        $redis->exec(
          sub {
            my $data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
            }

            $cv->send();
          },
        );
      }
    );
  } 'transaction';

  return;
}

####
sub t_no_leaks_eval_cached {
  my $redis = shift;

  my $ver = get_redis_version( $redis );

  SKIP: {
    if ( $ver < 2.00600 ) {
      skip 'redis-server 2.6 or higher is required for this test', 1;
    }

    my $script = <<LUA
return ARGV[1]
LUA
;
    no_leaks_ok {
      ev_loop(
        sub {
          my $cv = shift;

          $redis->eval_cached( $script, 0, 42,
            { on_done => sub {
                my $data = shift;
              },
            }
          );

          $redis->eval_cached( $script, 0, 9,
            sub {
              my $data = shift;

              if ( defined( $_[0] ) ) {
                diag( $_[0] );
              }

              $cv->send();
            }
          );
        }
      );
    } 'eval_cached';
  }

  return;
}
