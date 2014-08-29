use 5.008000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
use Scalar::Util qw( weaken );
require 't/test_helper.pl';

BEGIN {
  eval "use Test::LeakTrace 0.14";
  if ( $@ ) {
    plan skip_all => 'Test::LeakTrace 0.14 required for this test';
  }
  elsif ( $^O eq 'MSWin32' ) {
    plan skip_all => 'can\'t correctly run on MSWin32 platform';
  }
}

my $SERVER_INFO = run_redis_instance();
if ( !defined $SERVER_INFO ) {
  plan skip_all => 'redis-server is required for this test';
}
plan tests => 10;

my $REDIS = AnyEvent::Redis::RipeRedis->new(
  host => $SERVER_INFO->{host},
  port => $SERVER_INFO->{port},
);

t_leaks_status_reply_mth1( $REDIS );
t_leaks_status_reply_mth2( $REDIS );

t_leaks_bulk_reply_mth1( $REDIS );
t_leaks_bulk_reply_mth2( $REDIS );

t_leaks_mbulk_reply_mth1( $REDIS );
t_leaks_mbulk_reply_mth2( $REDIS );

t_leaks_nested_mbulk_reply_mth1( $REDIS );
t_leaks_nested_mbulk_reply_mth2( $REDIS );

my $ver = get_redis_version( $REDIS );

SKIP: {
  if ( $ver < 2.00600 ) {
    skip 'redis-server 2.6 or higher is required for this test', 2;
  }

  t_leaks_eval_cached_mth1( $REDIS );
  t_leaks_eval_cached_mth2( $REDIS );
}

$REDIS->disconnect();


####
sub t_leaks_status_reply_mth1 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        $redis->set( 'foo', "some\r\nstring",
          { on_done => sub {
              my $data = shift;
            },
          }
        );

        $redis->del( 'foo',
          { on_done => sub {
              $cv->send();
            }
          }
        );
      }
    );
  } 'leaks; \'on_done\' used; status reply';

  return;
}

####
sub t_leaks_status_reply_mth2 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        $redis->set( 'foo', "some\r\nstring",
          sub {
            my $data    = shift;
            my $err_msg = shift;

            if ( defined $err_msg ) {
              diag( $err_msg );
            }
          }
        );

        $redis->del( 'foo',
          sub {
            my $err_msg = $_[1];

            if ( defined $err_msg ) {
              diag( $err_msg );
            }

            $cv->send();
          }
        );
      }
    );
  } 'leaks; \'on_reply\' used; status reply';

  return;
}

####
sub t_leaks_bulk_reply_mth1 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        $redis->set( 'foo', "some\r\nstring" );

        $redis->get( 'foo',
          { on_done => sub {
              my $data = shift;
            },
          }
        );

        $redis->del( 'foo',
          { on_done => sub {
              $cv->send();
            }
          }
        );
      }
    );
  } 'leaks; \'on_done\' used; bulk reply';

  return;
}

####
sub t_leaks_bulk_reply_mth2 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        $redis->set( 'foo', "some\r\nstring" );

        $redis->get( 'foo',
          sub {
            my $data    = shift;
            my $err_msg = shift;

            if ( defined $err_msg ) {
              diag( $err_msg );
            }
          }
        );

        $redis->del( 'foo',
          sub {
            my $err_msg = $_[1];

            if ( defined $err_msg ) {
              diag( $err_msg );
            }

            $cv->send();
          }
        );
      }
    );
  } 'leaks; \'on_reply\' used; bulk reply';

  return;
}

####
sub t_leaks_mbulk_reply_mth1 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        for ( my $i = 1; $i <= 3; $i++ ) {
          $redis->rpush( 'list', "element_$i" );
        }

        $redis->lrange( 'list', 0, -1,
          { on_done => sub {
              my $data = shift;
            },
          }
        );

        $redis->del( 'list',
          { on_done => sub {
              $cv->send();
            }
          }
        );
      }
    );
  } 'leaks; \'on_done\' used; multi-bulk reply';

  return;
}

####
sub t_leaks_mbulk_reply_mth2 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        for ( my $i = 1; $i <= 3; $i++ ) {
          $redis->rpush( 'list', "element_$i" );
        }

        $redis->lrange( 'list', 0, -1,
          sub {
            my $data    = shift;
            my $err_msg = shift;

            if ( defined $err_msg ) {
              diag( $err_msg );
            }
          }
        );

        $redis->del( 'list',
          sub {
            my $err_msg = $_[1];

            if ( defined $err_msg ) {
              diag( $err_msg );
            }

            $cv->send();
          }
        );
      }
    );
  } 'leaks; \'on_reply\' used; multi-bulk reply';

  return;
}

####
sub t_leaks_nested_mbulk_reply_mth1 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        for ( my $i = 1; $i <= 3; $i++ ) {
          $redis->rpush( 'list', "element_$i" );
        }

        $redis->set( 'foo', "some\r\nstring" );

        $redis->multi();
        $redis->incr( 'bar' );
        $redis->lrange( 'list', 0, -1 );
        $redis->lrange( 'non_existent', 0, -1 );
        $redis->get( 'foo' );
        $redis->lrange( 'list', 0, -1 );
        $redis->exec(
          { on_done => sub {
              my $data = shift;
            },
          }
        );

        $redis->del( qw( foo list bar ),
          { on_done => sub {
              $cv->send();
            },
          }
        );
      }
    );
  } 'leaks; \'on_done\' used; nested multi-bulk reply';

  return;
}

####
sub t_leaks_nested_mbulk_reply_mth2 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        for ( my $i = 1; $i <= 3; $i++ ) {
          $redis->rpush( 'list', "element_$i" );
        }

        $redis->set( 'foo', "some\r\nstring" );

        $redis->multi();
        $redis->incr( 'bar' );
        $redis->lrange( 'list', 0, -1 );
        $redis->lrange( 'non_existent', 0, -1 );
        $redis->get( 'foo' );
        $redis->lrange( 'list', 0, -1 );
        $redis->exec(
          sub {
            my $data    = shift;
            my $err_msg = shift;

            if ( defined $err_msg ) {
              diag( $err_msg );
            }
          },
        );

        $redis->del( qw( foo list bar ),
          sub {
            my $err_msg = $_[1];

            if ( defined $err_msg ) {
              diag( $err_msg );
            }

            $cv->send();
          }
        );
      }
    );
  } 'leaks; \'on_reply\' used; nested multi-bulk reply';

  return;
}

####
sub t_leaks_eval_cached_mth1 {
  my $redis = shift;

  my $script = <<LUA
return ARGV[1]
LUA
;
  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        my $redis = $redis;
        weaken( $redis );

        $redis->eval_cached( $script, 0, 42,
          { on_done => sub {
              my $data = shift;

              $redis->eval_cached( $script, 0, 57,
                {
                  on_done => sub {
                    my $data = shift;
                    $cv->send();
                  },
                }
              );
            },
          }
        );
      }
    );
  } 'leaks; eval_cached; \'on_done\' used';

  return;
}

####
sub t_leaks_eval_cached_mth2 {
  my $redis = shift;

  my $script = <<LUA
return ARGV[1]
LUA
;
  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        my $redis = $redis;
        weaken( $redis );

        $redis->eval_cached( $script, 0, 42,
          sub {
            my $data    = shift;
            my $err_msg = shift;

            if ( defined $err_msg ) {
              diag( $err_msg );
              return;
            }

            $redis->eval_cached( $script, 0, 57,
              sub {
                my $data    = shift;
                my $err_msg = shift;

                if ( defined $err_msg ) {
                  diag( $err_msg );
                }

                $cv->send();
              }
            );
          }
        );
      }
    );
  } 'leaks; eval_cached; \'on_reply\' used';

  return;
}
