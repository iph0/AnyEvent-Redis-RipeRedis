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
    plan skip_all => "Test::LeakTrace 0.14 required for this test";
  }
}

my $SERVER_INFO = run_redis_instance();
if ( !defined( $SERVER_INFO ) ) {
  plan skip_all => 'redis-server is required for this test';
}
plan tests => 18;

my $REDIS = AnyEvent::Redis::RipeRedis->new(
  host => $SERVER_INFO->{host},
  port => $SERVER_INFO->{port},
);

t_leaks_status_reply_mth1( $REDIS );
t_leaks_status_reply_mth2( $REDIS );
t_leaks_status_reply_mth3( $REDIS );
t_leaks_status_reply_mth4( $REDIS );

t_leaks_bulk_reply_mth1( $REDIS );
t_leaks_bulk_reply_mth2( $REDIS );
t_leaks_bulk_reply_mth3( $REDIS );
t_leaks_bulk_reply_mth4( $REDIS );

t_leaks_mbulk_reply_mth1( $REDIS );
t_leaks_mbulk_reply_mth2( $REDIS );
t_leaks_mbulk_reply_mth3( $REDIS );
t_leaks_mbulk_reply_mth4( $REDIS );

t_leaks_nested_mbulk_reply_mth1( $REDIS );
t_leaks_nested_mbulk_reply_mth2( $REDIS );
t_leaks_nested_mbulk_reply_mth3( $REDIS );
t_leaks_nested_mbulk_reply_mth4( $REDIS );

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
              $cv->send();
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
            my $t_data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
            }
          }
        );

        $redis->del( 'foo',
          sub {
            if ( defined( $_[1] ) ) {
              diag( $_[1] );
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
sub t_leaks_status_reply_mth3 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        $redis->execute_cmd(
          { keyword => 'set',
            args    => [ 'bar', "some\r\nstring" ],
            on_done => sub {
              my $t_data = shift;
            },
          }
        );

        $redis->execute_cmd(
          { keyword => 'del',
            args    => [ 'foo' ],
            on_done => sub {
              $cv->send();
            }
          }
        );
      }
    );
  } 'leaks (execute_cmd); \'on_done\' used; status reply';

  return;
}

####
sub t_leaks_status_reply_mth4 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        $redis->execute_cmd(
          { keyword  => 'set',
            args     => [ 'bar', "some\r\nstring" ],
            on_reply => sub {
              my $t_data = shift;

              if ( defined( $_[0] ) ) {
                diag( $_[0] );
              }
            },
          }
        );

        $redis->execute_cmd(
          { keyword  => 'del',
            args     => [ 'bar' ],
            on_reply => sub {
              if ( defined( $_[1] ) ) {
                diag( $_[1] );
              }

              $cv->send();
            },
          }
        );
      }
    );
  } 'leaks (execute_cmd); \'on_reply\' used; status reply';

  return;
}
#
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
              my $t_data = shift;
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
            my $t_data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
            }
          }
        );

        $redis->del( 'foo',
          sub {
            if ( defined( $_[1] ) ) {
              diag( $_[1] );
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
sub t_leaks_bulk_reply_mth3 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        $redis->execute_cmd(
          { keyword => 'set',
            args    => [ 'foo', "some\r\nstring" ],
          }
        );

        $redis->execute_cmd(
          { keyword => 'get',
            args    => [ 'foo' ],
            on_done => sub {
              my $t_data = shift;
            },
          }
        );

        $redis->execute_cmd(
          { keyword => 'del',
            args    => [ 'foo' ],
            on_done => sub {
              $cv->send();
            }
          }
        );
      }
    );
  } 'leaks (execute_cmd); \'on_done\' used; bulk reply';

  return;
}

####
sub t_leaks_bulk_reply_mth4 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        $redis->execute_cmd(
          { keyword => 'set',
            args    => [ 'foo', "some\r\nstring" ],
          }
        );

        $redis->execute_cmd(
          { keyword  => 'get',
            args     => [ 'foo' ],
            on_reply => sub {
              my $t_data = shift;

              if ( defined( $_[0] ) ) {
                diag( $_[0] );
              }
            },
          }
        );

        $redis->execute_cmd(
          { keyword  => 'del',
            args     => [ 'foo' ],
            on_reply => sub {
              if ( defined( $_[1] ) ) {
                diag( $_[1] );
              }

              $cv->send();
            },
          }
        );
      }
    );
  } 'leaks (execute_cmd); \'on_reply\' used; bulk reply';

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
              my $t_data = shift;
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
            my $t_data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
            }
          }
        );

        $redis->del( 'list',
          sub {
            if ( defined( $_[1] ) ) {
              diag( $_[1] );
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
sub t_leaks_mbulk_reply_mth3 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        for ( my $i = 1; $i <= 3; $i++ ) {
          $redis->execute_cmd(
            { keyword => 'rpush',
              args    => [ 'list', "element_$i" ],
            }
          );
        }

        $redis->execute_cmd(
          { keyword => 'lrange',
            args    => [ 'list', 0, -1 ],
            on_done => sub {
              my $t_data = shift;
            },
          }
        );

        $redis->execute_cmd(
          { keyword => 'del',
            args    => [ 'list' ],
            on_done => sub {
              $cv->send();
            }
          }
        );
      }
    );
  } 'leaks (execute_cmd); \'on_done\' used; multi-bulk reply';

  return;
}

####
sub t_leaks_mbulk_reply_mth4 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        for ( my $i = 1; $i <= 3; $i++ ) {
          $redis->execute_cmd(
            { keyword => 'rpush',
              args    => [ 'list', "element_$i" ],
            }
          );
        }

        $redis->execute_cmd(
          { keyword  => 'lrange',
            args     => [ 'list', 0, -1 ],
            on_reply => sub {
              my $t_data = shift;

              if ( defined( $_[0] ) ) {
                diag( $_[0] );
              }
            },
          }
        );

        $redis->execute_cmd(
          { keyword  => 'del',
            args     => [ 'list' ],
            on_reply => sub {
              if ( defined( $_[1] ) ) {
                diag( $_[1] );
              }

              $cv->send();
            },
          }
        );
      }
    );
  } 'leaks (execute_cmd); \'on_reply\' used; multi-bulk reply';

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
              my $t_data = shift;
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
            my $t_data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
            }
          },
        );

        $redis->del( qw( foo list bar ),
          sub {
            if ( defined( $_[1] ) ) {
              diag( $_[1] );
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
sub t_leaks_nested_mbulk_reply_mth3 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        for ( my $i = 1; $i <= 3; $i++ ) {
          $redis->execute_cmd(
            { keyword => 'rpush',
              args    => [ 'list', "element_$i" ],
            }
          );
        }

        $redis->execute_cmd(
          { keyword => 'set',
            args    => [ 'foo', "some\r\nstring" ],
          }
        );

        $redis->execute_cmd( { keyword => 'multi' } );
        $redis->execute_cmd(
          { keyword => 'incr',
            args    => [ 'bar' ],
          }
        );
        $redis->execute_cmd(
          { keyword => 'lrange',
            args    => [ 'list', 0, -1 ],
          }
        );
        $redis->execute_cmd(
          { keyword => 'lrange',
            args    => [ 'non_existent', 0, -1 ],
          }
        );
        $redis->execute_cmd(
          { keyword => 'get',
            args    => [ 'foo' ],
          }
        );
        $redis->execute_cmd(
          { keyword => 'lrange',
            args    => [ 'list', 0, -1 ],
          }
        );
        $redis->execute_cmd(
          { keyword => 'exec',
            on_done => sub {
              my $t_data = shift;
            },
          }
        );

        $redis->execute_cmd(
          { keyword => 'del',
            args    => [ qw( foo list bar ) ],
            on_done => sub {
              $cv->send();
            }
          }
        );
      }
    );
  } 'leaks (execute_cmd); \'on_done\' used; nested multi-bulk reply';

  return;
}

####
sub t_leaks_nested_mbulk_reply_mth4 {
  my $redis = shift;

  no_leaks_ok {
    ev_loop(
      sub {
        my $cv = shift;

        for ( my $i = 1; $i <= 3; $i++ ) {
          $redis->execute_cmd(
            { keyword => 'rpush',
              args    => [ 'list', "element_$i" ],
            }
          );
        }

        $redis->execute_cmd(
          { keyword => 'set',
            args    => [ 'foo', "some\r\nstring" ],
          }
        );

        $redis->execute_cmd( { keyword => 'multi' } );
        $redis->execute_cmd(
          { keyword => 'incr',
            args    => [ 'bar' ],
          }
        );
        $redis->execute_cmd(
          { keyword => 'lrange',
            args    => [ 'list', 0, -1 ],
          }
        );
        $redis->execute_cmd(
          { keyword => 'lrange',
            args    => [ 'non_existent', 0, -1 ],
          }
        );
        $redis->execute_cmd(
          { keyword => 'get',
            args    => [ 'foo' ],
          }
        );
        $redis->execute_cmd(
          { keyword => 'lrange',
            args    => [ 'list', 0, -1 ],
          }
        );
        $redis->execute_cmd(
          { keyword  => 'exec',
            on_reply => sub {
              my $t_data = shift;

              if ( defined( $_[0] ) ) {
                diag( $_[0] );
              }
            },
          }
        );

        $redis->execute_cmd(
          { keyword  => 'del',
            args     => [ qw( foo list bar ) ],
            on_reply => sub {
              if ( defined( $_[1] ) ) {
                diag( $_[1] );
              }

              $cv->send();
            },
          }
        );
      }
    );
  } 'leaks (execute_cmd); \'on_reply\' used; nested multi-bulk reply';

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
            my $data = shift;

            $redis->eval_cached( $script, 0, 57,
              sub {
                my $data = shift;
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
