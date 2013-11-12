use 5.008000;
use strict;
use warnings;
use utf8;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
require 't/test_helper.pl';

my $SERVER_INFO = run_redis_instance();
if ( !defined( $SERVER_INFO ) ) {
  plan skip_all => 'redis-server is required for this test';
}
plan tests => 93;

my $REDIS;
my $T_IS_CONN = 0;
my $T_IS_DISCONN = 0;

ev_loop(
  sub {
    my $cv = shift;

    $REDIS = AnyEvent::Redis::RipeRedis->new(
      host               => $SERVER_INFO->{host},
      port               => $SERVER_INFO->{port},
      connection_timeout => 5,
      read_timeout       => 5,
      encoding           => 'utf8',

      on_connect => sub {
        $T_IS_CONN = 1;
        $cv->send();
      },
      on_disconnect => sub {
        $T_IS_DISCONN = 1;
      },
    );
  },
);

ok( $T_IS_CONN, 'on_connect' );

can_ok( $REDIS, 'execute_cmd' );

t_status_reply_mth1( $REDIS );
t_status_reply_mth2( $REDIS );
t_status_reply_mth3( $REDIS );
t_status_reply_mth4( $REDIS );

t_numeric_reply_mth1( $REDIS );
t_numeric_reply_mth2( $REDIS );
t_numeric_reply_mth3( $REDIS );
t_numeric_reply_mth4( $REDIS );

t_bulk_reply_mth1( $REDIS );
t_bulk_reply_mth2( $REDIS );
t_bulk_reply_mth3( $REDIS );
t_bulk_reply_mth4( $REDIS );

t_set_undef_mth1( $REDIS );
t_set_undef_mth2( $REDIS );
t_set_undef_mth3( $REDIS );
t_set_undef_mth4( $REDIS );

t_get_undef_mth1( $REDIS );
t_get_undef_mth2( $REDIS );
t_get_undef_mth3( $REDIS );
t_get_undef_mth4( $REDIS );

t_set_utf8_string_mth1( $REDIS );
t_set_utf8_string_mth2( $REDIS );
t_set_utf8_string_mth3( $REDIS );
t_set_utf8_string_mth4( $REDIS );

t_get_utf8_string_mth1( $REDIS );
t_get_utf8_string_mth2( $REDIS );
t_get_utf8_string_mth3( $REDIS );
t_get_utf8_string_mth4( $REDIS );

t_get_non_existent_mth1( $REDIS );
t_get_non_existent_mth2( $REDIS );
t_get_non_existent_mth3( $REDIS );
t_get_non_existent_mth4( $REDIS );

t_mbulk_reply_mth1( $REDIS );
t_mbulk_reply_mth2( $REDIS );
t_mbulk_reply_mth3( $REDIS );
t_mbulk_reply_mth4( $REDIS );

t_mbulk_reply_empty_list_mth1( $REDIS );
t_mbulk_reply_empty_list_mth2( $REDIS );
t_mbulk_reply_empty_list_mth3( $REDIS );
t_mbulk_reply_empty_list_mth4( $REDIS );

t_mbulk_reply_undef_mth1( $REDIS );
t_mbulk_reply_undef_mth2( $REDIS );
t_mbulk_reply_undef_mth3( $REDIS );
t_mbulk_reply_undef_mth4( $REDIS );

t_nested_mbulk_reply_mth1( $REDIS );
t_nested_mbulk_reply_mth2( $REDIS );
t_nested_mbulk_reply_mth3( $REDIS );
t_nested_mbulk_reply_mth4( $REDIS );

t_oprn_error_mth1( $REDIS );
t_oprn_error_mth2( $REDIS );
t_oprn_error_mth3( $REDIS );
t_oprn_error_mth4( $REDIS );

t_default_on_error( $REDIS );

t_error_after_exec_mth1( $REDIS );
t_error_after_exec_mth2( $REDIS );
t_error_after_exec_mth3( $REDIS );
t_error_after_exec_mth4( $REDIS );

t_quit( $REDIS );


####
sub t_status_reply_mth1 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'foo', "some\r\nstring",
        { on_done => sub {
            $t_data = shift;
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

  is( $t_data, 'OK', 'SET; \'on_done\' used; status reply' );

  return;
}

####
sub t_status_reply_mth2 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'foo', "some\r\nstring",
        sub {
          $t_data = shift;

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

  is( $t_data, 'OK', 'SET; \'on_reply\' used; status reply' );

  return;
}

####
sub t_status_reply_mth3 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword => 'set',
          args    => [ 'foo', "some\r\nstring" ],
          on_done => sub {
            $t_data = shift;
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

  is( $t_data, 'OK', 'SET (execute_cmd); \'on_done\' used; status reply' );

  return;
}

####
sub t_status_reply_mth4 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword  => 'set',
          args     => [ 'foo', "some\r\nstring" ],
          on_reply => sub {
            $t_data = shift;

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

  is( $t_data, 'OK', 'SET (execute_cmd); \'on_reply\' used; status reply' );

  return;
}

####
sub t_numeric_reply_mth1 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->incr( 'bar',
        { on_done => sub {
            $t_data = shift;
          },
        }
      );

      $redis->del( 'bar',
        { on_done => sub {
            $cv->send();
          }
        }
      );
    }
  );

  is( $t_data, 1, 'INCR; \'on_done\' used; numeric reply' );

  return;
}

####
sub t_numeric_reply_mth2 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->incr( 'bar',
        sub {
          $t_data = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }
        }
      );

      $redis->del( 'bar',
        sub {
          if ( defined( $_[1] ) ) {
            diag( $_[1] );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_data, 1, 'INCR; \'on_reply\' used; numeric reply' );

  return;
}

####
sub t_numeric_reply_mth3 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword => 'incr',
          args    => [ 'bar' ],
          on_done => sub {
            $t_data = shift;
          },
        }
      );

      $redis->execute_cmd(
        { keyword => 'del',
          args    => [ 'bar' ],
          on_done => sub {
            $cv->send();
          }
        }
      );
    }
  );

  is( $t_data, 1, 'INCR (execute_cmd); \'on_done\' used; numeric reply' );

  return;
}

####
sub t_numeric_reply_mth4 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword  => 'incr',
          args     => [ 'bar' ],
          on_reply => sub {
            $t_data = shift;

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

  is( $t_data, 1, 'INCR (execute_cmd); \'on_reply\' used; numeric reply' );

  return;
}

####
sub t_bulk_reply_mth1 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'foo', "some\r\nstring" );

      $redis->get( 'foo',
        { on_done => sub {
            $t_data = shift;
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

  is( $t_data, "some\r\nstring", 'GET; \'on_done\' used; bulk reply' );

  return;
}

####
sub t_bulk_reply_mth2 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'foo', "some\r\nstring" );

      $redis->get( 'foo',
        sub {
          $t_data = shift;

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

  is( $t_data, "some\r\nstring", 'GET; \'on_reply\' used; bulk reply' );

  return;
}

####
sub t_bulk_reply_mth3 {
  my $redis = shift;

  my $t_data;

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
            $t_data = shift;
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

  is( $t_data, "some\r\nstring",
      'GET (execute_cmd); \'on_done\' used; bulk reply' );

  return;
}

####
sub t_bulk_reply_mth4 {
  my $redis = shift;

  my $t_data;

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
            $t_data = shift;

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

  is( $t_data, "some\r\nstring",
      'GET (execute_cmd); \'on_reply\' used; bulk reply' );

  return;
}

####
sub t_set_undef_mth1 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'empty', undef,
        { on_done => sub {
            $t_data = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data, 'OK', 'SET; \'on_done\' used; undef' );

  return;
}

####
sub t_set_undef_mth2 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'empty', undef,
        sub {
          $t_data = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }

          $cv->send();
        },
      );
    }
  );

  is( $t_data, 'OK', 'SET; \'on_reply\' used; undef' );

  return;
}

####
sub t_set_undef_mth3 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword => 'set',
          args    => [ 'empty', undef ],
          on_done => sub {
            $t_data = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data, 'OK', 'SET (execute_cmd); \'on_done\' used; undef' );

  return;
}

####
sub t_set_undef_mth4 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword  => 'set',
          args     => [ 'empty', undef ],
          on_reply => sub {
            $t_data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
            }

            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data, 'OK', 'SET (execute_cmd); \'on_reply\' used; undef' );

  return;
}

####
sub t_get_undef_mth1 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'empty',
        { on_done => sub {
            $t_data = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data, '', 'GET; \'on_done\' used; undef' );

  return;
}

####
sub t_get_undef_mth2 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'empty',
        sub {
          $t_data = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_data, '', 'GET; \'on_reply\' used; undef' );

  return;
}

####
sub t_get_undef_mth3 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword => 'get',
          args    => [ 'empty' ],
          on_done => sub {
            $t_data = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data, '', 'GET (execute_cmd); \'on_done\' used; undef' );

  return;
}

####
sub t_get_undef_mth4 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword  => 'get',
          args     => [ 'empty' ],
          on_reply => sub {
            $t_data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
            }

            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data, '', 'GET (execute_cmd); \'on_reply\' used; undef' );

  return;
}

####
sub t_set_utf8_string_mth1 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'ключ', 'Значение',
        { on_done => sub {
            $t_data = shift;
          },
        }
      );

      $redis->del( 'ключ',
        { on_done => sub {
            $cv->send();
          }
        }
      );
    }
  );

  is( $t_data, 'OK', 'SET; \'on_done\' used; UTF-8 string' );

  return;
}

####
sub t_set_utf8_string_mth2 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'ключ', 'Значение',
        sub {
          $t_data = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }
        },
      );

      $redis->del( 'ключ',
        sub {
          if ( defined( $_[1] ) ) {
            diag( $_[1] );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_data, 'OK', 'SET; \'on_reply\' used; UTF-8 string' );

  return;
}

####
sub t_set_utf8_string_mth3 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword => 'set',
          args    => [ 'ключ', 'Значение' ],
          on_done => sub {
            $t_data = shift;
          },
        }
      );

      $redis->execute_cmd(
        { keyword => 'del',
          args    => [ 'ключ' ],
          on_done => sub {
            $cv->send();
          }
        }
      );
    }
  );

  is( $t_data, 'OK', 'SET (execute_cmd); \'on_done\' used; UTF-8 string' );

  return;
}

####
sub t_set_utf8_string_mth4 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword  => 'set',
          args     => [ 'ключ', 'Значение' ],
          on_reply => sub {
            $t_data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
            }
          },
        }
      );

      $redis->execute_cmd(
        { keyword  => 'del',
          args     => [ 'ключ' ],
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

  is( $t_data, 'OK', 'SET (execute_cmd); \'on_reply\' used; UTF-8 string' );

  return;
}

####
sub t_get_utf8_string_mth1 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'ключ', 'Значение' );

      $redis->get( 'ключ',
        { on_done => sub {
            $t_data = shift;
          },
        }
      );

      $redis->del( 'ключ',
        { on_done => sub {
            $cv->send();
          }
        }
      );
    }
  );

  is( $t_data, 'Значение', 'GET; \'on_done\' used; UTF-8 string' );

  return;
}

####
sub t_get_utf8_string_mth2 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'ключ', 'Значение' );

      $redis->get( 'ключ',
        sub {
          $t_data = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }
        }
      );

      $redis->del( 'ключ',
        sub {
          if ( defined( $_[1] ) ) {
            diag( $_[1] );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_data, 'Значение', 'GET; \'on_reply\' used; UTF-8 string' );

  return;
}

####
sub t_get_utf8_string_mth3 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword => 'set',
          args    => [ 'ключ', 'Значение' ],
        }
      );

      $redis->execute_cmd(
        { keyword => 'get',
          args    => [ 'ключ' ],
          on_done => sub {
            $t_data = shift;
          },
        }
      );

      $redis->execute_cmd(
        { keyword => 'del',
          args    => [ 'ключ' ],
          on_done => sub {
            $cv->send();
          }
        }
      );
    }
  );

  is( $t_data, 'Значение', 'GET (execute_cmd); \'on_done\' used; UTF-8 string' );

  return;
}

####
sub t_get_utf8_string_mth4 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword => 'set',
          args    => [ 'ключ', 'Значение' ],
        }
      );

      $redis->execute_cmd(
        { keyword  => 'get',
          args     => [ 'ключ' ],
          on_reply => sub {
            $t_data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
            }
          },
        }
      );

      $redis->execute_cmd(
        { keyword  => 'del',
          args     => [ 'ключ' ],
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

  is( $t_data, 'Значение', 'GET (execute_cmd); \'on_reply\' used; UTF-8 string' );

  return;
}

####
sub t_get_non_existent_mth1 {
  my $redis = shift;

  my $t_data = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'non_existent',
        { on_done => sub {
            $t_data = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data, undef, 'GET; \'on_done\' used; non existent key' );

  return;
}

####
sub t_get_non_existent_mth2 {
  my $redis = shift;

  my $t_data    = 'not_undef';
  my $t_err_msg = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'non_existent',
        sub {
          $t_data    = shift;
          $t_err_msg = shift;

          if ( defined( $t_err_msg ) ) {
            diag( $t_err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  ok( !defined( $t_data ) && !defined( $t_err_msg ),
      'GET; \'on_reply\' used; non existent key' );

  return;
}

####
sub t_get_non_existent_mth3 {
  my $redis = shift;

  my $t_data = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword => 'get',
          args    => [ 'non_existent' ],
          on_done => sub {
            $t_data = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data, undef, 'GET (execute_cmd); \'on_done\' used; non existent key' );

  return;
}

####
sub t_get_non_existent_mth4 {
  my $redis = shift;

  my $t_data    = 'not_undef';
  my $t_err_msg = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword  => 'get',
          args     => [ 'non_existent' ],
          on_reply => sub {
            $t_data    = shift;
            $t_err_msg = shift;

            if ( defined( $t_err_msg ) ) {
              diag( $t_err_msg );
            }

            $cv->send();
          }
        }
      );
    }
  );

  ok( !defined( $t_data ) && !defined( $t_err_msg ),
      'GET (execute_cmd); \'on_reply\' used; non existent key' );

  return;
}

####
sub t_mbulk_reply_mth1 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      for ( my $i = 1; $i <= 3; $i++ ) {
        $redis->rpush( 'list', "element_$i" );
      }

      $redis->lrange( 'list', 0, -1,
        { on_done => sub {
            $t_data = shift;
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

  is_deeply( $t_data,
    [ qw(
        element_1
        element_2
        element_3
      )
    ],
    'LRANGE; \'on_done\' used; multi-bulk reply'
  );

  return;
}

####
sub t_mbulk_reply_mth2 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      for ( my $i = 1; $i <= 3; $i++ ) {
        $redis->rpush( 'list', "element_$i" );
      }

      $redis->lrange( 'list', 0, -1,
        sub {
          $t_data = shift;

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

  is_deeply( $t_data,
    [ qw(
        element_1
        element_2
        element_3
      )
    ],
    'LRANGE; \'on_reply\' used; multi-bulk reply'
  );

  return;
}

####
sub t_mbulk_reply_mth3 {
  my $redis = shift;

  my $t_data;

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
            $t_data = shift;
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

  is_deeply( $t_data,
    [ qw(
        element_1
        element_2
        element_3
      )
    ],
    'LRANGE (execute_cmd); \'on_done\' used; multi-bulk reply'
  );

  return;
}

####
sub t_mbulk_reply_mth4 {
  my $redis = shift;

  my $t_data;

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
            $t_data = shift;

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

  is_deeply( $t_data,
    [ qw(
        element_1
        element_2
        element_3
      )
    ],
    'LRANGE (execute_cmd); \'on_reply\' used; multi-bulk reply'
  );

  return;
}

####
sub t_mbulk_reply_empty_list_mth1 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->lrange( 'non_existent', 0, -1,
        { on_done => sub {
            $t_data = shift;
            $cv->send();
          },
        }
      );
    },
  );

  is_deeply( $t_data, [], 'LRANGE; \'on_done\' used; empty list' );

  return;
}

####
sub t_mbulk_reply_empty_list_mth2 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->lrange( 'non_existent', 0, -1,
        sub {
          $t_data = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }

          $cv->send();
        }
      );
    },
  );

  is_deeply( $t_data, [], 'LRANGE; \'on_reply\' used; empty list' );

  return;
}

####
sub t_mbulk_reply_empty_list_mth3 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword => 'lrange',
          args    => [ 'non_existent', 0, -1 ],
          on_done => sub {
            $t_data = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is_deeply( $t_data, [], 'LRANGE (execute_cmd); \'on_done\' used; empty list' );

  return;
}

####
sub t_mbulk_reply_empty_list_mth4 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword  => 'lrange',
          args     => [ 'non_existent', 0, -1 ],
          on_reply => sub {
            $t_data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
            }

            $cv->send();
          },
        }
      );
    }
  );

  is_deeply( $t_data, [], 'LRANGE (execute_cmd); \'on_reply\' used; empty list' );

  return;
}

####
sub t_mbulk_reply_undef_mth1 {
  my $redis = shift;

  my $t_data = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->brpop( 'non_existent', '1',
        { on_done => sub {
            $t_data = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data, undef, 'BRPOP; \'on_done\' used; multi-bulk undef' );

  return;
}

####
sub t_mbulk_reply_undef_mth2 {
  my $redis = shift;

  my $t_data    = 'not_undef';
  my $t_err_msg = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->brpop( 'non_existent', '1',
        sub {
          $t_data    = shift;
          $t_err_msg = shift;

          if ( defined( $t_err_msg ) ) {
            diag( $t_err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  ok( !defined( $t_data ) && !defined( $t_err_msg ),
      'BRPOP; \'on_reply\' used; multi-bulk undef' );

  return;
}

####
sub t_mbulk_reply_undef_mth3 {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword => 'brpop',
          args    => [ 'non_existent', '1' ],
          on_done => sub {
            $t_data = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data, undef, 'BRPOP (execute_cmd); \'on_done\' used; multi-bulk undef' );

  return;
}

####
sub t_mbulk_reply_undef_mth4 {
  my $redis = shift;

  my $t_data    = 'not_undef';
  my $t_err_msg = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd(
        { keyword  => 'brpop',
          args     => [ 'non_existent', '1' ],
          on_reply => sub {
            $t_data = shift;
            $t_err_msg = shift;

            if ( defined( $t_err_msg ) ) {
              diag( $t_err_msg );
            }

            $cv->send();
          }
        }
      );
    }
  );

  ok( !defined( $t_data ) && !defined( $t_err_msg ),
      'BRPOP (execute_cmd); \'on_reply\' used; multi-bulk undef' );

  return;
}

####
sub t_nested_mbulk_reply_mth1 {
  my $redis = shift;

  my $t_data;

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
            $t_data = shift;
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

  is_deeply( $t_data,
    [ 1,
      [ qw(
          element_1
          element_2
          element_3
        )
      ],
      [],
      "some\r\nstring",
      [ qw(
          element_1
          element_2
          element_3
        )
      ],
    ],
    'EXEC; \'on_done\' used; nested multi-bulk reply'
  );

  return;
}

####
sub t_nested_mbulk_reply_mth2 {
  my $redis = shift;

  my $t_data;

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
          $t_data = shift;

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

  is_deeply( $t_data,
    [ 1,
      [ qw(
          element_1
          element_2
          element_3
        )
      ],
      [],
      "some\r\nstring",
      [ qw(
          element_1
          element_2
          element_3
        )
      ],
    ],
    'EXEC; \'on_reply\' used; nested multi-bulk reply'
  );

  return;
}

####
sub t_nested_mbulk_reply_mth3 {
  my $redis = shift;

  my $t_data;

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
            $t_data = shift;
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

  is_deeply( $t_data,
    [ 1,
      [ qw(
          element_1
          element_2
          element_3
        )
      ],
      [],
      "some\r\nstring",
      [ qw(
          element_1
          element_2
          element_3
        )
      ],
    ],
    'EXEC (execute_cmd); \'on_done\' used; nested multi-bulk reply'
  );

  return;
}

####
sub t_nested_mbulk_reply_mth4 {
  my $redis = shift;

  my $t_data;

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
            $t_data = shift;

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

  is_deeply( $t_data,
    [ 1,
      [ qw(
          element_1
          element_2
          element_3
        )
      ],
      [],
      "some\r\nstring",
      [ qw(
          element_1
          element_2
          element_3
        )
      ],
    ],
    'EXEC (execute_cmd); \'on_reply\' used; nested multi-bulk reply'
  );

  return;
}

####
sub t_oprn_error_mth1 {
  my $redis = shift;

  my $t_err_msg;
  my $t_err_code;

  ev_loop(
    sub {
      my $cv = shift;

      # missing argument
      $redis->set( 'foo',
        { on_error => sub {
            $t_err_msg  = shift;
            $t_err_code = shift;

            $cv->send();
          },
        }
      );
    }
  );

  like( $t_err_msg, qr/^ERR/, 'operation error; \'on_error\' used; error message' );
  is( $t_err_code, E_OPRN_ERROR, 'operation error; \'on_error\' used; error code' );

  return;
}

####
sub t_oprn_error_mth2 {
  my $redis = shift;

  my $t_err_msg;
  my $t_err_code;

  ev_loop(
    sub {
      my $cv = shift;

      # missing argument
      $redis->set( 'foo',
        sub {
          my $data = shift;

          if ( defined( $_[0] ) ) {
            $t_err_msg  = shift;
            $t_err_code = shift;
          }

          $cv->send();
        }
      );
    }
  );

  like( $t_err_msg, qr/^ERR/, 'operation error; \'on_reply\' used; error message' );
  is( $t_err_code, E_OPRN_ERROR, 'operation error; \'on_reply\' used; error code' );

  return;
}

####
sub t_oprn_error_mth3 {
  my $redis = shift;

  my $t_err_msg;
  my $t_err_code;

  ev_loop(
    sub {
      my $cv = shift;

      # missing argument
      $redis->execute_cmd(
        { keyword  => 'set',
          args     => [ 'foo' ],
          on_error => sub {
            $t_err_msg  = shift;
            $t_err_code = shift;

            $cv->send();
          },
        }
      );
    }
  );

  like( $t_err_msg, qr/^ERR/,
      'operation error (execute_cmd); \'on_error\' used; error message' );
  is( $t_err_code, E_OPRN_ERROR,
      'operation error (execute_cmd); \'on_error\' used; error code' );

  return;
}

####
sub t_oprn_error_mth4 {
  my $redis = shift;

  my $t_err_msg;
  my $t_err_code;

  ev_loop(
    sub {
      my $cv = shift;

      # missing argument
      $redis->execute_cmd(
        { keyword  => 'set',
          args     => [ 'foo' ],
          on_reply => sub {
            my $data = shift;

            if ( defined( $_[0] ) ) {
              $t_err_msg  = shift;
              $t_err_code = shift;
            }

            $cv->send();
          },
        }
      );
    }
  );

  like( $t_err_msg, qr/^ERR/,
      'operation error (execute_cmd); \'on_reply\' used; error message' );
  is( $t_err_code, E_OPRN_ERROR,
      'operation error (execute_cmd); \'on_reply\' used; error code' );

  return;
}

####
sub t_default_on_error {
  my $redis = shift;

  local %SIG;

  my $t_err_msg;

  ev_loop(
    sub {
      my $cv = shift;

      $SIG{__WARN__} = sub {
        $t_err_msg = shift;
        chomp( $t_err_msg );
        $cv->send();
      };
      $redis->set(); # missing argument
    }
  );

  undef( $SIG{__WARN__} );

  like( $t_err_msg, qr/^ERR/, "Default 'on_error' callback" );

  return;
}

####
sub t_error_after_exec_mth1 {
  my $redis = shift;

  my $t_err_msg;
  my $t_err_code;
  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi();
      $redis->set( 'foo', 'string' );
      $redis->incr( 'foo' );
      $redis->exec(
        { on_error => sub {
            $t_err_msg  = shift;
            $t_err_code = shift;
            $t_data     = shift;

            $cv->send();
          },
        }
      );
    }
  );

  is( $t_err_msg, 'Operation \'exec\' completed with errors.',
      'error after EXEC; \'on_error\' used; error message' );
  is( $t_err_code, E_OPRN_ERROR,
      'error after EXEC; \'on_error\' used; error code' );
  is( $t_data->[0], 'OK', 'error after EXEC; \'on_error\' used; status reply' );

  isa_ok( $t_data->[1], 'AnyEvent::Redis::RipeRedis::Error',
      'error after EXEC; \'on_error\' used;' );
  can_ok( $t_data->[1], 'code' );
  can_ok( $t_data->[1], 'message' );
  like( $t_data->[1]->message(), qr/^ERR/,
      'error after EXEC; \'on_error\' used; nested error message' );
  is( $t_data->[1]->code(), E_OPRN_ERROR,
      'error after EXEC; \'on_error\' used; nested error message' );

  return;
}

####
sub t_error_after_exec_mth2 {
  my $redis = shift;

  my $t_err_msg;
  my $t_err_code;
  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi();
      $redis->set( 'foo', 'string' );
      $redis->incr( 'foo' );
      $redis->exec(
        sub {
          $t_data = shift;

          if ( defined( $_[0] ) ) {
            $t_err_msg  = shift;
            $t_err_code = shift;
          }

          $cv->send();
        },
      );
    }
  );

  is( $t_err_msg, 'Operation \'exec\' completed with errors.',
      'error after EXEC; \'on_reply\' used; error message' );
  is( $t_err_code, E_OPRN_ERROR,
      'error after EXEC; \'on_reply\' used; error code' );
  is( $t_data->[0], 'OK', 'error after EXEC; \'on_reply\' used; status reply' );

  isa_ok( $t_data->[1], 'AnyEvent::Redis::RipeRedis::Error',
      'error after EXEC; \'on_reply\' used;' );
  can_ok( $t_data->[1], 'code' );
  can_ok( $t_data->[1], 'message' );
  like( $t_data->[1]->message(), qr/^ERR/,
      'error after EXEC; \'on_reply\' used; nested error message' );
  is( $t_data->[1]->code(), E_OPRN_ERROR,
      'error after EXEC; \'on_reply\' used; nested error message' );

  return;
}

####
sub t_error_after_exec_mth3 {
  my $redis = shift;

  my $t_err_msg;
  my $t_err_code;
  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd( { keyword => 'multi' } );
      $redis->execute_cmd(
        { keyword => 'set',
          args    => [ 'foo', 'string' ],
        }
      );
      $redis->execute_cmd(
        { keyword => 'incr',
          args    => [ 'foo' ],
        }
      );
      $redis->execute_cmd(
        { keyword  => 'exec',
          on_error => sub {
            $t_err_msg  = shift;
            $t_err_code = shift;
            $t_data     = shift;

            $cv->send();
          },
        }
      );
    }
  );

  is( $t_err_msg, 'Operation \'exec\' completed with errors.',
      'error after EXEC (execute_cmd); \'on_error\' used; error message' );
  is( $t_err_code, E_OPRN_ERROR,
      'error after EXEC (execute_cmd); \'on_error\' used; error code' );
  is( $t_data->[0], 'OK',
      'error after EXEC (execute_cmd); \'on_error\' used; status reply' );

  isa_ok( $t_data->[1], 'AnyEvent::Redis::RipeRedis::Error',
      'error after EXEC (execute_cmd); \'on_error\' used;' );
  can_ok( $t_data->[1], 'code' );
  can_ok( $t_data->[1], 'message' );
  like( $t_data->[1]->message(), qr/^ERR/,
      'error after EXEC (execute_cmd); \'on_error\' used; nested error message' );
  is( $t_data->[1]->code(), E_OPRN_ERROR,
      'error after EXEC (execute_cmd); \'on_error\' used; nested error message' );

  return;
}

####
sub t_error_after_exec_mth4 {
  my $redis = shift;

  my $t_err_msg;
  my $t_err_code;
  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute_cmd( { keyword => 'multi' } );
      $redis->execute_cmd(
        { keyword => 'set',
          args    => [ 'foo', 'string' ],
        }
      );
      $redis->execute_cmd(
        { keyword => 'incr',
          args    => [ 'foo' ],
        }
      );
      $redis->execute_cmd(
        { keyword  => 'exec',
          on_reply => sub {
            $t_data = shift;

            if ( defined( $_[0] ) ) {
              $t_err_msg  = shift;
              $t_err_code = shift;
            }

            $cv->send();
          },
        }
      );
    }
  );

  is( $t_err_msg, 'Operation \'exec\' completed with errors.',
      'error after EXEC (execute_cmd); \'on_reply\' used; error message' );
  is( $t_err_code, E_OPRN_ERROR,
      'error after EXEC (execute_cmd); \'on_reply\' used; error code' );
  is( $t_data->[0], 'OK',
      'error after EXEC (execute_cmd); \'on_reply\' used; status reply' );

  isa_ok( $t_data->[1], 'AnyEvent::Redis::RipeRedis::Error',
      'error after EXEC (execute_cmd); \'on_reply\' used;' );
  can_ok( $t_data->[1], 'code' );
  can_ok( $t_data->[1], 'message' );
  like( $t_data->[1]->message(), qr/^ERR/,
      'error after EXEC (execute_cmd); \'on_reply\' used; nested error message' );
  is( $t_data->[1]->code(), E_OPRN_ERROR,
      'error after EXEC (execute_cmd); \'on_reply\' used; nested error message' );

  return;
}

####
sub t_quit {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->quit(
        { on_done => sub {
            $t_data = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data, 'OK', 'QUIT; status reply; disconnect' );
  ok( $T_IS_DISCONN, 'on_disconnect' );

  return;
}
