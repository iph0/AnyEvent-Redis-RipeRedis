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
plan tests => 48;

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

t_status_reply_mth1( $REDIS );
t_status_reply_mth2( $REDIS );

t_numeric_reply_mth1( $REDIS );
t_numeric_reply_mth2( $REDIS );

t_bulk_reply_mth1( $REDIS );
t_bulk_reply_mth2( $REDIS );

t_set_undef_mth1( $REDIS );
t_set_undef_mth2( $REDIS );

t_get_undef_mth1( $REDIS );
t_get_undef_mth2( $REDIS );

t_set_utf8_string_mth1( $REDIS );
t_set_utf8_string_mth2( $REDIS );

t_get_utf8_string_mth1( $REDIS );
t_get_utf8_string_mth2( $REDIS );

t_get_non_existent_mth1( $REDIS );
t_get_non_existent_mth2( $REDIS );

t_mbulk_reply_mth1( $REDIS );
t_mbulk_reply_mth2( $REDIS );

t_mbulk_reply_empty_list_mth1( $REDIS );
t_mbulk_reply_empty_list_mth2( $REDIS );

t_mbulk_reply_undef_mth1( $REDIS );
t_mbulk_reply_undef_mth2( $REDIS );

t_nested_mbulk_reply_mth1( $REDIS );
t_nested_mbulk_reply_mth2( $REDIS );

t_oprn_error_mth1( $REDIS );
t_oprn_error_mth2( $REDIS );

t_default_on_error( $REDIS );

t_error_after_exec_mth1( $REDIS );
t_error_after_exec_mth2( $REDIS );

t_quit( $REDIS );


####
sub t_status_reply_mth1 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'foo', "some\r\nstring",
        { on_done => sub {
            $t_reply = shift;
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

  is( $t_reply, 'OK', 'SET; \'on_done\' used; status reply' );

  return;
}

####
sub t_status_reply_mth2 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'foo', "some\r\nstring",
        sub {
          $t_reply    = shift;
          my $err_msg = shift;

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }
        }
      );

      $redis->del( 'foo',
        sub {
          my $err_msg = $_[1];

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_reply, 'OK', 'SET; \'on_reply\' used; status reply' );

  return;
}

####
sub t_numeric_reply_mth1 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->incr( 'bar',
        { on_done => sub {
            $t_reply = shift;
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

  is( $t_reply, 1, 'INCR; \'on_done\' used; numeric reply' );

  return;
}

####
sub t_numeric_reply_mth2 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->incr( 'bar',
        sub {
          $t_reply    = shift;
          my $err_msg = $_[1];

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }
        }
      );

      $redis->del( 'bar',
        sub {
          my $err_msg = $_[1];

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_reply, 1, 'INCR; \'on_reply\' used; numeric reply' );

  return;
}

####
sub t_bulk_reply_mth1 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'foo', "some\r\nstring" );

      $redis->get( 'foo',
        { on_done => sub {
            $t_reply = shift;
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

  is( $t_reply, "some\r\nstring", 'GET; \'on_done\' used; bulk reply' );

  return;
}

####
sub t_bulk_reply_mth2 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'foo', "some\r\nstring" );

      $redis->get( 'foo',
        sub {
          $t_reply    = shift;
          my $err_msg = shift;

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }
        }
      );

      $redis->del( 'foo',
        sub {
          my $err_msg = $_[1];

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_reply, "some\r\nstring", 'GET; \'on_reply\' used; bulk reply' );

  return;
}

####
sub t_set_undef_mth1 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'empty', undef,
        { on_done => sub {
            $t_reply = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_reply, 'OK', 'SET; \'on_done\' used; undef' );

  return;
}

####
sub t_set_undef_mth2 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'empty', undef,
        sub {
          $t_reply    = shift;
          my $err_msg = shift;

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }

          $cv->send();
        },
      );
    }
  );

  is( $t_reply, 'OK', 'SET; \'on_reply\' used; undef' );

  return;
}

####
sub t_get_undef_mth1 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'empty',
        { on_done => sub {
            $t_reply = shift;

            $cv->send();
          },
        }
      );
    }
  );

  is( $t_reply, '', 'GET; \'on_done\' used; undef' );

  return;
}

####
sub t_get_undef_mth2 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'empty',
        sub {
          $t_reply    = shift;
          my $err_msg = shift;

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_reply, '', 'GET; \'on_reply\' used; undef' );

  return;
}

####
sub t_set_utf8_string_mth1 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'ключ', 'Значение',
        { on_done => sub {
            $t_reply = shift;
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

  is( $t_reply, 'OK', 'SET; \'on_done\' used; UTF-8 string' );

  return;
}

####
sub t_set_utf8_string_mth2 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'ключ', 'Значение',
        sub {
          $t_reply    = shift;
          my $err_msg = shift;

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }
        },
      );

      $redis->del( 'ключ',
        sub {
          my $err_msg = $_[1];

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_reply, 'OK', 'SET; \'on_reply\' used; UTF-8 string' );

  return;
}

####
sub t_get_utf8_string_mth1 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'ключ', 'Значение' );

      $redis->get( 'ключ',
        { on_done => sub {
            $t_reply = shift;
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

  is( $t_reply, 'Значение', 'GET; \'on_done\' used; UTF-8 string' );

  return;
}

####
sub t_get_utf8_string_mth2 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'ключ', 'Значение' );

      $redis->get( 'ключ',
        sub {
          $t_reply    = shift;
          my $err_msg = shift;

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }
        }
      );

      $redis->del( 'ключ',
        sub {
          my $err_msg = $_[1];

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_reply, 'Значение', 'GET; \'on_reply\' used; UTF-8 string' );

  return;
}

####
sub t_get_non_existent_mth1 {
  my $redis = shift;

  my $t_reply = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'non_existent',
        { on_done => sub {
            $t_reply = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_reply, undef, 'GET; \'on_done\' used; non existent key' );

  return;
}

####
sub t_get_non_existent_mth2 {
  my $redis = shift;

  my $t_reply   = 'not_undef';
  my $t_err_msg = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'non_existent',
        sub {
          $t_reply   = shift;
          $t_err_msg = shift;

          if ( defined( $t_err_msg ) ) {
            diag( $t_err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  ok( !defined( $t_reply ) && !defined( $t_err_msg ),
      'GET; \'on_reply\' used; non existent key' );

  return;
}

####
sub t_mbulk_reply_mth1 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      for ( my $i = 1; $i <= 3; $i++ ) {
        $redis->rpush( 'list', "element_$i" );
      }

      $redis->lrange( 'list', 0, -1,
        { on_done => sub {
            $t_reply = shift;
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

  is_deeply( $t_reply,
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

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      for ( my $i = 1; $i <= 3; $i++ ) {
        $redis->rpush( 'list', "element_$i" );
      }

      $redis->lrange( 'list', 0, -1,
        sub {
          $t_reply    = shift;
          my $err_msg = shift;

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }
        }
      );

      $redis->del( 'list',
        sub {
          my $err_msg = $_[1];

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  is_deeply( $t_reply,
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
sub t_mbulk_reply_empty_list_mth1 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->lrange( 'non_existent', 0, -1,
        { on_done => sub {
            $t_reply = shift;
            $cv->send();
          },
        }
      );
    },
  );

  is_deeply( $t_reply, [], 'LRANGE; \'on_done\' used; empty list' );

  return;
}

####
sub t_mbulk_reply_empty_list_mth2 {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->lrange( 'non_existent', 0, -1,
        sub {
          $t_reply    = shift;
          my $err_msg = shift;

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }

          $cv->send();
        }
      );
    },
  );

  is_deeply( $t_reply, [], 'LRANGE; \'on_reply\' used; empty list' );

  return;
}

####
sub t_mbulk_reply_undef_mth1 {
  my $redis = shift;

  my $t_reply = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->brpop( 'non_existent', '1',
        { on_done => sub {
            $t_reply = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_reply, undef, 'BRPOP; \'on_done\' used; multi-bulk undef' );

  return;
}

####
sub t_mbulk_reply_undef_mth2 {
  my $redis = shift;

  my $t_reply   = 'not_undef';
  my $t_err_msg = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->brpop( 'non_existent', '1',
        sub {
          $t_reply   = shift;
          $t_err_msg = shift;

          if ( defined( $t_err_msg ) ) {
            diag( $t_err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  ok( !defined( $t_reply ) && !defined( $t_err_msg ),
      'BRPOP; \'on_reply\' used; multi-bulk undef' );

  return;
}

####
sub t_nested_mbulk_reply_mth1 {
  my $redis = shift;

  my $t_reply;

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
            $t_reply = shift;
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

  is_deeply( $t_reply,
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

  my $t_reply;

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
          $t_reply    = shift;
          my $err_msg = shift;

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }
        },
      );

      $redis->del( qw( foo list bar ),
        sub {
          my $err_msg = $_[1];

          if ( defined( $err_msg ) ) {
            diag( $err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  is_deeply( $t_reply,
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

  ok( defined( $t_err_msg ), 'operation error; \'on_error\' used; error message' );
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
          my $reply  = shift;
          $t_err_msg = shift;

          if ( defined( $t_err_msg ) ) {
            $t_err_code = shift;
          }

          $cv->send();
        }
      );
    }
  );

  ok( defined( $t_err_msg ), 'operation error; \'on_reply\' used; error message' );
  is( $t_err_code, E_OPRN_ERROR, 'operation error; \'on_reply\' used; error code' );

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

  ok( defined( $t_err_msg ), "Default 'on_error' callback" );

  return;
}

####
sub t_error_after_exec_mth1 {
  my $redis = shift;

  my $t_err_msg;
  my $t_err_code;
  my $t_reply;

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
            $t_reply    = shift;

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
  is( $t_reply->[0], 'OK', 'error after EXEC; \'on_error\' used; status reply' );

  isa_ok( $t_reply->[1], 'AnyEvent::Redis::RipeRedis::Error',
      'error after EXEC; \'on_error\' used;' );
  can_ok( $t_reply->[1], 'code' );
  can_ok( $t_reply->[1], 'message' );
  ok( defined( $t_reply->[1]->message() ),
      'error after EXEC; \'on_error\' used; nested error message' );
  is( $t_reply->[1]->code(), E_OPRN_ERROR,
      'error after EXEC; \'on_error\' used; nested error message' );

  return;
}

####
sub t_error_after_exec_mth2 {
  my $redis = shift;

  my $t_err_msg;
  my $t_err_code;
  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi();
      $redis->set( 'foo', 'string' );
      $redis->incr( 'foo' );
      $redis->exec(
        sub {
          $t_reply   = shift;
          $t_err_msg = shift;

          if ( defined( $t_err_msg ) ) {
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
  is( $t_reply->[0], 'OK', 'error after EXEC; \'on_reply\' used; status reply' );

  isa_ok( $t_reply->[1], 'AnyEvent::Redis::RipeRedis::Error',
      'error after EXEC; \'on_reply\' used;' );
  can_ok( $t_reply->[1], 'code' );
  can_ok( $t_reply->[1], 'message' );
  ok( defined( $t_reply->[1]->message() ),
      'error after EXEC; \'on_reply\' used; nested error message' );
  is( $t_reply->[1]->code(), E_OPRN_ERROR,
      'error after EXEC; \'on_reply\' used; nested error message' );

  return;
}

####
sub t_quit {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->quit(
        { on_done => sub {
            $t_reply = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_reply, 'OK', 'QUIT; status reply; disconnect' );
  ok( $T_IS_DISCONN, 'on_disconnect' );

  return;
}
