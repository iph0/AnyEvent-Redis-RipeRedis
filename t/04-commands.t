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
plan tests => 40;

my $REDIS;
my $T_IS_CONN = 0;
my $T_IS_DISCONN = 0;

ev_loop(
  sub {
    my $cv = shift;

    $REDIS = AnyEvent::Redis::RipeRedis->new(
      host => $SERVER_INFO->{host},
      port => $SERVER_INFO->{port},
      connection_timeout => 5,
      read_timeout => 5,
      encoding => 'utf8',
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

ok( $T_IS_CONN, 'connected' );

t_ping( $REDIS );
t_incr( $REDIS );
t_set_get( $REDIS );
t_set_get_undef( $REDIS );
t_set_get_utf8( $REDIS );
t_get_non_existent( $REDIS );
t_lrange( $REDIS );
t_get_empty_list( $REDIS );
t_mbulk_undef( $REDIS );
t_transaction( $REDIS );
t_command_error( $REDIS );
t_default_on_error( $REDIS );
t_error_after_exec( $REDIS );
t_quit( $REDIS );


####
sub t_ping {
  my $redis = shift;

  my $t_data1;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->ping(
        {
          on_done => sub {
            $t_data1 = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data1, 'PONG', 'PING; status reply; on_done' );

  my $t_data2;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->ping(
        sub {
          $t_data2 = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_data2, 'PONG', 'PING; status reply; on_reply' );

  return;
}

####
sub t_incr {
  my $redis = shift;

  my $t_data1;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->incr( 'foo',
        {
          on_done => sub {
            $t_data1 = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data1, 1, 'INCR; numeric reply; on_done' );

  my $t_data2;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->incr( 'foo',
        sub {
          $t_data2 = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_data2, 2, 'INCR; numeric reply; on_reply' );

  return;
}

####
sub t_set_get {
  my $redis = shift;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'bar', "some\r\nstring",
        {
          on_done => sub {
            $cv->send();
          },
        }
      );

      $cv->send();
    }
  );

  my $t_data1;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'bar',
        {
          on_done => sub {
            $t_data1 = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data1, "some\r\nstring", 'GET; bulk reply; on_done' );

  my $t_data2;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'bar',
        sub {
          $t_data2 = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_data2, "some\r\nstring", 'GET; bulk reply; on_reply' );

  return;
}

####
sub t_set_get_undef {
  my $redis = shift;

    ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'empty', undef,
        {
          on_done => sub {
            $cv->send();
          },
        }
      );

      $cv->send();
    }
  );

  my $t_data1;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'empty',
        {
          on_done => sub {
            $t_data1 = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data1, '', 'SET/GET undef; on_done' );

  my $t_data2;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'empty',
        sub {
          $t_data2 = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_data2, '', 'SET/GET undef; on_reply' );

  return;
}

####
sub t_set_get_utf8 {
  my $redis = shift;

    ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'ключ', 'Значение',
        {
          on_done => sub {
            $cv->send();
          },
        }
      );

      $cv->send();
    }
  );

  my $t_data1;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'ключ',
        {
          on_done => sub {
            $t_data1 = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data1, 'Значение', 'SET/GET UTF-8 string; on_done' );

  my $t_data2;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'ключ',
        sub {
          $t_data2 = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }

          $cv->send();
        }
      );
    }
  );

  is( $t_data2, 'Значение', 'SET/GET UTF-8 string; on_reply' );

  return;
}

####
sub t_get_non_existent {
  my $redis = shift;

  my $t_data1 = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'non_existent',
        {
          on_done => sub {
            $t_data1 = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data1, undef, 'GET non existent key; on_done' );

  my $t_data2 = 'not_undef';
  my $err_msg;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'non_existent',
        sub {
          $t_data2 = shift;

          if ( defined( $_[0] ) ) {
            $err_msg = shift;
            diag( $err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  ok( !defined( $t_data2 ) && !defined( $err_msg ),
      'GET non existent key; on_reply' );

  return;
}

####
sub t_lrange {
  my $redis = shift;

  ev_loop(
    sub {
      my $cv = shift;

      for ( my $i = 2; $i <= 3; $i++ ) {
        $redis->rpush( 'list', "element_$i" );
      }
      $redis->lpush( 'list', 'element_1',
        {
          on_done => sub {
            $cv->send();
          },
        }
      );
    }
  );

  my $t_data1;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->lrange( 'list', 0, -1,
        {
          on_done => sub {
            $t_data1 = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is_deeply( $t_data1, [ qw(
    element_1
    element_2
    element_3
  ) ], 'LRANGE; multi-bulk reply; on_done' );

  my $t_data2;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->lrange( 'list', 0, -1,
        sub {
          $t_data2 = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }

          $cv->send();
        }
      );
    }
  );

  is_deeply( $t_data2, [ qw(
    element_1
    element_2
    element_3
  ) ], 'LRANGE; multi-bulk reply; on_reply' );

  return;
}

####
sub t_get_empty_list {
  my $redis = shift;

  my $t_data1;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->lrange( 'non_existent', 0, -1,
        {
          on_done => sub {
            $t_data1 = shift;
            $cv->send();
          },
        }
      );
    },
  );

  is_deeply( $t_data1, [], 'LRANGE; empty list; on_done' );

  my $t_data2;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->lrange( 'non_existent', 0, -1,
        sub {
          $t_data2 = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }

          $cv->send();
        }
      );
    },
  );

  is_deeply( $t_data2, [], 'LRANGE; empty list; on_reply' );

  return;
}

####
sub t_mbulk_undef {
  my $redis = shift;

  my $t_data1 = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->brpop( 'non_existent', '1',
        {
          on_done => sub {
            $t_data1 = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data1, undef, 'BRPOP; multi-bulk undef; on_done' );

  my $t_data2 = 'not_undef';
  my $err_msg;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->brpop( 'non_existent', '1',
        sub {
          $t_data2 = shift;

          if ( defined( $_[0] ) ) {
            $err_msg = shift;
            diag( $err_msg );
          }

          $cv->send();
        }
      );
    }
  );

  ok( !defined( $t_data2 ) && !defined( $err_msg ),
      'BRPOP; multi-bulk undef; on_reply' );

  return;
}

####
sub t_transaction {
  my $redis = shift;

  my $t_data1;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi();
      $redis->incr( 'foo' );
      $redis->lrange( 'list', 0, -1 );
      $redis->lrange( 'non_existent', 0, -1 );
      $redis->get( 'bar' );
      $redis->lrange( 'list', 0, -1 );
      $redis->exec(
        {
          on_done => sub {
            $t_data1 = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is_deeply( $t_data1,
    [
      3,
      [ qw(
        element_1
        element_2
        element_3
      ) ],
      [],
      "some\r\nstring",
      [ qw(
        element_1
        element_2
        element_3
      ) ],
    ],
    'EXEC; nested multi-bulk reply; on_done'
  );

  my $t_data2;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi();
      $redis->incr( 'foo' );
      $redis->lrange( 'list', 0, -1 );
      $redis->lrange( 'non_existent', 0, -1 );
      $redis->get( 'bar' );
      $redis->lrange( 'list', 0, -1 );
      $redis->exec(
        sub {
          $t_data2 = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
          }

          $cv->send();
        },
      );
    }
  );

  is_deeply( $t_data2,
    [
      4,
      [ qw(
        element_1
        element_2
        element_3
      ) ],
      [],
      "some\r\nstring",
      [ qw(
        element_1
        element_2
        element_3
      ) ],
    ],
    'EXEC; nested multi-bulk reply; on_reply'
  );

  return;
}

####
sub t_command_error {
  my $redis = shift;

  my $t_err_msg1;
  my $t_err_code1;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set(
        {
          on_error => sub {
            $t_err_msg1 = shift;
            $t_err_code1 = shift;

            $cv->send();
          },
        }
      );
    }
  );

  like( $t_err_msg1, qr/^ERR/o, "command error; error message; on_error" );
  is( $t_err_code1, E_OPRN_ERROR, "command error; error code; on_error" );

  my $t_err_msg2;
  my $t_err_code2;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get(
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

  like( $t_err_msg2, qr/^ERR/o, "command error; error message; on_reply" );
  is( $t_err_code2, E_OPRN_ERROR, "command error; error code; on_reply" );

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

  like( $t_err_msg, qr/^ERR/o, "Default 'on_error' callback" );

  return;
}

####
sub t_error_after_exec {
  my $redis = shift;

  my $t_err_msg1;
  my $t_err_code1;
  my $t_data1;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi();
      $redis->set( 'foo', 'string' );
      $redis->incr( 'foo' );
      $redis->exec(
        {
          on_error => sub {
            $t_err_msg1 = shift;
            $t_err_code1 = shift;
            $t_data1 = shift;

            $cv->send();
          },
        }
      );
    }
  );

  is( $t_err_msg1, "Operation 'exec' completed with errors.",
      "error after EXEC; on_error; error message" );
  is( $t_err_code1, E_OPRN_ERROR, "error after EXEC; on_error; error code" );
  is( $t_data1->[0], 'OK', "error after EXEC; on_error; status reply" );
  isa_ok( $t_data1->[1], 'AnyEvent::Redis::RipeRedis::Error',
      'error after EXEC; on_error;' );
  like( $t_data1->[1]->message(), qr/^ERR/o,
      "error after EXEC; on_error; nested error message" );
  is( $t_data1->[1]->code(), E_OPRN_ERROR,
      "error after EXEC; on_error; nested error code" );


  my $t_err_msg2;
  my $t_err_code2;
  my $t_data2;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi();
      $redis->set( 'foo', 'string' );
      $redis->incr( 'foo' );
      $redis->exec(
        sub {
          $t_data2 = shift;

          if ( defined( $_[0] ) ) {
            $t_err_msg2 = shift;
            $t_err_code2 = shift;
          }

          $cv->send();
        },
      );
    }
  );

  is( $t_err_msg2, "Operation 'exec' completed with errors.",
      "error after EXEC; on_reply; error message" );
  is( $t_err_code2, E_OPRN_ERROR, "error after EXEC; on_reply; error code" );
  is( $t_data2->[0], 'OK', "error after EXEC; on_reply; status reply" );
  isa_ok( $t_data2->[1], 'AnyEvent::Redis::RipeRedis::Error',
      'error after EXEC; on_reply;' );
  like( $t_data2->[1]->message(), qr/^ERR/o,
      "error after EXEC; on_error; nested error message" );
  is( $t_data2->[1]->code(), E_OPRN_ERROR,
      "error after EXEC; on_error; nested error code" );

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
        {
          on_done => sub {
            $t_data = shift;
            $cv->send();
          },
        }
      );
    }
  );

  is( $t_data, 'OK', 'QUIT; status reply; on_done' );
  ok( $T_IS_DISCONN, 'disconnected' );

  return;
}
