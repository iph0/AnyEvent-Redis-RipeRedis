use 5.006000;
use strict;
use warnings;
use utf8;

use lib 't/tlib';
use Test::More tests => 30;
use Test::AnyEvent::RedisHandle;
use Test::AnyEvent::EVLoop;

my $T_CLASS;

BEGIN {
  $T_CLASS = 'AnyEvent::Redis::RipeRedis';
  use_ok( $T_CLASS, qw( :err_codes ) );
}

can_ok( $T_CLASS, 'new' );
can_ok( $T_CLASS, 'disconnect' );
can_ok( $T_CLASS, 'AUTOLOAD' );
can_ok( $T_CLASS, 'DESTROY' );

# Connect
my $REDIS;
my $t_connected = 0;
my $t_disconnected = 0;

ev_loop(
  sub {
    my $cv = shift;

    $REDIS = new_ok( $T_CLASS, [
      host => 'localhost',
      port => '6379',
      password => 'test',
      database => 1,
      connection_timeout => 5,
      encoding => 'utf8',

      on_connect => sub {
        $t_connected = 1;
        $cv->send();
      },

      on_disconnect => sub {
        $t_disconnected = 1;
      },
    ] );
  },
);

ok( $t_connected, 'Connected' );

t_ping();
t_incr();
t_set_get();
t_set_get_utf8();
t_get_non_existent();
t_lrange();
t_get_empty_list();
t_mbulk_undef();
t_transaction();
t_quit();
t_error_codes();


# Subroutines

####
sub t_ping {
  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $REDIS->ping( {
        on_done => sub {
          $t_data = shift;
          $cv->send();
        },
      } );
    },
  );

  is( $t_data, 'PONG', 'ping (status reply)' );

  return;
}

####
sub t_incr {
  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $REDIS->incr( 'foo', {
        on_done => sub {
          $t_data = shift;
          $cv->send();
        },
      } );
    },
  );

  is( $t_data, 1, 'incr (numeric reply)' );

  return;
}

####
sub t_set_get {
  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $REDIS->set( 'bar', "Some\r\nstring" );
      $REDIS->get( 'bar', {
        on_done => sub {
          $t_data = shift;
          $cv->send();
        },
      } );
    },
  );

  is( $t_data, "Some\r\nstring", 'get (bulk reply)' );

  return;
}

####
sub t_set_get_utf8 {
  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $REDIS->set( 'ключ', 'Значение' );
      $REDIS->get( 'ключ', {
        on_done => sub {
          $t_data = shift;
          $cv->send();
        },
      } );
    },
  );

  is( $t_data, 'Значение', 'set/get UTF-8 string' );

  return;
}

####
sub t_get_non_existent {
  my $t_data = 'not_ubdef';

  ev_loop(
    sub {
      my $cv = shift;

      $REDIS->get( 'non_existent', {
        on_done => sub {
          $t_data = shift;
          $cv->send();
        },
      } );
    },
  );

  is( $t_data, undef, 'get (non existent key)' );

  return;
}

####
sub t_lrange {
  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      for ( my $i = 2; $i <= 3; $i++ ) {
        $REDIS->rpush( 'list', "element_$i" );
      }
      $REDIS->lpush( 'list', 'element_1' );
      $REDIS->lrange( 'list', 0, -1, {
        on_done => sub {
          $t_data = shift;
          $cv->send();
        },
      } );
    },
  );

  is_deeply( $t_data, [ qw(
    element_1
    element_2
    element_3
  ) ], 'lrange (multi-bulk reply)' );

  return;
}

####
sub t_get_empty_list {
  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $REDIS->lrange( 'non_existent', 0, -1, {
        on_done => sub {
          $t_data = shift;
          $cv->send();
        },
      } );
    },
  );

  is_deeply( $t_data, [], 'lrange (empty list)' );

  return;
}

####
sub t_mbulk_undef {
  my $t_data = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $REDIS->brpop( 'non_existent', '5', {
        on_done => sub {
          $t_data = shift;
          $cv->send();
        },
      } );
    },
  );

  is( $t_data, undef, 'brpop (multi-bulk undef)' );

  return;
}

####
sub t_transaction {
  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $REDIS->multi();
      $REDIS->incr( 'foo' );
      $REDIS->lrange( 'list', 0, -1 );
      $REDIS->lrange( 'non_existent', 0, -1 );
      $REDIS->get( 'bar' );
      $REDIS->lrange( 'list', 0, -1 );
      $REDIS->exec( {
        on_done => sub {
          $t_data = shift;
          $cv->send();
        },
      } );
    },
  );

  is_deeply( $t_data, [
    2,
    [ qw(
      element_1
      element_2
      element_3
    ) ],
    [],
    "Some\r\nstring",
    [ qw(
      element_1
      element_2
      element_3
    ) ],
  ], 'exec (nested multi-bulk reply)' );

  return;
}

####
sub t_quit {
  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $REDIS->quit( {
        on_done => sub {
          $t_data = shift;
          $cv->send();
        },
      } );
    },
  );

  is( $t_data, 'OK', 'quit (status reply)' );
  ok( $t_disconnected, 'Disconnected' );

  return;
}

####
sub t_error_codes {
  is( E_CANT_CONN, 1, 'Constant E_CANT_CONN' );
  is( E_LOADING_DATASET, 2, 'Constant E_LOADING_DATASET' );
  is( E_IO, 3, 'Constant E_IO' );
  is( E_CONN_CLOSED_BY_REMOTE_HOST, 4, 'Constant E_CONN_CLOSED_BY_REMOTE_HOST' );
  is( E_CONN_CLOSED_BY_CLIENT, 5, 'Constant E_CONN_CLOSED_BY_CLIENT' );
  is( E_NO_CONN, 6, 'Constant E_NO_CONN' );
  is( E_INVALID_PASS, 7, 'Constant E_INVALID_PASS' );
  is( E_OPRN_NOT_PERMITTED, 8, 'Constant E_OPRN_NOT_PERMITTED' );
  is( E_OPRN_ERROR, 9, 'Constant E_OPRN_ERROR' );
  is( E_UNEXPECTED_DATA, 10, 'Constant E_UNEXPECTED_DATA' );
  is( E_NO_SCRIPT, 11, 'Constant E_NO_SCRIPT' );
  is( E_READ_TIMEDOUT, 12, 'Constant E_RESP_TIMEDOUT' );
}
