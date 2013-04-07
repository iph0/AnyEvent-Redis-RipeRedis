use 5.006000;
use strict;
use warnings;
use utf8;

use Test::More;
use AnyEvent::Redis::RipeRedis;
require 't/test_helper.pl';

my $server_info = run_redis_instance();

plan tests => 12;

my $redis;
my $t_is_conn = 0;
my $t_is_disconn = 0;

ev_loop(
  sub {
    my $cv = shift;

    $redis = AnyEvent::Redis::RipeRedis->new(
      host => $server_info->{host},
      port => $server_info->{port},
      connection_timeout => 5,
      read_timeout => 5,
      encoding => 'utf8',

      on_connect => sub {
        $t_is_conn = 1;
        $cv->send();
      },

      on_disconnect => sub {
        $t_is_disconn = 1;
      },
    );
  },
);

ok( $t_is_conn, 'Connected' );

t_ping( $redis );
t_incr( $redis );
t_set_get( $redis );
t_set_get_utf8( $redis );
t_get_non_existent( $redis );
t_lrange( $redis );
t_get_empty_list( $redis );
t_mbulk_undef( $redis );
t_transaction( $redis );
t_quit( $redis );


####
sub t_ping {
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->ping( {
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
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->incr( 'foo', {
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
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'bar', "Some\r\nstring" );
      $redis->get( 'bar', {
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
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'ключ', 'Значение' );
      $redis->get( 'ключ', {
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
  my $redis = shift;

  my $t_data = 'not_ubdef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'non_existent', {
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
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      for ( my $i = 2; $i <= 3; $i++ ) {
        $redis->rpush( 'list', "element_$i" );
      }
      $redis->lpush( 'list', 'element_1' );
      $redis->lrange( 'list', 0, -1, {
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
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->lrange( 'non_existent', 0, -1, {
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
  my $redis = shift;

  my $t_data = 'not_undef';

  ev_loop(
    sub {
      my $cv = shift;

      $redis->brpop( 'non_existent', '1', {
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
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi();
      $redis->incr( 'foo' );
      $redis->lrange( 'list', 0, -1 );
      $redis->lrange( 'non_existent', 0, -1 );
      $redis->get( 'bar' );
      $redis->lrange( 'list', 0, -1 );
      $redis->exec( {
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
  my $redis = shift;

  my $t_data;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->quit( {
        on_done => sub {
          $t_data = shift;
          $cv->send();
        },
      } );
    },
  );

  is( $t_data, 'OK', 'quit (status reply)' );
  ok( $t_is_disconn, 'Disconnected' );

  return;
}
