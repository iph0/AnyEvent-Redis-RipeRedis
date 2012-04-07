use 5.006000;
use strict;
use warnings;
use utf8;

use lib 't/tlib';
use Test::More tests => 16;
use Test::AnyEvent;
use Test::AnyEvent::RedisHandle;
use AnyEvent;

my $T_CLASS;

BEGIN {
  $T_CLASS = 'AnyEvent::Redis::RipeRedis';
  use_ok( $T_CLASS );
}

can_ok( $T_CLASS, 'new' );
can_ok( $T_CLASS, 'AUTOLOAD' );
can_ok( $T_CLASS, 'DESTROY' );

# Connect
my $t_connected;
my $cv = AnyEvent->condvar();
my $REDIS = new_ok( $T_CLASS, [
  host => 'localhost',
  port => '6379',
  password => 'test',
  connection_timeout => 5,
  encoding => 'utf8',

  on_connect => sub {
    $t_connected = 1;
    $cv->send();
  },
] );
ev_loop( $cv );
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


# Subroutines

####
sub t_ping {
  my $t_data;
  my $cv = AnyEvent->condvar();
  $REDIS->ping( {
    on_done => sub {
      $t_data = shift;
      $cv->send();
    },
  } );
  ev_loop( $cv );
  
  is( $t_data, 'PONG', 'ping (status reply)' );

  return;
}

####
sub t_incr {
  my $t_data;
  my $cv = AnyEvent->condvar();
  $REDIS->incr( 'foo', {
    on_done => sub {
      $t_data = shift;
      $cv->send();
    },
  } );
  ev_loop( $cv );
  
  is( $t_data, 1, 'incr (numeric reply)' );

  return;
}

####
sub t_set_get {
  my $t_data;
  my $cv = AnyEvent->condvar();
  $REDIS->set( 'bar', 'Some string' );
  $REDIS->get( 'bar', {
    on_done => sub {
      $t_data = shift;
      $cv->send();
    },
  } );
  ev_loop( $cv );
  
  is( $t_data, 'Some string', 'get (bulk reply)' );

  return;
}

####
sub t_set_get_utf8 {
  my $t_data;
  my $cv = AnyEvent->condvar();
  $REDIS->set( 'ключ', 'Значение' );
  $REDIS->get( 'ключ', {
    on_done => sub {
      $t_data = shift;
      $cv->send();
    },
  } );
  ev_loop( $cv );

  is( $t_data, 'Значение', 'set/get UTF-8 string' );

  return;
}

####
sub t_get_non_existent {
  my $t_data = 'not_ubdef';
  my $cv = AnyEvent->condvar();
  $REDIS->get( 'non_existent', {
    on_done => sub {
      $t_data = shift;
      $cv->send();
    },
  } );
  ev_loop( $cv );

  is( $t_data, undef, 'get (non existent key)' );

  return;
}

####
sub t_lrange {
  my $t_data;
  my $cv = AnyEvent->condvar();
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
  ev_loop( $cv );
  
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
  my $cv = AnyEvent->condvar();
  $REDIS->lrange( 'non_existent', 0, -1, {
    on_done => sub {
      $t_data = shift;
      $cv->send();
    },
  } );
  ev_loop( $cv );

  is_deeply( $t_data, [], 'lrange (empty list)' );

  return;
}

####
sub t_mbulk_undef {
  my $t_data = 'not_undef';
  my $cv = AnyEvent->condvar();
  $REDIS->brpop( 'non_existent', '5', {
    on_done => sub {
      $t_data = shift;
      $cv->send();
    },
  } );
  ev_loop( $cv );

  is( $t_data, undef, 'brpop (multi-bulk undef)' );

  return;
}

####
sub t_transaction {
  my $t_data;
  my $cv = AnyEvent->condvar();
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
  ev_loop( $cv );

  is_deeply( $t_data, [
    2,
    [ qw( 
      element_1 
      element_2
      element_3
    ) ],
    [],
    'Some string',
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
  my $cv = AnyEvent->condvar();
  $REDIS->quit( {
    on_done => sub {
      $t_data = shift;
      $cv->send();
    },
  } );
  ev_loop( $cv );
  
  is( $t_data, 'OK', 'quit (status reply)' );

  return;
}
