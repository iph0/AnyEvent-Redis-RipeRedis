use 5.008000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
require 't/test_helper.pl';

plan tests => 16;

my $SERVER_INFO = run_redis_instance();
SKIP : {
  if ( !defined $SERVER_INFO ) {
    skip 'redis-server is required for this test', 5;
  }

  t_auto_select( $SERVER_INFO );
  t_select( $SERVER_INFO );
  t_invalid_db_index( $SERVER_INFO );
  t_auto_select_after_reconn( $SERVER_INFO );

  $SERVER_INFO->{server}->stop();
}


$SERVER_INFO = run_redis_instance(
  requirepass => 'testpass',
);
SKIP: {
  if ( !defined $SERVER_INFO ) {
    skip 'redis-server is required for this test', 1;
  }

  t_auto_select_after_auth( $SERVER_INFO );
}


####
sub t_auto_select {
  my $server_info = shift;

  my $redis_db1 = AnyEvent::Redis::RipeRedis->new(
    host     => $server_info->{host},
    port     => $server_info->{port},
    database => 1,
  );
  my $redis_db2 = AnyEvent::Redis::RipeRedis->new(
    host     => $server_info->{host},
    port     => $server_info->{port},
    database => 2,
  );

  ev_loop(
    sub {
      my $cv = shift;

      my $done_cnt = 0;
      my $on_done_cb = sub {
        if ( ++$done_cnt == 2 ) {
          $cv->send();
        }
      };
      $redis_db1->ping( { on_done => $on_done_cb } );
      $redis_db2->ping( { on_done => $on_done_cb } );
    }
  );
  my $db1_index = $redis_db1->selected_database();
  my $db2_index = $redis_db2->selected_database();

  my $t_reply = set_get( $redis_db1, $redis_db2 );

  is( $db1_index, 1, 'auto-selection of DB; first DB index' );
  is( $db2_index, 2, 'auto-selection of DB; second DB index' );
  is_deeply( $t_reply,
    { db1 => 'bar1',
      db2 => 'bar2',
    },
    'auto-selection of DB; SET and GET'
  );

  return;
}

####
sub t_select {
  my $server_info = shift;

  my $redis_db1 = AnyEvent::Redis::RipeRedis->new(
    host => $server_info->{host},
    port => $server_info->{port},
  );
  my $redis_db2 = AnyEvent::Redis::RipeRedis->new(
    host => $server_info->{host},
    port => $server_info->{port},
  );

  ev_loop(
    sub {
      my $cv = shift;

      my $done_cnt = 0;
      my $on_done_cb = sub {
        if ( ++$done_cnt == 2 ) {
          $cv->send();
        }
      };
      $redis_db1->select( 1, { on_done => $on_done_cb } );
      $redis_db2->select( 2, { on_done => $on_done_cb } );
    }
  );
  my $db1_index = $redis_db1->selected_database();
  my $db2_index = $redis_db2->selected_database();

  my $t_reply = set_get( $redis_db1, $redis_db2 );

  is( $db1_index, 1, 'SELECT; first DB index' );
  is( $db2_index, 2, 'SELECT; second DB index' );
  is_deeply( $t_reply,
    { db1 => 'bar1',
      db2 => 'bar2',
    },
    'SELECT; SET and GET'
  );

  return;
}

####
sub t_invalid_db_index {
  my $server_info = shift;

  my $redis;

  my $t_cli_err_msg;
  my $t_cli_err_code;
  my $t_cmd_err_msg;
  my $t_cmd_err_code;

  ev_loop(
    sub {
      my $cv = shift;

      $redis = AnyEvent::Redis::RipeRedis->new(
        host     => $server_info->{host},
        port     => $server_info->{port},
        database => 42,

        on_error => sub {
          $t_cli_err_msg  = shift;
          $t_cli_err_code = shift;
          $cv->send();
        },
      );

      $redis->ping(
        { on_error => sub {
            $t_cmd_err_msg = shift;
            $t_cmd_err_code = shift;
          },
        }
      );
    }
  );

  like( $t_cmd_err_msg, qr/^Operation 'ping' aborted:/,
      "invalid DB index; command error message" );
  is( $t_cmd_err_code, E_OPRN_ERROR, "invalid DB index; command error code" );
  ok( defined $t_cli_err_msg, "invalid DB index; client error message" );
  is( $t_cli_err_code, E_OPRN_ERROR, "invalid DB index; client error code" );

  return;
}

####
sub t_auto_select_after_reconn {
  my $server_info = shift;

  my $redis_db1 = AnyEvent::Redis::RipeRedis->new(
    host => $server_info->{host},
    port => $server_info->{port},
  );
  my $redis_db2 = AnyEvent::Redis::RipeRedis->new(
    host => $server_info->{host},
    port => $server_info->{port},
  );

  ev_loop(
    sub {
      my $cv = shift;

      $redis_db1->select( 1 );
      $redis_db2->select( 2 );

      $redis_db1->set( 'foo', 'bar1' );
      $redis_db2->set( 'foo', 'bar2' );

      my $done_cnt = 0;
      my $on_done_cb = sub {
        if ( ++$done_cnt == 2 ) {
          $cv->send();
        }
      };
      $redis_db1->quit( { on_done => $on_done_cb } );
      $redis_db2->quit( { on_done => $on_done_cb } );
    }
  );

  ev_loop(
    sub {
      my $cv = shift;

      my $done_cnt = 0;
      my $on_done_cb = sub {
        if ( ++$done_cnt == 2 ) {
          $cv->send();
        }
      };
      $redis_db1->ping( { on_done => $on_done_cb } );
      $redis_db2->ping( { on_done => $on_done_cb } );
    }
  );
  my $db1_index = $redis_db1->selected_database();
  my $db2_index = $redis_db2->selected_database();

  my %t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis_db1->get( 'foo',
        { on_done => sub {
            $t_reply{db1} = shift;
          },
        }
      );
      $redis_db2->get( 'foo',
        { on_done => sub {
            $t_reply{db2} = shift;
          },
        }
      );

      my $done_cnt = 0;
      my $on_done_cb = sub {
        if ( ++$done_cnt == 2 ) {
          $cv->send();
        }
      };
      $redis_db1->del( 'foo', { on_done => $on_done_cb } );
      $redis_db2->del( 'foo', { on_done => $on_done_cb } );
    }
  );

  is( $db1_index, 1, 'auto-selection of DB after reconnection; first DB index' );
  is( $db2_index, 2, 'auto-selection of DB after reconnection; second DB index' );
  is_deeply( \%t_reply,
    { db1 => 'bar1',
      db2 => 'bar2',
    },
    'auto-selection of DB after reconnection; SET and GET'
  );


  return;
}

####
sub t_auto_select_after_auth {
  my $server_info = shift;

  my $redis_db1 = AnyEvent::Redis::RipeRedis->new(
    host     => $server_info->{host},
    port     => $server_info->{port},
    password => $server_info->{password},
    database => 1,
  );
  my $redis_db2 = AnyEvent::Redis::RipeRedis->new(
    host     => $server_info->{host},
    port     => $server_info->{port},
    password => $server_info->{password},
    database => 2,
  );

  ev_loop(
    sub {
      my $cv = shift;

      my $done_cnt = 0;
      my $on_done_cb = sub {
        if ( ++$done_cnt == 2 ) {
          $cv->send();
        }
      };
      $redis_db1->ping( { on_done => $on_done_cb } );
      $redis_db2->ping( { on_done => $on_done_cb } );
    }
  );

  my $db1_index = $redis_db1->selected_database();
  my $db2_index = $redis_db2->selected_database();

  my $t_reply = set_get( $redis_db1, $redis_db2 );

  is( $db1_index, 1, 'auto-selection of DB after authentication;'
      . ' first DB index' );
  is( $db2_index, 2, 'auto-selection of DB after authentication;'
      . ' second DB index' );
  is_deeply( $t_reply,
    { db1 => 'bar1',
      db2 => 'bar2',
    },
    'auto-selection of DB after authentication; SET and GET'
  );

  return;
}

####
sub set_get {
  my $redis_db1 = shift;
  my $redis_db2 = shift;

  ev_loop(
    sub {
      my $cv = shift;

      my $done_cnt = 0;
      my $on_done = sub {
        ++$done_cnt;
        if ( $done_cnt == 2 ) {
          $cv->send();
        }
      };
      $redis_db1->set( 'foo', 'bar1', { on_done => $on_done } );
      $redis_db2->set( 'foo', 'bar2', { on_done => $on_done } );
    }
  );

  my %t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis_db1->get( 'foo',
        { on_done => sub {
            $t_reply{db1} = shift;
          },
        }
      );
      $redis_db2->get( 'foo',
        { on_done => sub {
            $t_reply{db2} = shift;
          },
        }
      );

      my $done_cnt = 0;

      my $on_done = sub {
        $done_cnt++;
        if ( $done_cnt == 2 ) {
          $cv->send();
        }
      };

      $redis_db1->del( 'foo', { on_done => $on_done } );
      $redis_db2->del( 'foo', { on_done => $on_done } );
    }
  );

  return \%t_reply;
}
