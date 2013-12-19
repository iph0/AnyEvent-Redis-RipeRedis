use 5.008000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
require 't/test_helper.pl';

my $SERVER_INFO = run_redis_instance();
if ( !defined $SERVER_INFO ) {
  plan skip_all => 'redis-server is required for this test';
}
plan tests => 6;

t_db_select( $SERVER_INFO );
t_invalid_db_index( $SERVER_INFO );

$SERVER_INFO->{server}->stop();

$SERVER_INFO = run_redis_instance(
  requirepass => 'testpass',
);

t_db_select_after_auth( $SERVER_INFO );

$SERVER_INFO->{server}->stop();


####
sub t_db_select {
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

  my $t_reply = t_set_get( $redis_db1, $redis_db2 );

  is_deeply( $t_reply,
    {
      db1 => 'bar1',
      db2 => 'bar2',
    },
    'DB select'
  );

  $redis_db1->disconnect();
  $redis_db2->disconnect();

  return;
}

####
sub t_invalid_db_index {
  my $server_info = shift;
  my $password = shift;

  my $redis;

  my $t_comm_err_msg;
  my $t_comm_err_code;
  my $t_cmd_err_msg;
  my $t_cmd_err_code;

  ev_loop(
    sub {
      my $cv = shift;

      $redis = AnyEvent::Redis::RipeRedis->new(
        host     => $server_info->{host},
        port     => $server_info->{port},
        password => $password,
        database => 42,

        on_error => sub {
          $t_comm_err_msg  = shift;
          $t_comm_err_code = shift;
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

  $redis->disconnect();

  my $t_name = 'invalid DB index';
  like( $t_cmd_err_msg, qr/^Operation 'ping' aborted:/,
      "$t_name; command error message" );
  is( $t_cmd_err_code, E_OPRN_ERROR, "$t_name; command error code" );
  ok( defined $t_comm_err_msg, "$t_name; common error message" );
  is( $t_comm_err_code, E_OPRN_ERROR, "$t_name common error code" );

  return;
}

####
sub t_db_select_after_auth {
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

  my $t_data = t_set_get( $redis_db1, $redis_db2 );

  is_deeply( $t_data,
    { db1 => 'bar1',
      db2 => 'bar2',
    },
    'DB select (after authentication)'
  );

  $redis_db1->disconnect();
  $redis_db2->disconnect();

  return;
}

####
sub t_set_get {
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

  my %t_data;

  ev_loop(
    sub {
      my $cv = shift;

      my $done_cnt = 0;

      my $on_done = sub {
        my $key = shift;
        my $val = shift;

        $t_data{$key} = $val;
        ++$done_cnt;
        if ( $done_cnt == 2 ) {
          $cv->send();
        }
      };

      $redis_db1->get( 'foo',
        { on_done => sub {
            my $val = shift;
            $on_done->( 'db1', $val );
          },
        }
      );
      $redis_db2->get( 'foo',
        { on_done => sub {
            my $val = shift;
            $on_done->( 'db2', $val );
          },
        }
      );
    }
  );

  return \%t_data;
}
