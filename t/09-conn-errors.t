use 5.008000;
use strict;
use warnings;

use Test::More tests => 14;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
use Scalar::Util qw( weaken );
require 't/test_helper.pl';

t_no_connection();
t_reconnection();
t_read_timeout();


####
sub t_no_connection {
  my $redis;
  my $port = get_tcp_port();

  my $t_comm_err_msg;
  my $t_first_cmd_err_msg;
  my $t_first_cmd_err_code;

  ev_loop(
    sub {
      my $cv = shift;

      $redis = AnyEvent::Redis::RipeRedis->new(
        port => $port,
        connection_timeout => 1,
        reconnect => 0,
        on_connect_error => sub {
          $t_comm_err_msg = shift;
          $cv->send();
        },
      );

      $redis->ping( {
        on_error => sub {
          $t_first_cmd_err_msg = shift;
          $t_first_cmd_err_code = shift;
        }
      } );
    },
  );

  my $t_name = 'no connection';

  like( $t_first_cmd_err_msg,
      qr/^Command 'ping' aborted: Can't connect to localhost:$port:/o,
      "$t_name; first command error message" );
  is( $t_first_cmd_err_code, E_CANT_CONN, "$t_name; first command error code" );
  like( $t_comm_err_msg, qr/^Can't connect to localhost:$port:/o,
      "$t_name; common error message" );

  my $t_second_cmd_err_msg;
  my $t_second_cmd_err_code;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->ping( {
        on_error => sub {
          $t_second_cmd_err_msg = shift;
          $t_second_cmd_err_code = shift;
          $cv->send();
        }
      } );
    }
  );

  is( $t_second_cmd_err_msg, "Can't handle the command 'ping'."
      . ' No connection to the server.',
      "$t_name; second command error message" );
  is( $t_second_cmd_err_code, E_NO_CONN, "$t_name; second command error code" );

  return;
}

####
sub t_reconnection {
  SKIP : {
    my $port = get_tcp_port();
    my $server_info = run_redis_instance(
      port => $port,
    );
    if ( !defined( $server_info ) ) {
      skip 'redis-server is required to this test', 5;
    }

    my $t_conn_cnt = 0;
    my $t_disconn_cnt = 0;
    my $t_comm_err_msg;
    my $t_comm_err_code;
    my $redis;

    ev_loop(
      sub {
        my $cv = shift;

        $redis = AnyEvent::Redis::RipeRedis->new(
          host => $server_info->{host},
          port => $server_info->{port},
          on_connect => sub {
            $t_conn_cnt++;
          },
          on_disconnect => sub {
            $t_disconn_cnt++;
            $cv->send();
          },
          on_error => sub {
            $t_comm_err_msg = shift;
            $t_comm_err_code = shift;
          },
        );

        $redis->ping( {
          on_done => sub {
            my $timer;
            $timer = AE::timer( 1, 0,
              sub {
                $server_info->{server}->stop();
                undef( $timer );
              }
            );
          }
        } );
      }
    );

    $server_info = run_redis_instance(
      port => $port,
    );

    my $t_pong;

    ev_loop(
      sub {
        my $cv = shift;

        $redis->ping( {
          on_done => sub {
            $t_pong = shift;
            $cv->send();
          }
        } );
      }
    );

    my $t_name = 'reconnection';
    is( $t_conn_cnt, 2, "$t_name; connections" );
    is( $t_disconn_cnt, 1, "$t_name; disconnections" );
    is( $t_comm_err_msg, 'Connection closed by remote host.',
        "$t_name; error message" );
    is( $t_comm_err_code, E_CONN_CLOSED_BY_REMOTE_HOST, "$t_name; error code" );
    is( $t_pong, 'PONG', "$t_name; success PING" );

    $redis->disconnect();
  }

  return;
}

####
sub t_read_timeout {
  SKIP: {
    my $server_info = run_redis_instance();
    if ( !defined( $server_info ) ) {
      skip 'redis-server is required to this test', 4;
    }

    my $redis;

    my $t_comm_err_msg;
    my $t_comm_err_code;
    my $t_cmd_err_msg;
    my $t_cmd_err_code;

    ev_loop(
      sub {
        my $cv = shift;

        $redis = AnyEvent::Redis::RipeRedis->new(
          host => $server_info->{host},
          port => $server_info->{port},
          reconnect => 0,
          read_timeout => 1,
          on_error => sub {
            $t_comm_err_msg = shift;
            $t_comm_err_code = shift;
            $cv->send();
          },
        );

        $redis->brpop( 'non_existent', '3', {
          on_error => sub {
            $t_cmd_err_msg = shift;
            $t_cmd_err_code = shift;
            $cv->send();
          },
        } );
      }
    );

    $redis->disconnect();

    my $t_name = 'read timeout';
    is( $t_cmd_err_msg, "Command 'brpop' aborted: Read timed out.",
        "$t_name; command error message" );
    is( $t_cmd_err_code, E_READ_TIMEDOUT, "$t_name; command error code" );
    is( $t_comm_err_msg, 'Read timed out.', "$t_name; common error message" );
    is( $t_comm_err_code, E_READ_TIMEDOUT, "$t_name; common error code" );
  }

  return;
}
