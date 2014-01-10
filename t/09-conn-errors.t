use 5.008000;
use strict;
use warnings;

use Test::More tests => 27;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
use Net::EmptyPort qw( empty_port );
use Scalar::Util qw( weaken );
require 't/test_helper.pl';

t_cant_connect_mth1();
t_cant_connect_mth2();

t_no_connection();
t_reconnection();
t_read_timeout();

t_premature_conn_close_mth1();
t_premature_conn_close_mth2();

####
sub t_cant_connect_mth1 {
  my $redis;
  my $port = empty_port();

  my $t_cli_err_msg;
  my $t_cmd_err_msg;
  my $t_cmd_err_code;

  AE::now_update();
  ev_loop(
    sub {
      my $cv = shift;

      $redis = AnyEvent::Redis::RipeRedis->new(
        port               => $port,
        connection_timeout => 3,
        reconnect          => 0,

        on_connect_error => sub {
          $t_cli_err_msg = shift;

          $cv->send();
        },
      );

      $redis->ping(
        { on_error => sub {
            $t_cmd_err_msg  = shift;
            $t_cmd_err_code = shift;
          },
        }
      );
    },
    0
  );

  like( $t_cmd_err_msg,
      qr/^Operation 'ping' aborted: Can't connect to localhost:$port:/,
      'can\'t connect; \'on_connect_error\' used; command error message' );
  is( $t_cmd_err_code, E_CANT_CONN,
      'can\'t connect; \'on_connect_error\' used; command error code' );
  like( $t_cli_err_msg, qr/^Can't connect to localhost:$port:/,
      'can\'t connect; \'on_connect_error\' used; client error message' );

  return;
}

####
sub t_cant_connect_mth2 {
  my $redis;
  my $port = empty_port();

  my $t_cli_err_msg;
  my $t_cli_err_code;
  my $t_cmd_err_msg;
  my $t_cmd_err_code;

  AE::now_update();
  ev_loop(
    sub {
      my $cv = shift;

      $redis = AnyEvent::Redis::RipeRedis->new(
        port               => $port,
        connection_timeout => 3,
        reconnect          => 0,

        on_error => sub {
          $t_cli_err_msg  = shift;
          $t_cli_err_code = shift;

          $cv->send();
        },
      );

      $redis->ping(
        { on_error => sub {
            $t_cmd_err_msg  = shift;
            $t_cmd_err_code = shift;
          },
        }
      );
    },
    0
  );

  like( $t_cmd_err_msg,
      qr/^Operation 'ping' aborted: Can't connect to localhost:$port:/,
      'can\'t connect; \'on_error\' used; command error message' );
  is( $t_cmd_err_code, E_CANT_CONN,
      'can\'t connect; \'on_error\' used; command error code' );
  like( $t_cli_err_msg, qr/^Can't connect to localhost:$port:/,
      'can\'t connect; \'on_error\' used; client error message' );
  is( $t_cli_err_code, E_CANT_CONN,
      'can\'t connect; \'on_error\' used; client error code' );

  return;
}

####
sub t_no_connection {
  my $redis;
  my $port = empty_port();

  my $t_cli_err_msg;
  my $t_cmd_err_msg_0;
  my $t_cmd_err_code_0;

  AE::now_update();
  ev_loop(
    sub {
      my $cv = shift;

      $redis = AnyEvent::Redis::RipeRedis->new(
        port               => $port,
        connection_timeout => 3,
        reconnect          => 0,

        on_connect_error => sub {
          $t_cli_err_msg = shift;

          $cv->send();
        },
      );

      $redis->ping(
        { on_error => sub {
            $t_cmd_err_msg_0  = shift;
            $t_cmd_err_code_0 = shift;
          },
        }
      );
    },
    0
  );

  my $t_name = 'no connection';

  like( $t_cmd_err_msg_0,
      qr/^Operation 'ping' aborted: Can't connect to localhost:$port:/,
      "$t_name; first command error message" );
  is( $t_cmd_err_code_0, E_CANT_CONN, "$t_name; first command error code" );
  like( $t_cli_err_msg, qr/^Can't connect to localhost:$port:/,
      "$t_name; client error message" );

  my $t_cmd_err_msg_1;
  my $t_cmd_err_code_1;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->ping(
        { on_error => sub {
            $t_cmd_err_msg_1 = shift;
            $t_cmd_err_code_1 = shift;

            $cv->send();
          },
        }
      );
    }
  );

  is( $t_cmd_err_msg_1,
      'Can\'t handle the command \'ping\'. No connection to the server.',
      "$t_name; second command error message" );
  is( $t_cmd_err_code_1, E_NO_CONN, "$t_name; second command error code" );

  return;
}

####
sub t_reconnection {
  my $port = empty_port();
  my $server_info = run_redis_instance(
    port => $port,
  );

  SKIP: {
    if ( !defined $server_info ) {
      skip 'redis-server is required for this test', 5;
    }

    my $t_conn_cnt = 0;
    my $t_disconn_cnt = 0;
    my $t_cli_err_msg;
    my $t_cli_err_code;
    my $redis;

    AE::now_update();
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
            $t_cli_err_msg  = shift;
            $t_cli_err_code = shift;
          },
        );

        $redis->ping(
          { on_done => sub {
              my $timer;
              $timer = AE::postpone(
                sub {
                  undef $timer;
                  $server_info->{server}->stop();
                }
              );
            },
          }
        );
      }
    );

    $server_info = run_redis_instance(
      port => $port,
    );

    my $t_pong;

    ev_loop(
      sub {
        my $cv = shift;

        $redis->ping(
          {
            on_done => sub {
              $t_pong = shift;
              $cv->send();
            },
          }
        );
      }
    );

    is( $t_conn_cnt, 2, 'reconnection; connections' );
    is( $t_disconn_cnt, 1, 'reconnection; disconnections' );
    is( $t_cli_err_msg, 'Connection closed by remote host.',
        'reconnection; error message' );
    is( $t_cli_err_code, E_CONN_CLOSED_BY_REMOTE_HOST,
        'reconnection; error code' );
    is( $t_pong, 'PONG', 'reconnection; success PING' );

    $redis->disconnect();
  }

  return;
}

####
sub t_read_timeout {
  my $server_info = run_redis_instance();

  SKIP: {
    if ( !defined $server_info ) {
      skip 'redis-server is required for this test', 4;
    }

    my $redis;

    my $t_cli_err_msg;
    my $t_cli_err_code;
    my $t_cmd_err_msg;
    my $t_cmd_err_code;

    ev_loop(
      sub {
        my $cv = shift;

        $redis = AnyEvent::Redis::RipeRedis->new(
          host         => $server_info->{host},
          port         => $server_info->{port},
          reconnect    => 0,
          read_timeout => 1,

          on_error => sub {
            $t_cli_err_msg  = shift;
            $t_cli_err_code = shift;
            $cv->send();
          },
        );

        $redis->brpop( 'non_existent', '3',
          { on_error => sub {
              $t_cmd_err_msg  = shift;
              $t_cmd_err_code = shift;

              $cv->send();
            },
          }
        );
      }
    );

    $redis->disconnect();

    is( $t_cmd_err_msg, 'Operation \'brpop\' aborted: Read timed out.',
        'read timeout; command error message' );
    is( $t_cmd_err_code, E_READ_TIMEDOUT, 'read timeout; command error code' );
    is( $t_cli_err_msg, 'Read timed out.', 'read timeout; client error message' );
    is( $t_cli_err_code, E_READ_TIMEDOUT, 'read timeout; client error code' );
  }

  return;
}

####
sub t_premature_conn_close_mth1 {
  my $t_cli_err_msg;
  my $t_cli_err_code;
  my $t_cmd_err_msg;
  my $t_cmd_err_code;

  my $redis = AnyEvent::Redis::RipeRedis->new(
    on_error => sub {
      $t_cli_err_msg = shift;
      $t_cli_err_code = shift;
    },
  );

  $redis->ping(
    { on_error => sub {
        $t_cmd_err_msg = shift;
        $t_cmd_err_code = shift;
      },
    }
  );

  $redis->disconnect();

  is( $t_cli_err_msg, 'Connection closed by client prematurely.',
      'premature connection close; disconnect() used; client error message' );
  is( $t_cli_err_code, E_CONN_CLOSED_BY_CLIENT,
      'premature connection close; disconnect() used; client error message' );
  is( $t_cmd_err_msg,
      'Operation \'ping\' aborted: Connection closed by client prematurely.',
      'premature connection close; disconnect() used; command error message' );
  is( $t_cmd_err_code, E_CONN_CLOSED_BY_CLIENT,
      'premature connection close; disconnect() used; command error message' );

  return;
}

####
sub t_premature_conn_close_mth2 {
  my $on_error_was_called = 0;
  my $t_cmd_err_msg;

  local $SIG{__WARN__} = sub {
    $t_cmd_err_msg = shift;

    chomp( $t_cmd_err_msg );
  };

  my $redis = AnyEvent::Redis::RipeRedis->new();

  $redis->ping(
    { on_error => sub {
        $on_error_was_called = 1;
      },
    }
  );

  undef $redis;

  ok( !$on_error_was_called, 'premature connection close; undef() used;'
      . ' \'on_error\' callback ignored' );
  is( $t_cmd_err_msg,
      'Operation \'ping\' aborted: Client object destroyed prematurely.',
      'premature connection close; undef() used; command error message' );

  return;
}
