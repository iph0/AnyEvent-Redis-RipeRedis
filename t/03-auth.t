use 5.008000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
require 't/test_helper.pl';

my $SERVER_INFO = run_redis_instance(
  requirepass => 'testpass',
);
if ( !defined $SERVER_INFO ) {
  plan skip_all => 'redis-server is required for this test';
}
plan tests => 6;

t_successful_auth( $SERVER_INFO );
t_invalid_password( $SERVER_INFO );


####
sub t_successful_auth {
  my $server_info = shift;

  my $redis = AnyEvent::Redis::RipeRedis->new(
    host     => $server_info->{host},
    port     => $server_info->{port},
    password => $server_info->{password},
  );

  can_ok( $redis, 'disconnect' );

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->ping(
        { on_done => sub {
            $t_reply = shift;
            $cv->send();
          },
        }
      );
    }
  );

  $redis->disconnect();

  is( $t_reply, 'PONG', 'successful AUTH' );
}

####
sub t_invalid_password {
  my $server_info = shift;

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
        password => 'invalid',

        on_error => sub {
          $t_comm_err_msg  = shift;
          $t_comm_err_code = shift;
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
    }
  );

  $redis->disconnect();

  like( $t_cmd_err_msg, qr/^Operation 'ping' aborted:/,
      'invalid password; command error message' );
  is( $t_cmd_err_code, E_OPRN_ERROR, 'invalid password; command error code' );
  ok( defined $t_comm_err_msg, 'invalid password; common error message' );
  is( $t_comm_err_code, E_OPRN_ERROR, 'invalid password; common error code' );

  return;
}
