use 5.008000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
require 't/test_helper.pl';

my $server_info = run_redis_instance(
  requirepass => 'testpass',
);
if ( !defined( $server_info ) ) {
  plan skip_all => 'redis-server is required for this test';
}
plan tests => 5;

t_successful_auth( $server_info );
t_invalid_password( $server_info );


####
sub t_successful_auth {
  my $server_info = shift;

  my $redis = AnyEvent::Redis::RipeRedis->new(
    host => $server_info->{host},
    port => $server_info->{port},
    password => $server_info->{password},
  );

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
    }
  );

  $redis->disconnect();

  is( $t_data, 'PONG', 'successful AUTH' );
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
        host => $server_info->{host},
        port => $server_info->{port},
        password => 'invalid',
        on_error => sub {
          $t_comm_err_msg = shift;
          $t_comm_err_code = shift;
          $cv->send();
        },
      );

      $redis->ping( {
        on_error => sub {
          $t_cmd_err_msg = shift;
          $t_cmd_err_code = shift;
        },
      } );
    }
  );

  $redis->disconnect();

  my $t_name = 'invalid password;';
  like( $t_cmd_err_msg, qr/^Command 'ping' aborted:/o,
      "$t_name; command error message" );
  is( $t_cmd_err_code, E_OPRN_ERROR, "$t_name; command error code" );
  like( $t_comm_err_msg, qr/^ERR/o, "$t_name; common error message" );
  is( $t_comm_err_code, E_OPRN_ERROR, "$t_name; common error code" );

  return;
}
