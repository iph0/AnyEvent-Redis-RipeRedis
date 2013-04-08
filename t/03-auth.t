use 5.006000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
require 't/test_helper.pl';

my $server_info = run_redis_instance(
  requirepass => 'testpass',
);
if ( !defined( $server_info ) ) {
  plan skip_all => 'redis-server is required to this test';
}
plan tests => 3;

t_oprn_not_permitted( $server_info );
t_successful_auth( $server_info );
t_invalid_password( $server_info );


####
sub t_oprn_not_permitted {
  my $server_info = shift;

  my $redis = AnyEvent::Redis::RipeRedis->new(
    host => $server_info->{host},
    port => $server_info->{port},
  );

  my $t_err_code;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->ping( {
        on_error => sub {
          $t_err_code = pop;
          $cv->send();
        }
      } );
    },
  );

  $redis->disconnect();

  is( $t_err_code, E_OPRN_NOT_PERMITTED, 'operation not permitted' );

  return;
}

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
    },
  );

  $redis->disconnect();

  is( $t_data, 'PONG', 'successful AUTH' );
}

####
sub t_invalid_password {
  my $server_info = shift;

  my @t_err_codes;

  my $redis = AnyEvent::Redis::RipeRedis->new(
    host => $server_info->{host},
    port => $server_info->{port},
    password => 'invalid',
    on_error => sub {
      my $err_code = pop;
      push( @t_err_codes, $err_code );
    },
  );

  ev_loop(
    sub {
      my $cv = shift;

      $redis->ping( {
        on_error => sub {
          my $err_code = pop;
          push( @t_err_codes, $err_code );
          $cv->send();
        }
      } );
    },
  );

  $redis->disconnect();

  is_deeply( \@t_err_codes, [ E_INVALID_PASS, E_INVALID_PASS ],
      'invalid password' );

  return;
}
