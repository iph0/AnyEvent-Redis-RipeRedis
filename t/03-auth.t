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
  plan skip_all => 'redis-server is required to this test';
}
plan tests => 2;

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

  is_deeply( \@t_err_codes, [ E_OPRN_ERROR, E_OPRN_ERROR ],
      'invalid password' );

  return;
}
