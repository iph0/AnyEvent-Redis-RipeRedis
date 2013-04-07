use 5.006000;
use strict;
use warnings;
use utf8;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( E_INVALID_PASS );
require 't/test_helper.pl';

my $password = 'testpass';
my $server_info = run_redis_instance(
  requirepass => $password,
);

plan tests => 2;

t_auth();
t_invalid_password();


####
sub t_auth {
  my $redis = AnyEvent::Redis::RipeRedis->new(
    host => $server_info->{host},
    port => $server_info->{port},
    password => $password,
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

  is( $t_data, 'PONG', 'Successful auth' );

  $redis->disconnect();
}

####
sub t_invalid_password {
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

  is_deeply( \@t_err_codes, [ E_INVALID_PASS, E_INVALID_PASS ],
      'Invalid password' );

  $redis->disconnect();

  return;
}
