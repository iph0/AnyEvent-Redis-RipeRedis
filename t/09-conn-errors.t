use 5.006000;
use strict;
use warnings;

use Test::More tests => 3;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
use Scalar::Util qw( weaken );
require 't/test_helper.pl';

t_no_connection();
t_reconnection();
t_read_timeout();


####
sub t_no_connection {
  my $redis;
  my @t_err_codes;

  ev_loop(
    sub {
      my $cv = shift;

      $redis = AnyEvent::Redis::RipeRedis->new(
        port => get_tcp_port(),
        connection_timeout => 1,
        reconnect => 0,
        on_connect_error => sub {
          push( @t_err_codes, E_CANT_CONN );
        },
        on_error => sub {
          my $err_code = pop;

          push( @t_err_codes, $err_code );
        },
      );

      $redis->ping( {
        on_error => sub {
          my $err_code = pop;

          push( @t_err_codes, $err_code );
          $cv->send();
        }
      } );
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

  is_deeply( \@t_err_codes, [ E_CANT_CONN, E_CANT_CONN, E_NO_CONN ],
      "no connection" );

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
      skip 'redis-server is required to this test', 1;
    }

    my @t_data;
    my $redis;

    ev_loop(
      sub {
        my $cv = shift;

        $redis = AnyEvent::Redis::RipeRedis->new(
          host => $server_info->{host},
          port => $server_info->{port},
          on_connect => sub {
            push( @t_data, 'Connected' );
          },
          on_disconnect => sub {
            push( @t_data, 'Disconnected' );
            $cv->send();
          },
          on_error => sub {
            my $err_code = pop;

            push( @t_data, $err_code );
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
      },
    );

    $server_info = run_redis_instance(
      port => $port,
    );

    ev_loop(
      sub {
        my $cv = shift;

        $redis->ping( {
          on_done => sub {
            my $pong = shift;
            push( @t_data, $pong );
            $cv->send();
          }
        } );
      },
    );

    is_deeply( \@t_data, [ 'Connected', E_CONN_CLOSED_BY_REMOTE_HOST,
        'Disconnected', 'Connected', 'PONG' ], 'reconnection' );

    $redis->disconnect();
  }

  return;
}

####
sub t_read_timeout {
  SKIP: {
    my $server_info = run_redis_instance();
    if ( !defined( $server_info ) ) {
      skip 'redis-server is required to this test', 1;
    }

    my $redis;
    my @t_errors;

    ev_loop(
      sub {
        my $cv = shift;

        $redis = AnyEvent::Redis::RipeRedis->new(
          host => $server_info->{host},
          port => $server_info->{port},
          reconnect => 0,
          read_timeout => 1,
          on_error => sub {
            my $err_msg = shift;
            my $err_code = shift;

            push( @t_errors, [ $err_msg, $err_code ] );
            $cv->send();
          },
        );

        $redis->brpop( 'non_existent', '5', {
          on_error => sub {
            my $err_msg = shift;
            my $err_code = shift;

            push( @t_errors, [ $err_msg, $err_code ] );
            $cv->send();
          },
        } );
      },
    );

    is_deeply( \@t_errors, [
      [ "Command 'brpop' aborted: Read timed out", E_READ_TIMEDOUT ],
      [ 'Read timed out', E_READ_TIMEDOUT ],
    ], 'read timeout' );
  }

  return;
}
