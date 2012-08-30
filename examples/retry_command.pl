#!/usr/bin/perl

use strict;
use warnings;

use AnyEvent;
use AnyEvent::Redis::RipeRedis qw( :err_codes );

my $redis = AnyEvent::Redis::RipeRedis->new(
  host => 'localhost',
  port => '6379',
  password => 'your_password',
  encoding => 'utf8',

  on_connect => sub {
    print "Connected\n";
  },

  on_disconnect => sub {
    print "Disconnected from Redis server\n";
  },

  on_error => sub {
    my $err_msg = shift;
    my $err_code = shift;

    warn "$err_msg. Error code: $err_code\n";
  },
);

my $cv = AnyEvent->condvar();

# Set value (retry on error)
set( $redis, 'bar', 'Some string', {
  on_done => sub {
    my $data = shift;
    print "$data\n";
    $cv->send();
  },

  on_error => sub {
    my $err_msg = shift;
    my $err_code = shift;

    warn "$err_msg. Error code: $err_code\n";
    $cv->croak();
  }
} );

$cv->recv();

$redis->disconnect();


####
sub set {
  my $redis = shift;
  my $key = shift;
  my $value = shift;
  my $params = shift;

  $redis->set( $key, $value, {
    on_done => $params->{on_done},
    on_error => sub {
      my $err_msg = shift;
      my $err_code = shift;

      if (
        $err_code == E_CANT_CONN
          or $err_code == E_LOADING_DATASET
          or $err_code == E_IO_OPERATION
          or $err_code == E_CONN_CLOSED_BY_REMOTE_HOST
          ) {
        warn "$err_msg. Error code: $err_code\n";
        my $timer;
        $timer = AnyEvent->timer(
          after => 3,
          cb => sub {
            undef( $timer );
            set( $redis, $key, $value, $params );
          },
        );
      }
      else {
        $params->{on_error}->( $err_msg, $err_code );
      }
    },
  } );
}
