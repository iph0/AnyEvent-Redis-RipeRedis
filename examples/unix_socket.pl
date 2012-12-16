#!/usr/bin/perl

use strict;
use warnings;

use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $cv = AE::cv();

my $redis;
$redis = AnyEvent::Redis::RipeRedis->new(
  host => 'unix/',
  port => '/tmp/redis.sock',
  password => 'your_password',
  read_timeout => 3,

  on_connect => sub {
    print "Connected to Redis server\n";
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

my $timer;
$timer = AE::timer( 0, 1,
  sub {
    $redis->incr( 'foo', {
      on_done => sub {
        my $data = shift;
        print "$data\n";
      },
    } );
  },
);

my $on_signal = sub {
  print "Stopped\n";
  $cv->send();
};

my $int_w = AE::signal( INT => $on_signal );
my $term_w = AE::signal( TERM => $on_signal );

$cv->recv();

$redis->disconnect();
