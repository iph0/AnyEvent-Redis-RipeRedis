#!/usr/bin/perl

use 5.010000;
use strict;
use warnings;

use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $cv = AnyEvent->condvar();

my $redis;
$redis = AnyEvent::Redis::RipeRedis->new(
  host => 'unix/',
  port => '/tmp/redis.sock',
  password => 'your_password',
  reconnect => 1,
  encoding => 'utf8',

  on_connect => sub {
    say 'Connected to Redis server';
  },

  on_disconnect => sub {
    say 'Disconnected from Redis server';
  },

  on_error => sub {
    my $err_msg = shift;
    my $err_code = shift;

    warn "$err\n";
  },
);

my $timer;
$timer = AnyEvent->timer(
  after => 0,
  interval => 1,
  cb => sub {
    $redis->incr( 'foo', {
      on_done => sub {
        my $data = shift;
        say $data;
      },
    } );
  },
);

my $sig_cb = sub {
  say 'Stopped';
  $cv->send();
};

my $int_watcher = AnyEvent->signal(
  signal => 'INT',
  cb => $sig_cb,
);

my $term_watcher = AnyEvent->signal(
  signal => 'TERM',
  cb => $sig_cb,
);

$cv->recv();

$redis->disconnect();
