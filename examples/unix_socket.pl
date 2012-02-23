#!/usr/bin/perl

use 5.010000;
use strict;
use warnings;

use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $redis = AnyEvent::Redis::RipeRedis->new( {
  host => 'unix/',
  port => '/tmp/redis.sock',
  password => 'your_password',
  encoding => 'utf8',
  reconnect => 1,
  reconnect_after => 5,
  max_connect_attempts => 10,

  on_connect => sub {
    my $attempt = shift;

    say "Connected: $attempt";
  },

  on_stop_reconnect => sub {
    say 'Stop reconnecting';
  },

  on_redis_error => sub {
    my $msg = shift;

    warn "Redis error: $msg\n";
  },

  on_error => sub {
    my $msg = shift;

    warn "$msg\n";
  }
} );

my $cv = AnyEvent->condvar();

my $timer;

$timer = AnyEvent->timer(
  after => 0,
  interval => 1,
  cb => sub {
    $redis->incr( 'foo', sub {
      my $val = shift;

      say $val;
    } );
  }
);

my $sig_cb = sub {
  say 'Stopped';

  $cv->send();
};

my $int_watcher = AnyEvent->signal(
  signal => 'INT',
  cb => $sig_cb
);

my $term_watcher = AnyEvent->signal(
  signal => 'TERM',
  cb => $sig_cb
);

$cv->recv();
