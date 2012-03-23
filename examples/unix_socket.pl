#!/usr/bin/perl

use 5.010000;
use strict;
use warnings;

use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $redis;
my $timer;

$redis = AnyEvent::Redis::RipeRedis->new(
  host => 'unix/',
  port => '/tmp/redis.sock',
  encoding => 'utf8',
  reconnect => 1,
  reconnect_after => 5,
  max_connect_attempts => 15,

  on_connect => sub {
    my $attempt = shift;

    say "Connected: $attempt";

    # Authenticate
    $redis->auth( 'your_password', {
      on_done => sub {
        my $resp = shift;

        say "Authentication $resp";
      },

      on_error => sub {
        my $msg = shift;

        warn "Authentication failed; $msg\n";
      },
    } );

    $timer = AnyEvent->timer(
      after => 0,
      interval => 1,
      cb => sub {
        $redis->incr( 'foo', {
          on_done => sub {
            my $val = shift;

            say $val;
          }
        } );
      },
    );
  },

  on_stop_reconnect => sub {
    say 'Stop reconnecting';
  },

  on_connect_error => sub {
    my $msg = shift;
    my $attempt = shift;

    warn "$msg; $attempt\n";
  },

  on_error => sub {
    my $msg = shift;

    warn "$msg\n";
  },
);

my $cv = AnyEvent->condvar();

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
