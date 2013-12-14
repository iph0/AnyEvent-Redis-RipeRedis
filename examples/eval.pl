#!/usr/bin/perl

use strict;
use warnings;

use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $cv = AE::cv();

my $redis = AnyEvent::Redis::RipeRedis->new(
  host => 'localhost',
  port => '6379',
  password => 'yourpass',

  on_connect => sub {
    print "Connected to Redis server\n";
  },

  on_disconnect => sub {
    print "Disconnected from Redis server\n";
  },
);

# Execute Lua script
$redis->eval_cached( 'return { KEYS[1], KEYS[2], ARGV[1], ARGV[2] }',
    2, 'key1', 'key2', 'first', 'second',
  { on_done => sub {
      my $reply = shift;

      foreach my $val ( @{$reply}  ) {
        print "$val\n";
      }

      $cv->send();
    },
  }
);

$cv->recv();

$redis->disconnect();
