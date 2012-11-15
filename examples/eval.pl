#!/usr/bin/perl

use strict;
use warnings;

use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $cv = AnyEvent->condvar();

my $redis = AnyEvent::Redis::RipeRedis->new(
  host => 'localhost',
  port => '6379',
  password => 'your_password',

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

# Set value
$redis->set( 'bar', 'Some string', {
  on_done => sub {
    my $data = shift;
    print "$data\n";
  },
} );

# Get value (Lua script)
my $script = <<LUA
  local data = redis.call( "get", KEYS[1] )
  return data
LUA
;
$redis->eval_cached( $script, 1, "bar", {
  on_done => sub {
    my $data = shift;
    print "$data\n";

    # Delete key
    $redis->del( qw( bar ), {
      on_done => sub {
        my $data = shift;
        print "$data\n";
      }
    } );

    # Disconnect
    $redis->quit( {
      on_done => sub {
        my $data = shift;
        print "$data\n";
        $cv->send();
      }
    } );
  },
} );

$cv->recv();
