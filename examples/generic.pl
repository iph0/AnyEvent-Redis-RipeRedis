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
  on_connect_error => sub {
    my $err_msg = shift;
    warn "$err_msg\n";
  },
);

# Increment
$redis->incr( 'foo', {
  on_done => sub {
    my $reply = shift;
    print "$reply\n";
  },
} );

# Set value
$redis->set( 'bar', 'Some string', {
  on_done => sub {
    print "SET is done\n";
  },
} );

# Get value
$redis->get( 'bar', {
  on_done => sub {
    my $reply = shift;
    print "$reply\n";
  },
} );

# Push values
for ( my $i = 1; $i <= 3; $i++ ) {
  $redis->rpush( 'list', "element_$i", {
    on_done => sub {
      my $reply = shift;
      print "$reply\n";
    },
  } );
}

# Get list of values
$redis->lrange( 'list', 0, -1, {
  on_done => sub {
    my $reply = shift;

    foreach my $val ( @{$reply} ) {
      print "$val\n";
    }
  },
} );

# Transaction
$redis->multi( {
  on_done => sub {
    print "Transaction begun\n";
  },
} );
$redis->incr( 'foo', {
  on_done => sub {
    my $reply = shift;
    print "$reply\n";
  },
} );
$redis->lrange( 'list', 0, -1, {
  on_done => sub {
    my $reply = shift;
    print "$reply\n";
  },
} );
$redis->get( 'bar', {
  on_done => sub {
    my $reply = shift;
    print "$reply\n";
  },
} );
$redis->exec( {
  on_done => sub {
    my $reply = shift;

    foreach my $chunk ( @{$reply} ) {
      if ( ref( $chunk ) eq 'ARRAY' ) {
        foreach my $val ( @{$chunk} ) {
          print "$val\n";
        }
      }
      else {
        print "$chunk\n";
      }
    }
  },
} );

# Delete keys
$redis->del( qw( foo bar list ), {
  on_done => sub {
    print "Keys removed\n";
  }
} );

# Disconnect
$redis->quit( {
  on_done => sub {
    $cv->send();
  }
} );

$cv->recv();
