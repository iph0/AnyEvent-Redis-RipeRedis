#!/usr/bin/perl

use 5.010000;
use strict;
use warnings;

use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $cv = AnyEvent->condvar();

my $redis = AnyEvent::Redis::RipeRedis->new(
  host => 'localhost',
  port => '6379',
  password => 'your_password',
  encoding => 'utf8',

  on_connect => sub {
    say 'Connected to Redis';
  },

  on_error => sub {
    my $err = shift;
    warn "$err\n";
  },
);

# Increment
$redis->incr( 'foo', {
  on_done => sub {
    my $data = shift;
    say $data;
  },
} );

# Set value
$redis->set( 'bar', 'Some string', {
  on_done => sub {
    my $data = shift;
    say $data;
  },
} );

# Get value
$redis->get( 'bar', {
  on_done => sub {
    my $data = shift;
    say $data;
  },
} );

# Push values
for ( my $i = 1; $i <= 3; $i++ ) {
  $redis->rpush( 'list', "element_$i", {
    on_done => sub {
      my $data = shift;
      say $data;
    },
  } );
}

# Get list of values
$redis->lrange( 'list', 0, -1, {
  on_done => sub {
    my $data = shift;

    foreach my $val ( @{ $data } ) {
      say $val;
    }
  },
} );

# Transaction
$redis->multi( {
  on_done => sub {
    my $data = shift;
    say $data;
  },
} );
$redis->incr( 'foo', {
  on_done => sub {
    my $data = shift;
    say $data;
  },
} );
$redis->lrange( 'list', 0, -1, {
  on_done => sub {
    my $data = shift;
    say $data;
  },
} );
$redis->get( 'bar', {
  on_done => sub {
    my $data = shift;
    say $data;
  },
} );
$redis->exec( {
  on_done => sub {
    my $data = shift;

    foreach my $chunk ( @{ $data } ) {
      if ( ref( $chunk ) eq 'ARRAY' ) {
        foreach my $val ( @{ $chunk } ) {
          say $val;
        }
      }
      else {
        say $chunk;
      }
    }
  },
} );

# Delete keys
foreach my $key ( qw( foo bar list ) ) {
  $redis->del( $key, {
    on_done => sub {
      my $data = shift;
      say $data;
    }
  } );
}

# Disconnect
$redis->quit( {
  on_done => sub {
    my $data = shift;
    say $data;
    $cv->send();
  }
} );

$cv->recv();
