#!/usr/bin/perl

use 5.010000;
use strict;
use warnings;

use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $redis = AnyEvent::Redis::RipeRedis->new(
  host => 'localhost',
  port => '6379',
  encoding => 'utf8',

  on_connect => sub {
    my $attempt = shift;

    say "Connected: $attempt";
  },

  on_error => sub {
    my $msg = shift;

    warn "$msg\n";
  },
);

my $cv = AnyEvent->condvar();

# Authenticate
$redis->auth( 'your_password', {
  on_done => sub {
    my $resp = shift;

    say $resp;
  },

  on_error => sub {
    my $msg = shift;

    warn "Authentication failed; $msg\n";
  },
} );

# Increment
$redis->incr( 'foo',sub {
  my $val = shift;

  say $val;
} );

# Set value
$redis->set( 'bar', 'Some string', sub {
  my $resp = shift;

  say $resp;
} );

# Get value
$redis->get( 'bar', sub {
  my $val = shift;

  say $val;
} );

# Push values
for ( my $i = 1; $i <= 3; $i++ ) {
  $redis->rpush( 'list', "element_$i", sub {
    my $resp = shift;

    say $resp;
  } );
}

# Get list of values
$redis->lrange( 'list', 0, -1, sub {
  my $list = shift;

  foreach my $val ( @{ $list } ) {
    say $val;
  }
} );


# Transaction

$redis->multi( sub {
  my $resp = shift;

  say $resp;
} );

$redis->incr( 'foo', sub {
  my $resp = shift;

  say $resp;
} );

$redis->lrange( 'list', 0, -1, sub {
  my $resp = shift;

  say $resp;
} );

$redis->get( 'bar', sub {
  my $resp = shift;

  say $resp;
} );

$redis->exec( sub {
  my $data_list = shift;

  foreach my $data ( @{ $data_list } ) {

    if ( ref( $data ) eq 'ARRAY' ) {

      foreach my $val ( @{ $data } ) {
        say $val;
      }
    }
    else {
      say $data;
    }
  }
} );


# Delete keys

foreach my $key ( qw( foo bar list ) ) {

  $redis->del( $key, sub {
    my $is_del = shift;

    say $is_del;
  } );
}

# Disconnect
$redis->quit( sub {
  my $resp = shift;

  say $resp;

  $cv->send();
 } );

$cv->recv();
