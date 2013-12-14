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

# Subscribe to channels by name
$redis->subscribe( qw( ch_foo ch_bar ),
  { on_done => sub {
      my $ch_name  = shift;
      my $subs_num = shift;

      print "Subscribed: $ch_name. Active: $subs_num\n";
    },

    on_message => sub {
      my $ch_name = shift;
      my $msg     = shift;

      print "$ch_name: $msg\n";
    },
  }
);

# Subscribe to channels by pattern
$redis->psubscribe( qw( info_* err_* ),
  { on_done => sub {
      my $ch_pattern = shift;
      my $subs_num   = shift;

      print "Subscribed: $ch_pattern. Active: $subs_num\n";
    },

    on_message => sub {
      my $ch_name    = shift;
      my $msg        = shift;
      my $ch_pattern = shift;

      print "$ch_name ($ch_pattern): $msg\n";
    },
  }
);

# Unsubscribe
my $on_signal = sub {
  print "Stopped\n";

  $redis->unsubscribe( qw( ch_foo ch_bar ),
    { on_done => sub {
        my $ch_name  = shift;
        my $subs_num = shift;

        print "Unsubscribed: $ch_name. Remaining: $subs_num\n";
      },
    }
  );

  $redis->punsubscribe(
    qw( info_* err_* ),
    { on_done => sub {
        my $ch_pattern = shift;
        my $subs_num   = shift;

        print "Unsubscribed: $ch_pattern. Remaining: $subs_num\n";

        if ( $subs_num == 0 ) {
          $cv->send();
        }
      },
    }
  );

  my $timer;
  $timer = AE::timer( 5, 0,
    sub {
      undef( $timer );
      exit 0; # Emergency exit
    },
  );
};

my $int_w = AE::signal( INT => $on_signal );
my $term_w = AE::signal( TERM => $on_signal );

$cv->recv();

$redis->disconnect();
