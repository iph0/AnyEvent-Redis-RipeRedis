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
  encoding => 'utf8',

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

# Subscribe to channels by name
$redis->subscribe( qw( ch_foo ch_bar ), {
  on_done =>  sub {
    my $ch_name = shift;
    my $subs_num = shift;

    print "Subscribed: $ch_name. Active: $subs_num\n";
  },

  on_message => sub {
    my $ch_name = shift;
    my $msg = shift;

    print "$ch_name: $msg\n";
  },
} );

# Subscribe to channels by pattern
$redis->psubscribe( qw( info_* err_* ), {
  on_done =>  sub {
    my $ch_pattern = shift;
    my $subs_num = shift;

    print "Subscribed: $ch_pattern. Active: $subs_num\n";
  },

  on_message => sub {
    my $ch_name = shift;
    my $msg = shift;
    my $ch_pattern = shift;

    print "$ch_name ($ch_pattern): $msg\n";
  },
} );

# Unsubscribe
my $sig_cb = sub {
  print "Stopped\n";

  $redis->unsubscribe( qw( ch_foo ch_bar ), {
    on_done => sub {
      my $ch_name = shift;
      my $subs_num = shift;

      print "Unsubscribed: $ch_name. Active: $subs_num\n";
    },
  } );

  $redis->punsubscribe( qw( info_* err_* ), {
    on_done => sub {
      my $ch_pattern = shift;
      my $subs_num = shift;

      print "Unsubscribed: $ch_pattern. Active: $subs_num\n";

      if ( $subs_num == 0 ) {
        $cv->send();
      }
    },
  } );

  my $timer;
  $timer = AnyEvent->timer(
    after => 5,
    cb => sub {
      undef( $timer );
      exit 0; # Emergency exit
    },
  );
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
