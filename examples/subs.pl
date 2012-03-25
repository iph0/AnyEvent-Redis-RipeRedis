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

  on_disconnect => sub {
    say 'Disconnected from Redis';
  },

  on_error => sub {
    my $err = shift;
    warn "$err\n";
  },
);

# Subscribe to channels by name
$redis->subscribe( qw( ch_foo ch_bar ), {
  on_done =>  sub {
    my $ch_name = shift;
    my $subs_num = shift;

    say "Subscribed: $ch_name. Active: $subs_num";
  },

  on_message => sub {
    my $ch_name = shift;
    my $msg = shift;

    say "$ch_name: $msg";
  },
} );

# Subscribe to channels by pattern
$redis->psubscribe( qw( info_* err_* ), {
  on_done =>  sub {
    my $ch_pattern = shift;
    my $subs_num = shift;

    say "Subscribed: $ch_pattern. Active: $subs_num";
  },

  on_message => sub {
    my $ch_name = shift;
    my $msg = shift;
    my $ch_pattern = shift;

    say "$ch_name ($ch_pattern): $msg";
  },
} );

# Unsubscribe
my $sig_cb = sub {
  say 'Stopped';

  $redis->unsubscribe( qw( ch_foo ch_bar ), {
    on_done => sub {
      my $ch_name = shift;
      my $subs_num = shift;

      say "Unsubscribed: $ch_name. Active: $subs_num";
    },
  } );

  $redis->punsubscribe( qw( info_* err_* ), {
    on_done => sub {
      my $ch_pattern = shift;
      my $subs_num = shift;

      say "Unsubscribed: $ch_pattern. Active: $subs_num";

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
