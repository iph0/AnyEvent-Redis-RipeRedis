#!/usr/bin/perl

use 5.010000;
use strict;
use warnings;

use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $redis = AnyEvent::Redis::RipeRedis->new( {
  host => 'localhost',
  port => '6379',
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


# Subscribe to channels by name

my $on_msg_cb = sub {
  my $ch_name = shift;
  my $msg = shift;

  say "$ch_name: $msg";

  if ( $msg eq 'unsub' ) {
    $redis->unsubscribe( qw( channel_1 foo bar ) );
  }
};

$redis->subscribe( 'channel_1', $on_msg_cb );

$redis->subscribe( qw( foo bar ), {
  on_subscribe =>  sub {
    my $ch_name = shift;
    my $subs_num = shift;

    say "Subscribed: $ch_name. Active subscriptions: $subs_num";
  },

  on_message => $on_msg_cb,

  on_unsubscribe => sub {
    my $ch_name = shift;
    my $subs_num = shift;

    say "Unsubscribed: $ch_name. Active subscriptions: $subs_num";

    if ( $subs_num == 0 ) {
      $cv->send();
    }
  }
} );


# Subscribe to channels by pattern

$redis->psubscribe( 'channel_*', {
  on_subscribe =>  sub {
    my $ch_pattern = shift;
    my $subs_num = shift;

    say "Subscribed: $ch_pattern. Active subscriptions: $subs_num";
  },

  on_message => sub {
    my $ch_pattern = shift;
    my $ch_name = shift;
    my $msg = shift;

    say "$ch_name ($ch_pattern): $msg";

    if ( $msg eq 'unsub' ) {
      $redis->punsubscribe( 'channel_*' );
    }
  },

  on_unsubscribe => sub {
    my $ch_pattern = shift;
    my $subs_num = shift;

    say "Unsubscribed: $ch_pattern. Active subscriptions: $subs_num";

    if ( $subs_num == 0 ) {
      $cv->send();
    }
  }
} );

$cv->recv();
