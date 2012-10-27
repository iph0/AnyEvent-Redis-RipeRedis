use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 6;
use Test::AnyEvent::RedisHandle;
use Test::AnyEvent::EVLoop;
use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $T_CLASS = 'AnyEvent::Redis::RipeRedis';

my $cv = AnyEvent->condvar();

my $redis = $T_CLASS->new(
  password => 'test',
  lazy => 1,
);

# Authenticate
$redis->auth( 'test' );

# Subscribe to channels by name
my @t_sub_data;
my @t_sub_msgs;
$redis->subscribe( qw( ch_foo ch_bar ), {
  on_done =>  sub {
    my $ch_name = shift;
    my $subs_num = shift;

    push( @t_sub_data, {
      ch_name => $ch_name,
      subs_num => $subs_num,
    } )
  },

  on_message => sub {
    my $ch_name = shift;
    my $msg = shift;

    push( @t_sub_msgs, {
      ch_name => $ch_name,
      message => $msg,
    } );
  },
} );
$redis->subscribe( 'ch_test', {
  on_done =>  sub {
    my $ch_name = shift;
    my $subs_num = shift;

    push( @t_sub_data, {
      ch_name => $ch_name,
      subs_num => $subs_num,
    } )
  },
} );

# Subscribe to channels by pattern
my @t_psub_data;
my @t_psub_msgs;
$redis->psubscribe( qw( info_* err_* ), {
  on_done =>  sub {
    my $ch_pattern = shift;
    my $subs_num = shift;

    push( @t_psub_data, {
      ch_pattern => $ch_pattern,
      subs_num => $subs_num,
    } )
  },

  on_message => sub {
    my $ch_name = shift;
    my $msg = shift;
    my $ch_pattern = shift;

    push( @t_psub_msgs, {
      ch_name => $ch_name,
      message => $msg,
      ch_pattern => $ch_pattern,
    } );
  },
} );

# Unsubscribe after timeout
my @t_unsub_data;
my @t_punsub_data;
my $unsub_timer;
$unsub_timer = AnyEvent->timer(
  after => 0.001,
  cb => sub {
    undef( $unsub_timer );

    $redis->unsubscribe( qw( ch_foo ch_bar ), {
      on_done => sub {
        my $ch_name = shift;
        my $subs_num = shift;

        push( @t_unsub_data, {
          ch_name => $ch_name,
          subs_num => $subs_num,
        } );
      },
    } );

    $redis->punsubscribe( qw( info_* err_* ), {
      on_done => sub {
        my $ch_pattern = shift;
        my $subs_num = shift;

        push( @t_punsub_data, {
          ch_pattern => $ch_pattern,
          subs_num => $subs_num,
        } );
      },
    } );

    $redis->quit( {
      on_done => sub {
        $cv->send();
      },
    } );
  }
);

ev_loop( $cv );

is_deeply( \@t_sub_data, [
  {
    ch_name => 'ch_foo',
    subs_num => 1,
  },
  {
    ch_name => 'ch_bar',
    subs_num => 2,
  },
  {
    ch_name => 'ch_test',
    subs_num => 3,
  },
], 'subscribe' );

is_deeply( \@t_sub_msgs, [
  {
    ch_name => 'ch_foo',
    message => 'test',
  },
  {
    ch_name => 'ch_bar',
    message => 'test',
  },
], 'message' );

is_deeply( \@t_unsub_data, [
  {
    ch_name => 'ch_foo',
    subs_num => 4,
  },
  {
    ch_name => 'ch_bar',
    subs_num => 3,
  },
], 'unsubscribe' );

is_deeply( \@t_psub_data, [
  {
    ch_pattern => 'info_*',
    subs_num => 4,
  },
  {
    ch_pattern => 'err_*',
    subs_num => 5,
  }
], 'psubscribe' );

is_deeply( \@t_psub_msgs, [
  {
    ch_name => 'info_some',
    message => 'test',
    ch_pattern => 'info_*',
  },
  {
    ch_name => 'err_some',
    message => 'test',
    ch_pattern => 'err_*',
  },
], 'pmessage' );

is_deeply( \@t_punsub_data, [
  {
    ch_pattern => 'info_*',
    subs_num => 2,
  },
  {
    ch_pattern => 'err_*',
    subs_num => 1,
  },
], 'punsubscribe' );
