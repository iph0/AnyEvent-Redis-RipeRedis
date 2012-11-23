use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 6;
use Test::AnyEvent::RedisHandle;
use Test::AnyEvent::EVLoop;
use AnyEvent::Redis::RipeRedis;

my $T_CLASS = 'AnyEvent::Redis::RipeRedis';

my $redis = $T_CLASS->new(
  password => 'test',
);

my @t_sub_data;
my @t_sub_msgs;
my @t_psub_data;
my @t_psub_msgs;
my @t_unsub_data;
my @t_punsub_data;

ev_loop(
  sub {
    my $cv = shift;

    # Subscribe to channels by name
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

    # Subscribe to channels by pattern
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

            if ( $subs_num == 0 ) {
              $cv->send();
            }
          },
        } );
      }
    );
  },
);

is_deeply( \@t_sub_data, [
  {
    ch_name => 'ch_foo',
    subs_num => 1,
  },
  {
    ch_name => 'ch_bar',
    subs_num => 2,
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
    subs_num => 3,
  },
  {
    ch_name => 'ch_bar',
    subs_num => 2,
  },
], 'unsubscribe' );

is_deeply( \@t_psub_data, [
  {
    ch_pattern => 'info_*',
    subs_num => 3,
  },
  {
    ch_pattern => 'err_*',
    subs_num => 4,
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
    subs_num => 1,
  },
  {
    ch_pattern => 'err_*',
    subs_num => 0,
  },
], 'punsubscribe' );

$redis->disconnect();
