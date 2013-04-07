use 5.006000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis;
require 't/test_helper.pl';

my $server_info = run_redis_instance();

plan tests => 6;

# Connect
my $r_consum = AnyEvent::Redis::RipeRedis->new(
  host => $server_info->{host},
  port => $server_info->{port},
);
my $r_transm = AnyEvent::Redis::RipeRedis->new(
  host => $server_info->{host},
  port => $server_info->{port},
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

    # Sub/Unsub to channels by name
    $r_consum->subscribe( qw( ch_foo ch_bar ), {
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

        if ( $msg eq 'unsub' ) {
          $r_consum->unsubscribe( qw( ch_foo ch_bar ), {
            on_done => sub {
              my $ch_name = shift;
              my $subs_num = shift;

              push( @t_unsub_data, {
                ch_name => $ch_name,
                subs_num => $subs_num,
              } );
            },
          } );
        }
      },
    } );
    $r_transm->publish( 'ch_foo', 'test' );
    $r_transm->publish( 'ch_bar', 'unsub' );

    # Subscribe to channels by pattern
    $r_consum->psubscribe( qw( info_* err_* ), {
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

        if ( $msg eq 'unsub' ) {
          $r_consum->punsubscribe( qw( info_* err_* ), {
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
      },
    } );
    $r_transm->publish( 'info_some', 'test' );
    $r_transm->publish( 'err_some', 'unsub' );
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
    message => 'unsub',
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
    message => 'unsub',
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

$r_consum->disconnect();
