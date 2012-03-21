use 5.010000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 7;
use Test::AnyEvent::RedisHandle;
use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $t_class = 'AnyEvent::Redis::RipeRedis';

my $cv = AnyEvent->condvar();

my $redis = $t_class->new(
  host => 'localhost',
  port => '6379',
  encoding => 'utf8',

  on_connect => sub {
    my $attempt = shift;

    is( $attempt, 1, 'on_connect' );
  },

  on_error => sub {
    my $msg = shift;

    diag( $msg );
  },
);

# Authenticate
$redis->auth( 'test' );


# Subscribe to channels by name

my @sub_data;
my @sub_msgs;

$redis->subscribe( qw( ch_1 ch_2 ), sub {
  my $ch_name = shift;
  my $msg = shift;

  push( @sub_msgs, {
    ch_name => $ch_name,
    message => $msg,
  } );
} );

$redis->subscribe( qw( ch_foo ch_bar ), {
  on_done =>  sub {
    my $ch_name = shift;
    my $subs_num = shift;

    push( @sub_data, {
      ch_name => $ch_name,
      subs_num => $subs_num,
    } )
  },

  on_message => sub {
    my $ch_name = shift;
    my $msg = shift;

    push( @sub_msgs, {
      ch_name => $ch_name,
      message => $msg,
    } );
  }
} );


# Subscribe to channels by pattern

my @psub_data;
my @psub_msgs;

$redis->psubscribe( qw( chan_* alert_* ), sub {
  my $ch_name = shift;
  my $msg = shift;
  my $ch_pattern = shift;

  push( @psub_msgs, {
    ch_name => $ch_name,
    message => $msg,
    ch_pattern => $ch_pattern,
  } );
} );

$redis->psubscribe( qw( info_* err_* ), {
  on_done =>  sub {
    my $ch_pattern = shift;
    my $subs_num = shift;

    push( @psub_data, {
      ch_pattern => $ch_pattern,
      subs_num => $subs_num,
    } )
  },

  on_message => sub {
    my $ch_name = shift;
    my $msg = shift;
    my $ch_pattern = shift;

    push( @psub_msgs, {
      ch_name => $ch_name,
      message => $msg,
      ch_pattern => $ch_pattern,
    } );
  }
} );


# Unsubscribe after timeout

my @unsub_data;
my @punsub_data;

my $unsub_timeout;

$unsub_timeout = AnyEvent->timer(
  after => 0.001,

  cb => sub {
    undef( $unsub_timeout );

    $redis->unsubscribe( qw( ch_1 ch_2 ) );

    $redis->unsubscribe( qw( ch_foo ch_bar ), sub {
      my $ch_name = shift;
      my $subs_num = shift;

      push( @unsub_data, {
        ch_name => $ch_name,
        subs_num => $subs_num,
      } );
    } );

    $redis->punsubscribe( qw( chan_* alert_* ) );

    $redis->punsubscribe( qw( info_* err_* ), sub {
      my $ch_pattern = shift;
      my $subs_num = shift;

      push( @punsub_data, {
        ch_pattern => $ch_pattern,
        subs_num => $subs_num,
      } );

      if ( $subs_num == 0 ) {
        $cv->send();
      }
    } );
  }
);

my $timeout;

$timeout = AnyEvent->timer(
  after => 5,
  cb => sub {
    undef( $timeout );

    exit 0; # Emergency exit
  },
);

$cv->recv();

my $exp_sub_data = [
  {
    ch_name => 'ch_foo',
    subs_num => 3,
  },
  {
    ch_name => 'ch_bar',
    subs_num => 4,
  },
];

is_deeply( \@sub_data, $exp_sub_data, 'subscribe (on_done)' );

my $exp_sub_msgs = [
  {
    ch_name => 'ch_1',
    message => 'test',
  },
  {
    ch_name => 'ch_2',
    message => 'test',
  },
  {
    ch_name => 'ch_foo',
    message => 'test',
  },
  {
    ch_name => 'ch_bar',
    message => 'test',
  },
];

is_deeply( \@sub_msgs, $exp_sub_msgs, 'message' );

my $exp_unsub_data = [
  {
    ch_name => 'ch_foo',
    subs_num => 5,
  },
  {
    ch_name => 'ch_bar',
    subs_num => 4,
  },
];

is_deeply( \@unsub_data, $exp_unsub_data, 'unsubscribe' );

my $exp_psub_data = [
  {
    ch_pattern => 'info_*',
    subs_num => 7,
  },
  {
    ch_pattern => 'err_*',
    subs_num => 8,
  }
];

is_deeply( \@psub_data, $exp_psub_data, 'psubscribe (on_done)' );

my $exp_psub_msgs = [
  {
    ch_name => 'chan_some',
    message => 'test',
    ch_pattern => 'chan_*',
  },
  {
    ch_name => 'alert_some',
    message => 'test',
    ch_pattern => 'alert_*',
  },
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
];

is_deeply( \@psub_msgs, $exp_psub_msgs, 'pmessage' );

my $exp_punsub_data = [
  {
    ch_pattern => 'info_*',
    subs_num => 1,
  },
  {
    ch_pattern => 'err_*',
    subs_num => 0,
  },
];

is_deeply( \@punsub_data, $exp_punsub_data, 'punsubscribe' );
