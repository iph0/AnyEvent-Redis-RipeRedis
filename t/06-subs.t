use 5.006000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
require 't/test_helper.pl';

my $server_info = run_redis_instance();
if ( !defined( $server_info ) ) {
  plan skip_all => 'redis-server is required to this test';
}
plan tests => 7;

my $r_consum = AnyEvent::Redis::RipeRedis->new(
  host => $server_info->{host},
  port => $server_info->{port},
);
my $r_transm = AnyEvent::Redis::RipeRedis->new(
  host => $server_info->{host},
  port => $server_info->{port},
);

t_sub_unsub( $r_consum, $r_transm );
t_psub_punsub( $r_consum, $r_transm );

$r_consum->disconnect();
$r_transm->disconnect();

t_sub_after_multi( $server_info );


####
sub t_sub_unsub {
  my $r_consum = shift;
  my $r_transm = shift;

  my @t_sub_data;
  my @t_sub_msgs;

  ev_loop(
    sub {
      my $cv = shift;

      my $done_cnt = 0;

      $r_consum->subscribe( qw( ch_foo ch_bar ), {
        on_done =>  sub {
          my $ch_name = shift;
          my $subs_num = shift;

          push( @t_sub_data, {
            ch_name => $ch_name,
            subs_num => $subs_num,
          } );
          $r_transm->publish( $ch_name, "test$subs_num" );
        },

        on_message => sub {
          my $ch_name = shift;
          my $msg = shift;

          push( @t_sub_msgs, {
            ch_name => $ch_name,
            message => $msg,
          } );
          if ( ++$done_cnt == 2 ) {
            $cv->send();
          }
        },
      } );
    }
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
  ], 'SUBSCRIBE' );

  is_deeply( \@t_sub_msgs, [
    {
      ch_name => 'ch_foo',
      message => 'test1',
    },
    {
      ch_name => 'ch_bar',
      message => 'test2',
    },
  ], 'publish message' );

  my @t_unsub_data;

  ev_loop(
    sub {
      my $cv = shift;

      $r_consum->unsubscribe( qw( ch_foo ch_bar ), {
        on_done => sub {
          my $ch_name = shift;
          my $subs_num = shift;

          push( @t_unsub_data, {
            ch_name => $ch_name,
            subs_num => $subs_num,
          } );
          if ( $subs_num == 0 ) {
            $cv->send();
          }
        },
      } );
    }
  );

  is_deeply( \@t_unsub_data, [
    {
      ch_name => 'ch_foo',
      subs_num => 1,
    },
    {
      ch_name => 'ch_bar',
      subs_num => 0,
    },
  ], 'UNSUBSCRIBE' );

  return;
}

####
sub t_psub_punsub {
  my $r_consum = shift;
  my $r_transm = shift;

  my @t_psub_data;
  my @t_psub_msgs;

  ev_loop(
    sub {
      my $cv = shift;

      my $done_cnt = 0;

      $r_consum->psubscribe( qw( info_* err_* ), {
        on_done =>  sub {
          my $ch_pattern = shift;
          my $subs_num = shift;

          push( @t_psub_data, {
            ch_pattern => $ch_pattern,
            subs_num => $subs_num,
          } );
          my $ch_name = $ch_pattern;
          $ch_name =~ s/\*/some/o;
          $r_transm->publish( $ch_name, "test$subs_num" );
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
          if ( ++$done_cnt == 2 ) {
            $cv->send();
          }
        },
      } );
    }
  );

  is_deeply( \@t_psub_data, [
    {
      ch_pattern => 'info_*',
      subs_num => 1,
    },
    {
      ch_pattern => 'err_*',
      subs_num => 2,
    }
  ], 'PSUBSCRIBE' );

  is_deeply( \@t_psub_msgs, [
    {
      ch_name => 'info_some',
      message => 'test1',
      ch_pattern => 'info_*',
    },
    {
      ch_name => 'err_some',
      message => 'test2',
      ch_pattern => 'err_*',
    },
  ], 'publish pmessage' );

  my @t_punsub_data;

  ev_loop(
    sub {
      my $cv = shift;

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
  );

  is_deeply( \@t_punsub_data, [
    {
      ch_pattern => 'info_*',
      subs_num => 1,
    },
    {
      ch_pattern => 'err_*',
      subs_num => 0,
    },
  ], 'PUNSUBSCRIBE' );

  return;
}

####
sub t_sub_after_multi {
  my $server_info = shift;

  my $redis = AnyEvent::Redis::RipeRedis->new(
    host => $server_info->{host},
    port => $server_info->{port},
  );

  my $t_err_msg;
  my $t_err_code;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi();
      $redis->subscribe( 'channel', {
        on_message => sub {
          my $msg = shift;
        },
        on_error => sub {
          $t_err_msg = shift;
          $t_err_code = shift;

          $cv->send();
        }
      } );
    },
  );

  is_deeply( [ $t_err_msg, $t_err_code ], [ "Command 'subscribe' not allowed"
      . " after 'multi' command. First, the transaction must be completed",
      E_OPRN_ERROR ],
      'subscription after MULTI command' );

  return;
}
