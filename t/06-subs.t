use 5.008000;
use strict;
use warnings;

use Test::More;
use AnyEvent::Redis::RipeRedis qw( :err_codes );
require 't/test_helper.pl';

my $SERVER_INFO = run_redis_instance();
if ( !defined( $SERVER_INFO ) ) {
  plan skip_all => 'redis-server is required for this test';
}
plan tests => 8;

my $R_CONSUM = AnyEvent::Redis::RipeRedis->new(
  host => $SERVER_INFO->{host},
  port => $SERVER_INFO->{port},
);
my $R_TRANSM = AnyEvent::Redis::RipeRedis->new(
  host => $SERVER_INFO->{host},
  port => $SERVER_INFO->{port},
);

t_sub_unsub( $R_CONSUM, $R_TRANSM );
t_psub_punsub( $R_CONSUM, $R_TRANSM );

$R_CONSUM->disconnect();
$R_TRANSM->disconnect();

t_sub_after_multi( $SERVER_INFO );


####
sub t_sub_unsub {
  my $r_consum = shift;
  my $r_transm = shift;

  my @t_sub_data;
  my @t_sub_msgs;

  ev_loop(
    sub {
      my $cv = shift;

      my $msg_cnt = 0;

      $r_consum->subscribe( qw( ch_foo ch_bar ),
        { on_done =>  sub {
            my $ch_name = shift;
            my $subs_num = shift;

            push( @t_sub_data,
              { ch_name  => $ch_name,
                subs_num => $subs_num,
              }
            );

            $r_transm->publish( $ch_name, "test$subs_num" );
          },

          on_message => sub {
            my $ch_name = shift;
            my $msg = shift;

            push( @t_sub_msgs,
              { ch_name => $ch_name,
                message => $msg,
              }
            );

            ++$msg_cnt;
          },
        }
      );

      $r_consum->subscribe( 'ch_coo',
        sub {
          my $ch_name = shift;
          my $msg = shift;

          push( @t_sub_msgs,
            { ch_name => $ch_name,
              message => $msg,
            }
          );

          $msg_cnt++;
        }
      );

      $r_consum->subscribe( 'ch_doo',
        { on_reply =>  sub {
            my $data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
              return;
            }

            push( @t_sub_data,
              { ch_name  => $data->[0],
                subs_num => $data->[1],
              }
            );

            $r_transm->publish( 'ch_coo', 'test3' );
            $r_transm->publish( $data->[0], "test$data->[1]" );
          },

          on_message => sub {
            my $ch_name = shift;
            my $msg = shift;

            push( @t_sub_msgs,
              { ch_name => $ch_name,
                message => $msg,
              }
            );

            $msg_cnt++;
            if ( $msg_cnt == 4 ) {
              $cv->send();
            }
          },
        }
      );
    }
  );

  is_deeply( \@t_sub_data,
    [ { ch_name  => 'ch_foo',
        subs_num => 1,
      },
      { ch_name  => 'ch_bar',
        subs_num => 2,
      },
      { ch_name  => 'ch_doo',
        subs_num => 4,
      },
    ],
    'SUBSCRIBE'
  );

  is_deeply( \@t_sub_msgs,
    [ { ch_name => 'ch_foo',
        message => 'test1',
      },
      { ch_name => 'ch_bar',
        message => 'test2',
      },
      { ch_name => 'ch_coo',
        message => 'test3',
      },
      { ch_name => 'ch_doo',
        message => 'test4',
      },
    ],
    'publish message'
  );

  my @t_unsub_data;

  ev_loop(
    sub {
      my $cv = shift;

      $r_consum->unsubscribe( qw( ch_foo ch_bar ),
        { on_done => sub {
            my $ch_name = shift;
            my $subs_num = shift;

            push( @t_unsub_data,
              { ch_name  => $ch_name,
                subs_num => $subs_num,
              }
            );
          },
        }
      );

      $r_consum->unsubscribe( qw( ch_coo ch_doo ),
        sub {
          my $data = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
            return;
          }

          push( @t_unsub_data,
            { ch_name  => $data->[0],
              subs_num => $data->[1],
            }
          );

          if ( $data->[1] == 0 ) {
            $cv->send();
          }
        }
      );
    }
  );

  is_deeply( \@t_unsub_data,
    [ { ch_name  => 'ch_foo',
        subs_num => 3,
      },
      { ch_name  => 'ch_bar',
        subs_num => 2,
      },
      { ch_name  => 'ch_coo',
        subs_num => 1,
      },
      { ch_name  => 'ch_doo',
        subs_num => 0,
      },
    ],
    'UNSUBSCRIBE'
  );

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

      my $msg_cnt = 0;

      $r_consum->psubscribe( qw( info_* err_* ),
        { on_done =>  sub {
            my $ch_pattern = shift;
            my $subs_num = shift;

            push( @t_psub_data,
              { ch_pattern => $ch_pattern,
                subs_num   => $subs_num,
              }
            );

            my $ch_name = $ch_pattern;
            $ch_name =~ s/\*/some/o;
            $r_transm->publish( $ch_name, "test$subs_num" );
          },

          on_message => sub {
            my $ch_name = shift;
            my $msg = shift;
            my $ch_pattern = shift;

            push( @t_psub_msgs,
              { ch_name    => $ch_name,
                message    => $msg,
                ch_pattern => $ch_pattern,
              }
            );

            $msg_cnt++;
          },
        }
      );

      $r_consum->psubscribe( 'debug_*',
        sub {
          my $ch_name = shift;
          my $msg = shift;
          my $ch_pattern = shift;

          push( @t_psub_msgs,
            { ch_name    => $ch_name,
              message    => $msg,
              ch_pattern => $ch_pattern,
            }
          );

          $msg_cnt++;
        }
      );

      $r_consum->psubscribe( 'warn_*',
        { on_reply => sub {
            my $data = shift;

            if ( defined( $_[0] ) ) {
              diag( $_[0] );
              return;
            }

            push( @t_psub_data,
              { ch_pattern => $data->[0],
                subs_num   => $data->[1],
              }
            );

            $r_transm->publish( 'debug_some', "test3" );

            my $ch_name = $data->[0];
            $ch_name =~ s/\*/some/o;
            $r_transm->publish( $ch_name, "test$data->[1]" );
          },

          on_message => sub {
            my $ch_name = shift;
            my $msg = shift;
            my $ch_pattern = shift;

            push( @t_psub_msgs,
              { ch_name    => $ch_name,
                message    => $msg,
                ch_pattern => $ch_pattern,
              }
            );

            $msg_cnt++;
            if ( $msg_cnt == 4 ) {
              $cv->send();
            }
          },
        }
      );
    }
  );

  is_deeply( \@t_psub_data,
    [ { ch_pattern => 'info_*',
        subs_num   => 1,
      },
      { ch_pattern => 'err_*',
        subs_num   => 2,
      },
      { ch_pattern => 'warn_*',
        subs_num   => 4,
      },
    ],
    'PSUBSCRIBE'
  );

  is_deeply( \@t_psub_msgs,
    [ { ch_name    => 'info_some',
        message    => 'test1',
        ch_pattern => 'info_*',
      },
      { ch_name    => 'err_some',
        message    => 'test2',
        ch_pattern => 'err_*',
      },
      { ch_name    => 'debug_some',
        message    => 'test3',
        ch_pattern => 'debug_*',
      },
      { ch_name    => 'warn_some',
        message    => 'test4',
        ch_pattern => 'warn_*',
      },
    ],
    'publish pmessage'
  );

  my @t_punsub_data;

  ev_loop(
    sub {
      my $cv = shift;

      $r_consum->punsubscribe( qw( info_* err_* ),
        { on_done => sub {
            my $ch_pattern = shift;
            my $subs_num = shift;

            push( @t_punsub_data,
              { ch_pattern => $ch_pattern,
                subs_num   => $subs_num,
              }
            );
          },
        }
      );

      $r_consum->punsubscribe( qw( debug_* warn_* ),
        sub {
          my $data = shift;

          if ( defined( $_[0] ) ) {
            diag( $_[0] );
            return;
          }
          push( @t_punsub_data,
            { ch_pattern => $data->[0],
              subs_num   => $data->[1],
            }
          );

          if ( $data->[1] == 0 ) {
            $cv->send();
          }
        },
      );
    }
  );

  is_deeply( \@t_punsub_data,
    [ { ch_pattern => 'info_*',
        subs_num   => 3,
      },
      { ch_pattern => 'err_*',
        subs_num   => 2,
      },
      { ch_pattern => 'debug_*',
        subs_num   => 1,
      },
      { ch_pattern => 'warn_*',
        subs_num   => 0,
      },
    ],
    'PUNSUBSCRIBE'
  );

  return;
}

####
sub t_sub_after_multi {
  my $server_info = shift;

  my $redis = AnyEvent::Redis::RipeRedis->new(
    host => $server_info->{host},
    port => $server_info->{port},
    on_error => sub {
      # do not print this errors
    },
  );

  my $t_err_msg;
  my $t_err_code;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi();
      $redis->subscribe( 'channel',
        { on_message => sub {
            # empty callback
          },

          on_error => sub {
            $t_err_msg = shift;
            $t_err_code = shift;

            $cv->send();
          },
        }
      );
    }
  );

  $redis->disconnect();

  my $t_name = 'subscription after MULTI command';
  is( $t_err_msg, "Command 'subscribe' not allowed"
      . " after 'multi' command. First, the transaction must be completed.",
      "$t_name; error message" );
  is( $t_err_code, E_OPRN_ERROR, "$t_name; error code" );

  return;
}
