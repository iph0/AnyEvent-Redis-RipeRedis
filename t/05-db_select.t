use 5.006000;
use strict;
use warnings;
use utf8;

use Test::More;
use AnyEvent::Redis::RipeRedis;
require 't/test_helper.pl';

setup_redis_instance(
  code => sub {
    my $redis_db1 = shift;
    my $redis_db2 = shift;

    my $t_data = t_set_get( $redis_db1, $redis_db2 );

    is_deeply( $t_data, [ qw( bar1 bar2 ) ], 'DB select without auth' );
  }
);

setup_redis_instance(
  password => 'test',
  code => sub {
    my $redis_db1 = shift;
    my $redis_db2 = shift;

    my $t_data = t_set_get( $redis_db1, $redis_db2 );

    is_deeply( $t_data, [ qw( bar1 bar2 ) ], 'DB select with auth' );
  }
);

done_testing();


####
sub setup_redis_instance {
  my %params = @_;

  my $server_info = run_redis_instance(
    requirepass => $params{password},
  );

  my $redis_db1 = AnyEvent::Redis::RipeRedis->new(
    host => $server_info->{host},
    port => $server_info->{port},
    password => $params{password},
    database => 1,
  );
  my $redis_db2 = AnyEvent::Redis::RipeRedis->new(
    host => $server_info->{host},
    port => $server_info->{port},
    password => $params{password},
    database => 2,
  );

  $params{code}->( $redis_db1, $redis_db2 );

  $redis_db1->disconnect();
  $redis_db2->disconnect();

  $server_info->{server}->stop();
}

####
sub t_set_get {
  my $redis_db1 = shift;
  my $redis_db2 = shift;

  ev_loop(
    sub {
      my $cv = shift;

      my $done_cnt = 0;
      my $on_done = sub {
        ++$done_cnt;
        if ( $done_cnt == 2 ) {
          $cv->send();
        }
      };
      $redis_db1->set( 'foo', 'bar1', {
        on_done => $on_done,
      } );
      $redis_db2->set( 'foo', 'bar2', {
        on_done => $on_done,
      } );
    },
  );

  my @t_data;

  ev_loop(
    sub {
      my $cv = shift;

      my $on_done = sub {
        my $val = shift;

        push( @t_data, $val );
        if ( scalar( @t_data ) == 2 ) {
          $cv->send();
        }
      };
      $redis_db1->get( 'foo', {
        on_done => $on_done,
      } );
      $redis_db2->get( 'foo', {
        on_done => $on_done,
      } );
    },
  );

  return \@t_data;
}
