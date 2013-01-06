use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 15;
use Test::AnyEvent::RedisHandle;
use AnyEvent::Redis::RipeRedis qw( E_OPRN_ERROR );

my $T_CLASS = 'AnyEvent::Redis::RipeRedis';

my $T_REDIS = $T_CLASS->new(
  password => 'test',
  connection_timeout => 10,
  read_timeout => 5,
  encoding => 'UTF-16',

  on_disconnect => sub {
    return 1;
  },

  on_error => sub {
    return 2;
  },
);

t_conn_timeout();
t_read_timeout();
t_encoding();
t_on_disconnect();
t_on_error();


####
sub t_conn_timeout {
  my $t_conn_timeout = $T_REDIS->connection_timeout();
  is( $t_conn_timeout, 10, "Get 'connection_timeout'" );

  $T_REDIS->connection_timeout( undef );
  is( $T_REDIS->{connection_timeout}, undef,
      "Reset to default 'connection_timeout'" );

  $T_REDIS->connection_timeout( 15 );
  is( $T_REDIS->{connection_timeout}, 15, "Set 'connection_timeout'" );

  return;
}

####
sub t_read_timeout {
  my $t_read_timeout = $T_REDIS->read_timeout();
  is( $t_read_timeout, 5, "Get 'read_timeout'" );

  $T_REDIS->read_timeout( undef );
  is( $T_REDIS->{read_timeout}, undef, "Remove 'read_timeout'" );

  $T_REDIS->read_timeout( 10 );
  is( $T_REDIS->{read_timeout}, 10, "Set 'read_timeout'" );

  return;
}

####
sub t_encoding {
  my $t_enc = $T_REDIS->encoding();
  is( $T_REDIS->{encoding}->name(), 'UTF-16', "Get 'encoding'" );

  $T_REDIS->encoding( undef );
  is( $T_REDIS->{encoding}, undef, "Remove 'encoding'" );

  $T_REDIS->encoding( 'utf8' );
  is( $T_REDIS->{encoding}->name(), 'utf8', "Set 'encoding'" );

  return;
}

####
sub t_on_disconnect {
  my $on_disconn = $T_REDIS->on_disconnect();
  is( $on_disconn->(), 1, "Get 'on_disconnect' callback" );

  $T_REDIS->on_disconnect( undef );
  is( $T_REDIS->{on_disconnect}, undef, "Remove 'on_disconnect' callback" );

  $T_REDIS->on_disconnect(
    sub {
      return 3;
    }
  );
  is( $T_REDIS->{on_disconnect}->(), 3, "Set 'on_disconnect' callback" );

  return;
}

####
sub t_on_error {
  my $on_error = $T_REDIS->on_error();
  is( $on_error->(), 2, "Get 'on_error' callback" );

  local %SIG;
  my $t_err;
  $SIG{__WARN__} = sub {
    $t_err = shift;
    chomp( $t_err );
  };
  $T_REDIS->on_error( undef );
  $T_REDIS->{on_error}->( 'Some error', E_OPRN_ERROR );
  is( $t_err, 'Some error', "Reset to default 'on_error' callback" );

  $T_REDIS->on_error(
    sub {
      return 4;
    }
  );
  is( $T_REDIS->{on_error}->(), 4, "Set 'on_error' callback" );

  return;
}

$T_REDIS->disconnect();
