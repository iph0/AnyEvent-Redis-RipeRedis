use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 35;
use Test::AnyEvent::RedisHandle;

my $T_CLASS;

BEGIN {
  $T_CLASS = 'AnyEvent::Redis::RipeRedis';
  use_ok( $T_CLASS, qw( E_OPRN_ERROR ) );
}

can_ok( $T_CLASS, 'new' );
can_ok( $T_CLASS, 'connection_timeout' );
can_ok( $T_CLASS, 'read_timeout' );
can_ok( $T_CLASS, 'reconnect' );
can_ok( $T_CLASS, 'encoding' );
can_ok( $T_CLASS, 'on_connect' );
can_ok( $T_CLASS, 'on_disconnect' );
can_ok( $T_CLASS, 'on_connect_error' );
can_ok( $T_CLASS, 'on_error' );

my $T_REDIS = new_ok( $T_CLASS, [
  password => 'test',
  connection_timeout => 10,
  read_timeout => 5,
  reconnect => 1,
  encoding => 'UTF-16',

  on_connect => sub {
    return 1;
  },

  on_disconnect => sub {
    return 2;
  },

  on_connect_error => sub {
    return 3;
  },

  on_error => sub {
    return 4;
  },
] );

t_conn_timeout();
t_read_timeout();
t_reconnect();
t_encoding();
t_on_connect();
t_on_disconnect();
t_on_connect_error();
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
  is( $T_REDIS->{read_timeout}, undef, "Disable 'read_timeout'" );

  $T_REDIS->read_timeout( 10 );
  is( $T_REDIS->{read_timeout}, 10, "Set 'read_timeout'" );

  return;
}

####
sub t_reconnect {
  my $reconn_state = $T_REDIS->reconnect();
  is( $reconn_state, 1, "Get current reconnection mode state" );

  $T_REDIS->reconnect( undef );
  is( $T_REDIS->{reconnect}, undef, "Disable reconnection mode" );

  $T_REDIS->reconnect( 1 );
  is( $T_REDIS->{reconnect}, 1, "Enable reconnection mode" );

  return;
}

####
sub t_encoding {
  my $t_enc = $T_REDIS->encoding();
  is( $T_REDIS->{encoding}->name(), 'UTF-16', "Get 'encoding'" );

  $T_REDIS->encoding( undef );
  is( $T_REDIS->{encoding}, undef, "Disable 'encoding'" );

  $T_REDIS->encoding( 'utf8' );
  is( $T_REDIS->{encoding}->name(), 'utf8', "Set 'encoding'" );

  return;
}

####
sub t_on_connect {
  my $on_conn = $T_REDIS->on_connect();
  is( $on_conn->(), 1, "Get 'on_connect' callback" );

  $T_REDIS->on_connect( undef );
  is( $T_REDIS->{on_connect}, undef, "Disable 'on_connect' callback" );

  $T_REDIS->on_connect(
    sub {
      return 5;
    }
  );
  is( $T_REDIS->{on_connect}->(), 5, "Set 'on_connect' callback" );

  return;
}

####
sub t_on_disconnect {
  my $on_disconn = $T_REDIS->on_disconnect();
  is( $on_disconn->(), 2, "Get 'on_disconnect' callback" );

  $T_REDIS->on_disconnect( undef );
  is( $T_REDIS->{on_disconnect}, undef, "Disable 'on_disconnect' callback" );

  $T_REDIS->on_disconnect(
    sub {
      return 6;
    }
  );
  is( $T_REDIS->{on_disconnect}->(), 6, "Set 'on_disconnect' callback" );

  return;
}

####
sub t_on_connect_error {
  my $on_conn_error = $T_REDIS->on_connect_error();
  is( $on_conn_error->(), 3, "Get 'on_connect_error' callback" );

  $T_REDIS->on_connect_error( undef );
  is( $T_REDIS->{on_connect_error}, undef, "Disable 'on_connect_error' callback" );

  $T_REDIS->on_connect_error(
    sub {
      return 7;
    }
  );
  is( $T_REDIS->{on_connect_error}->(), 7, "Set 'on_connect_error' callback" );

  return;
}

####
sub t_on_error {
  my $on_error = $T_REDIS->on_error();
  is( $on_error->(), 4, "Get 'on_error' callback" );

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
      return 8;
    }
  );
  is( $T_REDIS->{on_error}->(), 8, "Set 'on_error' callback" );

  return;
}

$T_REDIS->disconnect();
