use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 24;
use AnyEvent::Redis::RipeRedis qw( E_OPRN_ERROR );

my $redis = AnyEvent::Redis::RipeRedis->new(
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
);

t_conn_timeout( $redis );
t_read_timeout( $redis );
t_reconnect( $redis );
t_encoding( $redis );
t_on_connect( $redis );
t_on_disconnect( $redis );
t_on_connect_error( $redis );
t_on_error( $redis );

$redis->disconnect();


####
sub t_conn_timeout {
  my $redis = shift;

  my $t_conn_timeout = $redis->connection_timeout();
  is( $t_conn_timeout, 10, "Get 'connection_timeout'" );

  $redis->connection_timeout( undef );
  is( $redis->{connection_timeout}, undef,
      "Reset to default 'connection_timeout'" );

  $redis->connection_timeout( 15 );
  is( $redis->{connection_timeout}, 15, "Set 'connection_timeout'" );

  return;
}

####
sub t_read_timeout {
  my $redis = shift;

  my $t_read_timeout = $redis->read_timeout();
  is( $t_read_timeout, 5, "Get 'read_timeout'" );

  $redis->read_timeout( undef );
  is( $redis->{read_timeout}, undef, "Disable 'read_timeout'" );

  $redis->read_timeout( 10 );
  is( $redis->{read_timeout}, 10, "Set 'read_timeout'" );

  return;
}

####
sub t_reconnect {
  my $redis = shift;

  my $reconn_state = $redis->reconnect();
  is( $reconn_state, 1, "Get current reconnection mode state" );

  $redis->reconnect( undef );
  is( $redis->{reconnect}, undef, "Disable reconnection mode" );

  $redis->reconnect( 1 );
  is( $redis->{reconnect}, 1, "Enable reconnection mode" );

  return;
}

####
sub t_encoding {
  my $redis = shift;

  my $t_enc = $redis->encoding();
  is( $redis->{encoding}->name(), 'UTF-16', "Get 'encoding'" );

  $redis->encoding( undef );
  is( $redis->{encoding}, undef, "Disable 'encoding'" );

  $redis->encoding( 'utf8' );
  is( $redis->{encoding}->name(), 'utf8', "Set 'encoding'" );

  return;
}

####
sub t_on_connect {
  my $redis = shift;

  my $on_conn = $redis->on_connect();
  is( $on_conn->(), 1, "Get 'on_connect' callback" );

  $redis->on_connect( undef );
  is( $redis->{on_connect}, undef, "Disable 'on_connect' callback" );

  $redis->on_connect(
    sub {
      return 5;
    }
  );
  is( $redis->{on_connect}->(), 5, "Set 'on_connect' callback" );

  return;
}

####
sub t_on_disconnect {
  my $redis = shift;

  my $on_disconn = $redis->on_disconnect();
  is( $on_disconn->(), 2, "Get 'on_disconnect' callback" );

  $redis->on_disconnect( undef );
  is( $redis->{on_disconnect}, undef, "Disable 'on_disconnect' callback" );

  $redis->on_disconnect(
    sub {
      return 6;
    }
  );
  is( $redis->{on_disconnect}->(), 6, "Set 'on_disconnect' callback" );

  return;
}

####
sub t_on_connect_error {
  my $redis = shift;

  my $on_conn_error = $redis->on_connect_error();
  is( $on_conn_error->(), 3, "Get 'on_connect_error' callback" );

  $redis->on_connect_error( undef );
  is( $redis->{on_connect_error}, undef, "Disable 'on_connect_error' callback" );

  $redis->on_connect_error(
    sub {
      return 7;
    }
  );
  is( $redis->{on_connect_error}->(), 7, "Set 'on_connect_error' callback" );

  return;
}

####
sub t_on_error {
  my $redis = shift;

  my $on_error = $redis->on_error();
  is( $on_error->(), 4, "Get 'on_error' callback" );

  local %SIG;
  my $t_err;
  $SIG{__WARN__} = sub {
    $t_err = shift;
    chomp( $t_err );
  };
  $redis->on_error( undef );
  $redis->{on_error}->( 'Some error', E_OPRN_ERROR );
  is( $t_err, 'Some error', "Reset to default 'on_error' callback" );

  $redis->on_error(
    sub {
      return 8;
    }
  );
  is( $redis->{on_error}->(), 8, "Set 'on_error' callback" );

  return;
}
