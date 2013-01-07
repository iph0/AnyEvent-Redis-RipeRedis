use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 16;
use Test::AnyEvent::RedisHandle;
use AnyEvent;

my $T_CLASS;

BEGIN {
  $T_CLASS = 'AnyEvent::Redis::RipeRedis';
  use_ok( $T_CLASS );
}

can_ok( $T_CLASS, 'new' );

t_conn_timeout();
t_read_timeout();
t_encoding();
t_on_message();


####
sub t_conn_timeout {
  my $t_except;

  eval {
    my $t_redis = $T_CLASS->new(
      connection_timeout => 'invalid',
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Connection timeout must be a positive number/o,
      'Invalid connection timeout (constructor, character string)' );
  undef( $t_except );

  eval {
    my $t_redis = $T_CLASS->new(
      connection_timeout => -5,
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Connection timeout must be a positive number/o,
      'Invalid connection timeout (constructor, negative number)' );
  undef( $t_except );

  my $t_redis = new_ok( $T_CLASS );

  eval {
    $t_redis->connection_timeout( 'invalid' );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Connection timeout must be a positive number/o,
      'Invalid connection timeout (accessor, character string)' );
  undef( $t_except );

  eval {
    $t_redis->connection_timeout( -5 );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Connection timeout must be a positive number/o,
      'Invalid connection timeout (accessor, negative number)' );

  return;
}

####
sub t_read_timeout {
  my $t_except;

  eval {
    my $t_redis = $T_CLASS->new(
      read_timeout => 'invalid',
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Read timeout must be a positive number/o,
      'Invalid read timeout (constructor, character string)' );
  undef( $t_except );

  eval {
    my $t_redis = $T_CLASS->new(
      read_timeout => -5,
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Read timeout must be a positive number/o,
      'Invalid read timeout (constructor, negative number)' );
  undef( $t_except );

  my $t_redis = new_ok( $T_CLASS );

  eval {
    $t_redis->read_timeout( 'invalid' );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Read timeout must be a positive number/o,
      'Invalid read timeout (accessor, character string)' );
  undef( $t_except );

  eval {
    $t_redis->read_timeout( -5 );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Read timeout must be a positive number/o,
      'Invalid read timeout (accessor, negative number)' );

  return;
}

####
sub t_encoding {
  my $t_except;

  eval {
    my $t_redis = $T_CLASS->new(
      encoding => 'utf88',
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Encoding 'utf88' not found/o,
      'Invalid encoding (constructor)' );
  undef( $t_except );

  my $t_redis = new_ok( $T_CLASS );

  eval {
    $t_redis->encoding( 'utf88' );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Encoding 'utf88' not found/o,
      'Invalid encoding (accessor)' );

  return;
}

####
sub t_on_message {
  my $t_redis = $T_CLASS->new();

  my $t_except;

  eval {
    $t_redis->subscribe( 'channel' );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^'on_message' callback must be specified/o,
      "'on_message' callback not specified" );

  return;
}
