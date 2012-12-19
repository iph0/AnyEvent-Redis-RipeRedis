use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 6;
use Test::AnyEvent::RedisHandle;
use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $T_CLASS = 'AnyEvent::Redis::RipeRedis';

t_conn_timeout();
t_read_timeout();
t_encoding();
t_on_message();


# Subroutines

####
sub t_conn_timeout {
  my $t_except;

  eval {
    my $redis = $T_CLASS->new(
      connection_timeout => 'invalid_timeout',
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Connection timeout must be a positive number/o,
      'Invalid connection timeout (character string)' );

  undef( $t_except );
  eval {
    my $redis = $T_CLASS->new(
      connection_timeout => -5,
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Connection timeout must be a positive number/o,
      'Invalid connection timeout (negative number)' );

  return;
}

####
sub t_read_timeout {
  my $t_except;

  eval {
    my $redis = $T_CLASS->new(
      read_timeout => 'invalid_timeout',
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Read timeout must be a positive number/o,
      'Invalid read timeout (character string)' );

  undef( $t_except );
  eval {
    my $redis = $T_CLASS->new(
      read_timeout => -5,
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Read timeout must be a positive number/o,
      'Invalid read timeout (negative number)' );

  return;
}

####
sub t_encoding {
  eval {
    my $redis = $T_CLASS->new(
      encoding => 'invalid_enc',
    );
  };
  my $t_except;
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }

  ok( $t_except =~ m/^Encoding 'invalid_enc' not found/o,
      'Invalid encoding' );

  return;
}

####
sub t_on_message {
  my $redis = $T_CLASS->new();

  my $t_except;

  eval {
    $redis->subscribe( 'channel' );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^'on_message' callback must be specified/o,
      "'on_message' callback not specified" );

  return;
}
