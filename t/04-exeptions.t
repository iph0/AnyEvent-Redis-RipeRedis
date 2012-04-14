use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 10;
use Test::AnyEvent::RedisHandle;
use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $T_CLASS = 'AnyEvent::Redis::RipeRedis';

t_conn_timeout();
t_encoding();
t_on_connect();
t_on_disconnect();
t_on_error();
t_on_done();
t_cmd_on_error();
t_on_message();
t_sub_after_multi();


# Subroutines

####
sub t_conn_timeout {
  my $t_err;
  my $exp_err;

  eval {
    my $redis = $T_CLASS->new(
      connection_timeout => 'invalid_timeout',
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  ok( $t_err =~ m/^Connection timeout must be a positive number/o,
      'Invalid connection timeout (character string)' );

  undef( $t_err );
  eval {
    my $redis = $T_CLASS->new(
      connection_timeout => -5,
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  ok( $t_err =~ m/^Connection timeout must be a positive number/o,
      'Invalid connection timeout (negative number)' );

  return;
}

####
sub t_encoding {
  my $t_err;
  eval {
    my $redis = $T_CLASS->new(
      encoding => 'invalid_enc',
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  ok( $t_err =~ m/^Encoding 'invalid_enc' not found/o,
      'Invalid encoding' );

  return;
}

# Invalid "on_connect"
####
sub t_on_connect {
  my $t_err;
  eval {
    my $redis = $T_CLASS->new(
      on_connect => 'invalid',
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  ok( $t_err =~ m/^'on_connect' callback must be a CODE reference/o,
      "Invalid 'on_connect' callback" );

  return;
}

####
sub t_on_disconnect {
  my $t_err;
  eval {
    my $redis = $T_CLASS->new(
      on_disconnect => {},
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  ok( $t_err =~ m/^'on_disconnect' callback must be a CODE reference/o,
      "Invalid 'on_disconnect' callback" );

  return;
}

####
sub t_on_error {
  my $t_err;
  eval {
    my $redis = $T_CLASS->new(
      on_error => [],
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  ok( $t_err =~ m/^'on_error' callback must be a CODE reference/o,
      "Invalid 'on_error' callback in the constructor" );

  return;
}

####
sub t_on_done {
  my $t_err;
  eval {
    my $redis = $T_CLASS->new();
    $redis->incr( 'foo', {
      on_done => {},
    } );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  ok( $t_err =~ m/^'on_done' callback must be a CODE reference/o,
      "Invalid 'on_done' callback" );

  return;
}

# Invalid "on_error"
sub t_cmd_on_error {
  my $t_err;
  eval {
    my $redis = $T_CLASS->new();
    $redis->incr( 'foo', {
      on_error => [],
    } );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  ok( $t_err =~ m/^'on_error' callback must be a CODE reference/o,
      "Invalid 'on_error' callback in the method of the command" );

  return;
}

####
sub t_on_message {
  my $t_err;
  eval {
    my $redis = $T_CLASS->new();
    $redis->subscribe( 'channel', {
      on_message => 'invalid',
    } );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  ok( $t_err =~ m/^'on_message' callback must be a CODE reference/o,
      "Invalid 'on_message' callback" );

  return;
}

####
sub t_sub_after_multi {
  my $t_err;
  my $redis = $T_CLASS->new();
  $redis->multi();
  eval {
    $redis->subscribe( 'channel' );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  my $exp_err = quotemeta( "Command 'subscribe' not allowed in this context."
      . " First, the transaction must be completed" );
  ok( $t_err =~ m/^$exp_err/o, 'Invalid context for subscribtion' );

  return;
}
