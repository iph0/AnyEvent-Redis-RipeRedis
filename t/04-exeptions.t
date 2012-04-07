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
  my $line;
  my $exp_err;

  eval {
    $line = __LINE__ + 1;
    my $redis = $T_CLASS->new(
      connection_timeout => 'invalid_timeout',
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  is( $t_err, "Connection timeout must be a positive number at $0 line $line",
      'Invalid connection timeout (character string)' );

  undef( $t_err );
  eval {
    $line = __LINE__ + 1;
    my $redis = $T_CLASS->new(
      connection_timeout => -5,
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  is( $t_err, "Connection timeout must be a positive number at $0 line $line",
      'Invalid connection timeout (negative number)' );

  return;
}

####
sub t_encoding {
  my $t_err;
  my $line;
  eval {
    $line = __LINE__ + 1;
    my $redis = $T_CLASS->new(
      encoding => 'invalid_enc',
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }
  
  is( $t_err, "Encoding 'invalid_enc' not found at $0 line $line",
      'Invalid encoding' );

  return;
}

# Invalid "on_connect"
####
sub t_on_connect {
  my $t_err;
  my $line;
  eval {
    $line = __LINE__ + 1;
    my $redis = $T_CLASS->new(
      on_connect => 'invalid',
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  is( $t_err, "'on_connect' callback must be a CODE reference at $0 line $line",
      "Invalid 'on_connect' callback" );

  return;
}

####
sub t_on_disconnect {
  my $t_err;
  my $line;
  eval {
    $line = __LINE__ + 1;
    my $redis = $T_CLASS->new(
      on_disconnect => {},
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }

  is( $t_err, "'on_disconnect' callback must be a CODE reference at $0 line $line",
      "Invalid 'on_disconnect' callback" );

  return;
}

####
sub t_on_error {
  my $t_err;
  my $line;
  eval {
    $line = __LINE__ + 1;
    my $redis = $T_CLASS->new(
      on_error => [],
    );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }
  
  is( $t_err, "'on_error' callback must be a CODE reference at $0 line $line",
      "Invalid 'on_error' callback in the constructor" );

  return;
}

####
sub t_on_done {
  my $t_err;
  my $line;
  eval {
    my $redis = $T_CLASS->new();
    $line = __LINE__ + 1;
    $redis->incr( 'foo', {
      on_done => {},
    } );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }
  
  is( $t_err, "'on_done' callback must be a CODE reference at $0 line $line",
      "Invalid 'on_done' callback" );

  return;
}

# Invalid "on_error"
sub t_cmd_on_error {
  my $t_err;
  my $line;
  eval {
    my $redis = $T_CLASS->new();
    $line = __LINE__ + 1;
    $redis->incr( 'foo', {
      on_error => [],
    } );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }
  
  is( $t_err, "'on_error' callback must be a CODE reference at $0 line $line",
      "Invalid 'on_error' callback in the method of the command" );

  return;
}

####
sub t_on_message {
  my $t_err;
  my $line;
  eval {
    my $redis = $T_CLASS->new();
    $line = __LINE__ + 1;
    $redis->subscribe( 'channel', {
      on_message => 'invalid',
    } );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }
  
  is( $t_err, "'on_message' callback must be a CODE reference at $0 line $line",
      "Invalid 'on_message' callback" );

  return;
}

####
sub t_sub_after_multi {
  my $t_err;
  my $line;
  my $redis = $T_CLASS->new();
  $redis->multi();
  eval {
    $line = __LINE__ + 1;
    $redis->subscribe( 'channel' );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_err = $@;
  }
  
  is( $t_err, "Command 'subscribe' not allowed in this context."
      . " First, the transaction must be completed at $0 line $line",
      'Invalid context for subscribtion' );
}
