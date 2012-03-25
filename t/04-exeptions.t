use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 8;
use Test::AnyEvent::RedisHandle;
use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $t_class = 'AnyEvent::Redis::RipeRedis';

my $redis;

# Invalid encoding
eval {
  $redis = $t_class->new(
    encoding => 'invalid_enc',
  );
};
if ( $@ ) {
  my $exp_msg = "Encoding 'invalid_enc' not found";
  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}

# Invalid "on_connect"
eval {
  $redis = $t_class->new(
    on_connect => 'invalid',
  );
};
if ( $@ ) {
  my $exp_msg = "'on_connect' callback must be a CODE reference";
  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}

# Invalid "on_disconnect"
eval {
  $redis = $t_class->new(
    on_disconnect => {},
  );
};
if ( $@ ) {
  my $exp_msg = "'on_disconnect' callback must be a CODE reference";
  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}

# Invalid "on_error"
eval {
  $redis = $t_class->new(
    on_error => [],
  );
};
if ( $@ ) {
  my $exp_msg = "'on_error' callback must be a CODE reference";
  ok( index( $@, $exp_msg ) == 0, "$exp_msg (parameter of the constructor)" );
}

$redis = $t_class->new();

# Invalid "on_done"
eval {
  $redis->incr( 'foo', {
    on_done => {},
  } );
};
if ( $@ ) {
  my $exp_msg = "'on_done' callback must be a CODE reference";
  ok( index( $@, $exp_msg ) == 0, "$exp_msg" );
}

# Invalid "on_error"
eval {
  $redis->incr( 'foo', {
    on_error => [],
  } );
};
if ( $@ ) {
  my $exp_msg = "'on_error' callback must be a CODE reference";
  ok( index( $@, $exp_msg ) == 0, "$exp_msg (parameter of the method)" );
}

# Invalid "on_message"
eval {
  $redis->subscribe( 'channel', {
    on_message => 'invalid',
  } );
};
if ( $@ ) {
  my $exp_msg = "'on_message' callback must be a CODE reference";
  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}

# Subscription in transactional context
$redis->multi();
eval {
  $redis->subscribe( 'channel' );
};
if ( $@ ) {
  my $exp_msg = "Command 'subscribe' not allowed in this context."
      . " First, the transaction must be completed.";
  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}
