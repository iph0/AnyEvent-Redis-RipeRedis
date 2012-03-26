use 5.006000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 10;
use Test::AnyEvent::RedisHandle;
use AnyEvent;
use AnyEvent::Redis::RipeRedis;

my $t_class = 'AnyEvent::Redis::RipeRedis';

my $redis;
my $line;


# Connection timeout

eval {
  $line = __LINE__ + 1;
  $redis = $t_class->new(
    connection_timeout => 'invalid_timeout',
  );
};
if ( $@ ) {
  chomp( $@ );
  my $exp_msg = "Connection timeout must be a positive number at $0 line $line";
  is( $@, $exp_msg, 'Invalid connection timeout (character string)' );
}

eval {
  $line = __LINE__ + 1;
  $redis = $t_class->new(
    connection_timeout => -5,
  );
};
if ( $@ ) {
  chomp( $@ );
  my $exp_msg = "Connection timeout must be a positive number at $0 line $line";
  is( $@, $exp_msg, 'Invalid connection timeout (negative number)' );
}


# Invalid encoding
eval {
  $line = __LINE__ + 1;
  $redis = $t_class->new(
    encoding => 'invalid_enc',
  );
};
if ( $@ ) {
  chomp( $@ );
  my $exp_msg = "Encoding 'invalid_enc' not found at $0 line $line";
  is( $@, $exp_msg, 'Invalid encoding' );
}

# Invalid "on_connect"
eval {
  $line = __LINE__ + 1;
  $redis = $t_class->new(
    on_connect => 'invalid',
  );
};
if ( $@ ) {
  chomp( $@ );
  my $exp_msg = "'on_connect' callback must be a CODE reference at $0 line $line";
  is( $@, $exp_msg, "Invalid 'on_connect' callback" );
}

# Invalid "on_disconnect"
eval {
  $line = __LINE__ + 1;
  $redis = $t_class->new(
    on_disconnect => {},
  );
};
if ( $@ ) {
  chomp( $@ );
  my $exp_msg = "'on_disconnect' callback must be a CODE reference at $0 line $line";
  is( $@, $exp_msg, "Invalid 'on_disconnect' callback" );
}

# Invalid "on_error"
eval {
  $line = __LINE__ + 1;
  $redis = $t_class->new(
    on_error => [],
  );
};
if ( $@ ) {
  chomp( $@ );
  my $exp_msg = "'on_error' callback must be a CODE reference at $0 line $line";
  is( $@, $exp_msg, "Invalid 'on_error' callback" );
}

$redis = $t_class->new();

# Invalid "on_done"
eval {
  $line = __LINE__ + 1;
  $redis->incr( 'foo', {
    on_done => {},
  } );
};
if ( $@ ) {
  chomp( $@ );
  my $exp_msg = "'on_done' callback must be a CODE reference at $0 line $line";
  is( $@, $exp_msg, "Invalid 'on_done' callback" );
}

# Invalid "on_error"
eval {
  $line = __LINE__ + 1;
  $redis->incr( 'foo', {
    on_error => [],
  } );
};
if ( $@ ) {
  chomp( $@ );
  my $exp_msg = "'on_error' callback must be a CODE reference at $0 line $line";
  is( $@, $exp_msg, "Invalid 'on_error' callback" );
}

# Invalid "on_message"
eval {
  $line = __LINE__ + 1;
  $redis->subscribe( 'channel', {
    on_message => 'invalid',
  } );
};
if ( $@ ) {
  chomp( $@ );
  my $exp_msg = "'on_message' callback must be a CODE reference at $0 line $line";
  is( $@, $exp_msg, "Invalid 'on_message' callback" );
}

# Subscription in transactional context
$redis->multi();
eval {
  $line = __LINE__ + 1;
  $redis->subscribe( 'channel' );
};
if ( $@ ) {
  chomp( $@ );
  my $exp_msg = "Command 'subscribe' not allowed in this context."
      . " First, the transaction must be completed at $0 line $line";
  is( $@, $exp_msg, 'Invalid context for subscribtion' );
}
