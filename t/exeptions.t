use 5.010000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 15;
use Test::AnyEvent::RedisHandle;
use AnyEvent;

my $t_class;

BEGIN {
  $t_class = 'AnyEvent::Redis::RipeRedis';

  use_ok( $t_class );
}

my $redis;


# Invalid encoding

eval {
  $redis = $t_class->new(
    encoding => 'invalid_enc'
  );
};

if ( $@ ) {
  my $exp_msg = 'Encoding "invalid_enc" not found';

  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}


# Invalid "reconnect_after"

eval {
  $redis = $t_class->new(
    reconnect => 1,
    reconnect_after => '10_invalid'
  );
};

if ( $@ ) {
  my $exp_msg = '"reconnect_after" must be a positive number';

  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}

eval {
  $redis = $t_class->new(
    reconnect => 1,
    reconnect_after => -10
  );
};

if ( $@ ) {
  my $exp_msg = '"reconnect_after" must be a positive number';

  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}


# Invalid "max_connect_attempts"

eval {
  $redis = $t_class->new(
    reconnect => 1,
    max_connect_attempts => '10_invalid'
  );
};

if ( $@ ) {
  my $exp_msg = '"max_connect_attempts" must be a positive integer number';

  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}

eval {
  $redis = $t_class->new(
    reconnect => 1,
    max_connect_attempts => -10
  );
};

if ( $@ ) {
  my $exp_msg = '"max_connect_attempts" must be a positive integer number';

  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}


# Invalid "on_connect"

eval {
  $redis = $t_class->new(
    on_connect => 'invalid'
  );
};

if ( $@ ) {
  my $exp_msg = '"on_connect" callback must be a CODE reference';

  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}


# Invalid "on_stop_reconnect"

eval {
  $redis = $t_class->new(
    on_stop_reconnect => {}
  );
};

if ( $@ ) {
  my $exp_msg = '"on_stop_reconnect" callback must be a CODE reference';

  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}


# Invalid "on_connect_error"

eval {
  $redis = $t_class->new(
    on_connect_error => 1
  );
};

if ( $@ ) {
  my $exp_msg = '"on_connect_error" callback must be a CODE reference';

  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}


# Invalid "on_redis_error"

eval {
  $redis = $t_class->new(
    on_redis_error => ''
  );
};

if ( $@ ) {
  my $exp_msg = '"on_redis_error" callback must be a CODE reference';

  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}


# Invalid "on_error"

eval {
  $redis = $t_class->new(
    on_error => []
  );
};

if ( $@ ) {
  my $exp_msg = '"on_error" callback must be a CODE reference';

  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}


$redis = new_ok( $t_class );


# Invalid "on_subscribe"

eval {
  $redis->subscribe( 'channel', {
    on_subscribe => {}
  } );
};

if ( $@ ) {
  my $exp_msg = '"on_subscribe" callback must be a CODE reference';

  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}

eval {
  $redis->subscribe( 'channel', {
    on_message => 'invalid'
  } );
};

if ( $@ ) {
  my $exp_msg = '"on_message" callback must be a CODE reference';

  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}


# Subscription in transactional context

$redis->multi();

eval {
  $redis->subscribe( 'channel' );
};

if ( $@ ) {
  my $exp_msg = 'Command "subscribe" not allowed in this context.'
      . ' First, the transaction must be completed.';

  ok( index( $@, $exp_msg ) == 0, $exp_msg );
}
