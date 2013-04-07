use 5.006000;
use strict;
use warnings;

use Test::More tests => 11;
use AnyEvent::Redis::RipeRedis;

t_conn_timeout();
t_read_timeout();
t_encoding();
t_on_message();


####
sub t_conn_timeout {
  my $t_except;

  eval {
    my $redis = AnyEvent::Redis::RipeRedis->new(
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
    my $redis = AnyEvent::Redis::RipeRedis->new(
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

  my $redis = AnyEvent::Redis::RipeRedis->new();

  eval {
    $redis->connection_timeout( 'invalid' );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Connection timeout must be a positive number/o,
      'Invalid connection timeout (accessor, character string)' );
  undef( $t_except );

  eval {
    $redis->connection_timeout( -5 );
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
    my $redis = AnyEvent::Redis::RipeRedis->new(
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
    my $redis = AnyEvent::Redis::RipeRedis->new(
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

  my $redis = AnyEvent::Redis::RipeRedis->new();

  eval {
    $redis->read_timeout( 'invalid' );
  };
  if ( $@ ) {
    chomp( $@ );
    $t_except = $@;
  }
  ok( $t_except =~ m/^Read timeout must be a positive number/o,
      'Invalid read timeout (accessor, character string)' );
  undef( $t_except );

  eval {
    $redis->read_timeout( -5 );
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
    my $redis = AnyEvent::Redis::RipeRedis->new(
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

  my $redis = AnyEvent::Redis::RipeRedis->new();

  eval {
    $redis->encoding( 'utf88' );
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
  my $redis = AnyEvent::Redis::RipeRedis->new();

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
