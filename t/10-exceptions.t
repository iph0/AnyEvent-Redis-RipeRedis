use 5.008000;
use strict;
use warnings;

use Test::More tests => 11;
use Test::Fatal;
use AnyEvent::Redis::RipeRedis;

t_conn_timeout();
t_read_timeout();
t_encoding();
t_on_message();


####
sub t_conn_timeout {
  like(
    exception {
      my $redis = AnyEvent::Redis::RipeRedis->new(
        connection_timeout => 'invalid',
      );
    },
    qr/Connection timeout must be a positive number/,
    'invalid connection timeout (character string; constructor)'
  );

  like(
    exception {
      my $redis = AnyEvent::Redis::RipeRedis->new(
        connection_timeout => -5,
      );
    },
    qr/Connection timeout must be a positive number/,
    'invalid connection timeout (negative number; constructor)'
  );

  my $redis = AnyEvent::Redis::RipeRedis->new();

  like(
    exception {
      $redis->connection_timeout( 'invalid' );
    },
    qr/Connection timeout must be a positive number/,
    'invalid connection timeout (character string; accessor)'
  );

  like(
    exception {
      $redis->connection_timeout( -5 );
    },
    qr/Connection timeout must be a positive number/,
    'invalid connection timeout (negative number; accessor)'
  );

  return;
}

####
sub t_read_timeout {
  like(
    exception {
      my $redis = AnyEvent::Redis::RipeRedis->new(
        read_timeout => 'invalid',
      );
    },
    qr/Read timeout must be a positive number/,
    'invalid read timeout (character string; constructor)',
  );

  like(
    exception {
      my $redis = AnyEvent::Redis::RipeRedis->new(
        read_timeout => -5,
      );
    },
    qr/Read timeout must be a positive number/,
    'invalid read timeout (negative number; constructor)',
  );

  my $redis = AnyEvent::Redis::RipeRedis->new();

  like(
    exception {
      $redis->read_timeout( 'invalid' );
    },
    qr/Read timeout must be a positive number/,
    'invalid read timeout (character string; accessor)',
  );

  like(
    exception {
      $redis->read_timeout( -5 );
    },
    qr/Read timeout must be a positive number/,
    'invalid read timeout (negative number; accessor)',
  );

  return;
}

####
sub t_encoding {
  like(
    exception {
      my $redis = AnyEvent::Redis::RipeRedis->new(
        encoding => 'utf88',
      );
    },
    qr/Encoding 'utf88' not found/,
    'invalid encoding (constructor)',
  );

  my $redis = AnyEvent::Redis::RipeRedis->new();

  like(
    exception {
      $redis->encoding( 'utf88' );
    },
    qr/Encoding 'utf88' not found/,
    'invalid encoding (accessor)',
  );

  return;
}

####
sub t_on_message {
  my $redis = AnyEvent::Redis::RipeRedis->new();

  like(
    exception {
      $redis->subscribe( 'channel' );
    },
    qr/'on_message' callback must be specified/,
    '\'on_message\' callback not specified',
  );

  return;
}
