use 5.008000;
use strict;
use warnings;

use Test::More;
use Test::RedisServer;
use Net::EmptyPort qw( empty_port );
use AnyEvent;

####
sub run_redis_instance {
  my %params = @_;

  my $redis_server = eval {
    return Test::RedisServer->new(
      conf => \%params,
    );
  };
  if ( !defined( $redis_server ) ) {
    return;
  }

  my $host;
  my $port;
  my %conn_info = $redis_server->connect_info();
  if ( defined( $conn_info{server} ) ) {
    ( $host, $port ) = split( ':', $conn_info{server} );
  }
  elsif ( defined( $conn_info{sock} ) ) {
    $host = 'unix/';
    $port = $conn_info{sock};
  }
  else {
    BAIL_OUT( "Can't obtain connection info for redis-server" );
  }

  return {
    server => $redis_server,
    host => $host,
    port => $port,
    password => $params{requirepass},
  };
}

####
sub ev_loop {
  my $sub = shift;

  my $cv = AE::cv();

  $sub->( $cv );

  my $timer = AE::timer( 3, 0,
    sub {
      diag( 'Emergency exit from event loop. Test failed' );
      $cv->send();
    },
  );

  $cv->recv();
  undef( $timer );

  return;
}

####
sub get_redis_version {
  my $redis = shift;

  my $ver;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->info( {
        on_done => sub {
          my $data = shift;

          if ( $data =~ m/^redis_version:([0-9]+)\.([0-9]+)\.([0-9]+)/mo ) {
            my $prod_ver = 0;
            if ( defined( $1 ) and $1 ne '' ) {
              $prod_ver = $1;
            }
            my $major_ver = 0;
            if ( defined( $2 ) and $2 ne '' ) {
              $major_ver = $2;
            }
            my $minor_ver = 0;
            if ( defined( $3 ) and $3 ne '' ) {
              $minor_ver = $3;
            }
            $ver = $prod_ver + ( $major_ver * 10 ** -3 )
                + ( $minor_ver * 10 ** -6 );
          }
          $cv->send();
        }
      } );
    }
  );

  return $ver;
}

1;
