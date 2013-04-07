use 5.006000;
use strict;
use warnings;

use Test::More tests => 23;

my $T_CLASS;

BEGIN {
  $T_CLASS = 'AnyEvent::Redis::RipeRedis';
  eval "use $T_CLASS qw( :err_codes )";
}

# Constructor
can_ok( $T_CLASS, 'new' );

# Accessors
can_ok( $T_CLASS, 'connection_timeout' );
can_ok( $T_CLASS, 'read_timeout' );
can_ok( $T_CLASS, 'reconnect' );
can_ok( $T_CLASS, 'encoding' );
can_ok( $T_CLASS, 'on_connect' );
can_ok( $T_CLASS, 'on_disconnect' );
can_ok( $T_CLASS, 'on_connect_error' );
can_ok( $T_CLASS, 'on_error' );

# Other methods
can_ok( $T_CLASS, 'eval_cached' );
can_ok( $T_CLASS, 'disconnect' );

# Constants
is( E_CANT_CONN, 1, 'E_CANT_CONN' );
is( E_LOADING_DATASET, 2, 'E_LOADING_DATASET' );
is( E_IO, 3, 'E_IO' );
is( E_CONN_CLOSED_BY_REMOTE_HOST, 4, 'E_CONN_CLOSED_BY_REMOTE_HOST' );
is( E_CONN_CLOSED_BY_CLIENT, 5, 'E_CONN_CLOSED_BY_CLIENT' );
is( E_NO_CONN, 6, 'E_NO_CONN' );
is( E_INVALID_PASS, 7, 'E_INVALID_PASS' );
is( E_OPRN_NOT_PERMITTED, 8, 'E_OPRN_NOT_PERMITTED' );
is( E_OPRN_ERROR, 9, 'E_OPRN_ERROR' );
is( E_UNEXPECTED_DATA, 10, 'E_UNEXPECTED_DATA' );
is( E_NO_SCRIPT, 11, 'E_NO_SCRIPT' );
is( E_READ_TIMEDOUT, 12, 'E_RESP_TIMEDOUT' );
