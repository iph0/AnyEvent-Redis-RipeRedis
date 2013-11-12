use 5.008000;
use strict;
use warnings;

use Test::More tests => 10;
use AnyEvent::Redis::RipeRedis qw( :err_codes );

is( E_CANT_CONN, 1, 'E_CANT_CONN' );
is( E_LOADING_DATASET, 2, 'E_LOADING_DATASET' );
is( E_IO, 3, 'E_IO' );
is( E_CONN_CLOSED_BY_REMOTE_HOST, 4, 'E_CONN_CLOSED_BY_REMOTE_HOST' );
is( E_CONN_CLOSED_BY_CLIENT, 5, 'E_CONN_CLOSED_BY_CLIENT' );
is( E_NO_CONN, 6, 'E_NO_CONN' );
is( E_OPRN_ERROR, 9, 'E_OPRN_ERROR' );
is( E_UNEXPECTED_DATA, 10, 'E_UNEXPECTED_DATA' );
is( E_NO_SCRIPT, 11, 'E_NO_SCRIPT' );
is( E_READ_TIMEDOUT, 12, 'E_RESP_TIMEDOUT' );
