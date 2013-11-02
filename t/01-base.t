use 5.008000;
use strict;
use warnings;

use Test::More tests => 24;

my $T_CLI_CLASS;

BEGIN {
  $T_CLI_CLASS = 'AnyEvent::Redis::RipeRedis';
  eval "use $T_CLI_CLASS qw( :err_codes )";
}

# Constructor
can_ok( $T_CLI_CLASS, 'new' );

# Accessors
can_ok( $T_CLI_CLASS, 'connection_timeout' );
can_ok( $T_CLI_CLASS, 'read_timeout' );
can_ok( $T_CLI_CLASS, 'reconnect' );
can_ok( $T_CLI_CLASS, 'encoding' );
can_ok( $T_CLI_CLASS, 'on_connect' );
can_ok( $T_CLI_CLASS, 'on_disconnect' );
can_ok( $T_CLI_CLASS, 'on_connect_error' );
can_ok( $T_CLI_CLASS, 'on_error' );

# Other methods
my $redis = $T_CLI_CLASS->new();
can_ok( $redis, 'eval_cached' );
can_ok( $redis, 'disconnect' );

# Constants
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


# Error object class

my $T_ERR_CLASS = 'AnyEvent::Redis::RipeRedis::Error';

# Constructor
can_ok( $T_ERR_CLASS, 'new' );

# Getters
my $t_err_obj = $T_ERR_CLASS->new(
  message => 'ERR something happened',
  code => E_OPRN_ERROR,
);
can_ok( $t_err_obj, 'code' );
can_ok( $t_err_obj, 'message' );
