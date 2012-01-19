# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl AnyEvent-Redis.t'

#########################

use strict;
use warnings;

use Test::More tests => 4;

use_ok( 'AnyEvent::RipeRedis' );

# Test methods
can_ok( 'AnyEvent::RipeRedis', 'connect' );
can_ok( 'AnyEvent::RipeRedis', 'reconnect' );
can_ok( 'AnyEvent::RipeRedis', 'disconnect' );
