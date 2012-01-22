# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl AnyEvent-Redis-RipeRedis.t'

#########################

use strict;
use warnings;

use Test::More tests => 1;

my $t_class = 'AnyEvent::Redis::RipeRedis';

use_ok( $t_class );
