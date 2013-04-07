use 5.006000;
use strict;
use warnings;

use Test::More tests => 1;

my $T_CLASS;

BEGIN {
  $T_CLASS = 'AnyEvent::Redis::RipeRedis';
  use_ok( $T_CLASS );
}
