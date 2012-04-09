package Test::AnyEvent::EVLoop;

use 5.006000;
use strict;
use warnings;
use base 'Exporter';

use Test::More;

our @EXPORT = qw( ev_loop );

sub ev_loop {
  my $cv = shift;

  my $timer;
  $timer = AnyEvent->timer(
    after => 5,
    cb => sub {
      diag( 'Emergency exit from event loop. Test failed' );
      $cv->send();
    },
  );
  $cv->recv();
  undef( $timer );

  return;
}

1;
