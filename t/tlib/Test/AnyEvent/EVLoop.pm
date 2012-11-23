package Test::AnyEvent::EVLoop;

use 5.006000;
use strict;
use warnings;
use base 'Exporter';

use Test::More;
use AnyEvent

our @EXPORT = qw( ev_loop );

sub ev_loop {
  my $sub = shift;

  my $cv = AnyEvent->condvar();

  $sub->( $cv );

  my $timer;
  $timer = AnyEvent->timer(
    after => 3,
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
