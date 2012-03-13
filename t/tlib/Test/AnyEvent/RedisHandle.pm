package Test::AnyEvent::RedisHandle;

use strict;
use warnings;

our $VERSION = '0.0300000';

use Test::MockObject;
use Test::AnyEvent::RedisEmulator;
use AnyEvent;

my $REDIS_IS_DOWN = 0;
my $CONN_IS_BROKEN = 0;


# Create mock object

my $mock = Test::MockObject->new( {} );

####
$mock->fake_module(
  'AnyEvent::Handle',

  new => sub {
    my $proto = shift;
    my %params = @_;

    $mock->{on_connect} = $params{on_connect};
    $mock->{on_connect_error} = $params{on_connect_error};
    $mock->{on_error} = $params{on_error};
    $mock->{on_eof} = $params{on_eof};
    $mock->{on_read} = $params{on_read};

    $mock->{rbuf} = '';

    $mock->{_redis_emu} = undef;

    $mock->{_write_queue} = [];
    $mock->{_read_queue} = [];
    $mock->{_continue_read} = undef;
    $mock->{_curr_on_read} = undef;
    $mock->{_destroyed} = undef;

    $mock->{_start} = AnyEvent->timer(
      after => 0,
      cb => sub {
        $mock->_connect();

        undef( $mock->{_start} );
      }
    );

    return $mock;
  }
);

####
$mock->mock( 'push_write', sub {
  my $self = shift;
  my $cmd_szd = shift;

  push( @{ $self->{_write_queue} }, $cmd_szd );

  return;
} );

####
$mock->mock( 'unshift_read', sub {
  my $self = shift;
  my $cb = shift;

  unshift( @{ $self->{_read_queue} }, $cb );

  $self->{_continue_read} = 1;

  return;
} );

####
$mock->mock( 'destroyed', sub {
  my $self = shift;

  return $self->{_destroyed};
} );

####
$mock->mock( 'destroy', sub {
  my $self = shift;

  $self->{_destroyed} = undef;

  return;
} );

####
$mock->mock( '_connect', sub {
  my $self = shift;

  if ( $REDIS_IS_DOWN || $CONN_IS_BROKEN ) {
    my $msg = 'Some connection error';

    if ( exists( $self->{on_connect_error} ) ) {
      $self->{on_connect_error}->( $self, $msg );
    }
    else {
      $self->{on_error}->( $self, $msg );
    }

    return;
  }

  $self->{_redis_emu} = Test::AnyEvent::RedisEmulator->new();

  $self->{on_connect}->();

  my $after = 0;
  my $interval = 0.001;

  $self->{_write_timer} = AnyEvent->timer(
    after => $after,
    interval => $interval,

    cb => sub {

      if ( @{ $self->{_write_queue} } ) {
        $self->_write();
      }
    }
  );

  $self->{_read_timer} = AnyEvent->timer(
    after => $after,
    interval => $interval,

    cb => sub {

      if ( $self->{_continue_read} ) {
        $self->_read();
      }
    }
  );

  return;
} );

####
$mock->mock( '_write', sub {
  my $self = shift;

  if ( !defined( $self->{_redis_emu} ) ) {
    return;
  }

  if ( $REDIS_IS_DOWN || $CONN_IS_BROKEN ) {
    $self->{on_error}->( $self, 'Some error on socket' );

    return;
  }

  my $cmd_szd = shift( @{ $self->{_write_queue} } );
  my $resp_str = $self->{_redis_emu}->process_command( $cmd_szd );
  $self->{rbuf} .= $resp_str;

  if ( !$self->{_continue_read} ) {
    $self->{_continue_read} = 1;
  }

  return;
} );

####
$mock->mock( '_read', sub {
  my $self = shift;

  if ( @{ $self->{_read_queue} } && !defined( $self->{_curr_on_read} ) ) {
    $self->{_curr_on_read} = shift( @{ $self->{_read_queue} } );
  }

  if ( defined( $self->{_curr_on_read} ) ) {

    if ( !$self->{_curr_on_read}->( $self ) ) {
      $self->{_continue_read} = undef;

      return;
    }

    $self->{_curr_on_read} = undef;
  }
  else {
    $self->{_continue_read} = undef;

    $self->{on_read}->( $self );
  }

  return;
} );


# Public methods

####
sub redis_down {
  $REDIS_IS_DOWN = 1;
}

####
sub redis_up {
  $REDIS_IS_DOWN = 0;
}

####
sub break_connection {
  $CONN_IS_BROKEN = 1;
}

####
sub restore_connection {
  $CONN_IS_BROKEN = 0;
}

1;
