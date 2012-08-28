package Test::AnyEvent::RedisHandle;

use 5.006000;
use strict;
use warnings;

our $VERSION = '0.101000';

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

    $mock->{on_prepare} = $params{on_prepare};
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

    $mock->{_start_timer} = AnyEvent->timer(
      after => 0,
      cb => sub {
        $mock->_connect();

        undef( $mock->{_start} );
      },
    );

    $mock->{_process_timer} = undef;

    return $mock;
  },
);

####
$mock->mock( 'push_write', sub {
  my $self = shift;
  my $cmd_szd = shift;

  push( @{$self->{_write_queue}}, $cmd_szd );

  return;
} );

####
$mock->mock( 'unshift_read', sub {
  my $self = shift;
  my $cb = shift;

  unshift( @{$self->{_read_queue}}, $cb );
  $self->{_continue_read} = 1;

  return;
} );

####
####
$mock->mock( 'destroy', sub {
  my $self = shift;

  undef( $self->{rbuf} );
  undef( $mock->{_redis_emu} );
  $mock->{_write_queue} = [];
  $mock->{_read_queue} = [];
  undef( $mock->{_continue_read} );
  undef( $mock->{_curr_on_read} );
  undef( $mock->{_start} );
  undef( $mock->{_process_timer} );

  return;
} );

####
$mock->mock( '_connect', sub {
  my $self = shift;

  if ( $REDIS_IS_DOWN || $CONN_IS_BROKEN ) {
    my $msg = 'Server not responding';
    if ( exists( $self->{on_connect_error} ) ) {
      $self->{on_connect_error}->( $self, $msg );
    }
    else {
      $self->{on_error}->( $self, $msg );
    }

    return;
  }

  $self->{_redis_emu} = Test::AnyEvent::RedisEmulator->new();
  if ( defined( $self->{on_prepare} ) ) {
    $self->{on_prepare}->();
  }
  if ( defined( $self->{on_connect} ) ) {
    $self->{on_connect}->();
  }

  $self->{_process_timer} = AnyEvent->timer(
    after => 0,
    interval => 0.001,
    cb => sub {
      if ( @{$self->{_write_queue}} ) {
        $self->_write();
      }
      if ( $self->{_continue_read} ) {
        $self->_read();
      }
      if (
        $REDIS_IS_DOWN
          && !@{$self->{_write_queue}}
          && !$self->{_continue_read}
          && $self->{_redis_emu}
          ) {
        undef( $self->{_redis_emu} );
        $self->{on_eof}->();
      }
    },
  );

  return;
} );

####
$mock->mock( '_write', sub {
  my $self = shift;

  if ( $REDIS_IS_DOWN || $CONN_IS_BROKEN ) {
    undef( $self->{_redis_emu} );
    $self->{on_error}->( $self, "Can't write to socket" );
    return;
  }

  my $cmd_szd = shift( @{$self->{_write_queue}} );
  $self->{rbuf} .= $self->{_redis_emu}->process_command( $cmd_szd );
  if ( !$self->{_continue_read} ) {
    $self->{_continue_read} = 1;
  }

  return;
} );

####
$mock->mock( '_read', sub {
  my $self = shift;

  if ( $REDIS_IS_DOWN || $CONN_IS_BROKEN ) {
    undef( $self->{_redis_emu} );
    $self->{on_error}->( $self, 'Error reading from socket' );
    return;
  }

  if ( @{$self->{_read_queue}} && !defined( $self->{_curr_on_read} ) ) {
    $self->{_curr_on_read} = shift( @{$self->{_read_queue}} );
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
  return;
}

####
sub redis_up {
  $REDIS_IS_DOWN = 0;
  $CONN_IS_BROKEN = 0;
  return;
}

####
sub break_connection {
  $CONN_IS_BROKEN = 1;
  return;
}

####
sub fix_connection {
  $CONN_IS_BROKEN = 0;
  return;
}

1;
