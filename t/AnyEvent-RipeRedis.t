# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl AnyEvent-Redis.t'

#########################

use strict;
use warnings;

use Test::More tests => 17;
use Test::MockObject;
use AnyEvent;

my $mock;

BEGIN {
  $mock = Test::MockObject->new();

  $mock->fake_module( 'AnyEvent::Handle',
    new => sub {     
      shift;
      
      my %opts = @_;

      foreach my $opt_name ( keys %opts ) {
        $mock->{ $opt_name } = $opts{ $opt_name };
      }
      
      my $timer;

      $timer = AnyEvent->timer( 
        after => 0, 
        cb => sub { 
          $mock->{ 'connected' } = 1;
          $mock->{ 'on_connect' }->();

          undef( $timer );
        } 
      );
      
      $mock->{ 'read_queue' } = [];

      $mock->{ 'resp_data' } = [
        '+OK',
        '+OK',
        '+QUEUED',
        '+QUEUED',
        '+QUEUED',

        '*3',
        '+OK',
        '$3',
        'bar',
        '*2',
        '$3',
        'foo',
        '$3',
        'bar',
        
        ':10',
        '+PONG',
      ];

      $mock->{ 'destroyed' } = 0;

      return $mock;
    },

    push_write => sub {
      return 1;  
    },
  );

  # Creating fake methods
  
  ####
  $mock->set_true( qw( 
    push_write
  ) );
  
  ####
  $mock->mock( 'push_read', sub { 
    my $self = shift;
    my @params = @_;

    push( @{ $self->{ 'read_queue' } }, \@params );
  } );
  
  ####
  $mock->mock( 'unshift_read', sub { 
    my $self = shift;
    my @params = @_;

    unshift( @{ $self->{ 'read_queue' } }, \@params );
  } );

  ####
  $mock->mock( 'destroy', sub {
    my $self = shift;

    $self->{ 'destroyed' } = 1;

    return 1;  
  } );

  ####
  $mock->mock( 'destroyed', sub {
    my $self = shift;

    return $self->{ 'destroyed' };
  } );

  use_ok( 'AnyEvent::RipeRedis' );
}

can_ok( 'AnyEvent::RipeRedis', 'new' );
can_ok( 'AnyEvent::RipeRedis', '_connect' );
can_ok( 'AnyEvent::RipeRedis', '_post_connect' );
can_ok( 'AnyEvent::RipeRedis', '_error' );
can_ok( 'AnyEvent::RipeRedis', '_execute_command' );
can_ok( 'AnyEvent::RipeRedis', '_read_response' );

my $cv = AnyEvent->condvar();

my $proc_read_timer;

$proc_read_timer = AnyEvent->timer( 
  after => 0,
  interval => 0.1,
  cb => sub {

    if ( $mock->{ 'connected' } ) {

      while ( my $params = shift( @{ $mock->{ 'read_queue' } } ) ) {
        my $type = $params->[ 0 ];
        
        my $cb;

        if ( $type eq 'line' ) {
          $cb = $params->[ 1 ];
        }
        elsif ( $type eq 'chunk' ) {
          $cb = $params->[ 2 ];
        }
        
        my $str = shift( @{ $mock->{ 'resp_data' } } );

        $cb->( $mock, $str );
      }
    }
    
    return 1;
  }
);

my $r;

$r = new_ok( 'AnyEvent::RipeRedis' => [ { 
  host => 'localhost',
  port => '6379',
  password => 'dsk2344GHjK82#sd',
  encoding => 'utf8',
  reconnect => 1,
  reconnect_after => '1',
  max_reconnect_attempts => '10',

  on_connect => sub { 
    my $attempt = shift;
    
    is( $attempt, 1, 'Connected' );

    $r->ping( { 
      retry => 0,      
      cb => sub { 
        my $resp = shift;

        is( $resp, 'PONG', 'PING' );

        return 1;
      }
    } );

    return 1;
  },
  
  on_auth => sub {
    my $resp = shift;

    is( $resp, 'OK', 'AUTH' );

    return 1;
  },

  on_stop_reconnect => sub {
    diag( "Stop reconnect\n" );
  },
  
  on_error => sub {
    my $msg = shift;
    my $err_type = shift;
    
    diag( "Error [$err_type]: $msg\n" );

    return 1;
  }
} ] );


$r->multi( { 
  cb => sub {
    my $resp = shift;

    is( $resp, 'OK', 'MULTI' );

    return 1;
  }
} );

$r->set( 'foo', 'bar', {
  cb => sub {
    my $resp = shift;

    is( $resp, 'QUEUED', 'SET' );

    return 1;
  }
} );

$r->get( 'foo', {
  cb => sub {
    my $resp = shift;

    is( $resp, 'QUEUED', 'GET' );
 
    return 1;
  }
} );

$r->lrange( 'list', 0, -1, { 
  cb => sub {
    my $resp = shift;

    is( $resp, 'QUEUED', 'LRANGE' );

    return 1;
  }
} );

$r->exec( { 
  cb => sub {
    my $data = shift;
    
    is_deeply( $data, [ 'OK', 'bar', [ qw( foo bar ) ] ], 'EXEC' );

    return 1;
  }
} );

$r->incr( 'counter', { 
  cb => sub {
    my $val = shift;

    is( $val, '10', 'INCR' );

    undef( $proc_read_timer );
    $cv->send();

    return 1;
  }
} );

# TODO 
# Test on_read method
# Test error handling and reconnect feature

$cv->recv();
