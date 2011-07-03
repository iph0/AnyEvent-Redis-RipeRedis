# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl AnyEvent-Redis.t'

#########################

use strict;
use warnings;

use Test::More tests => 23;
use Test::MockObject;
use AnyEvent;
use Scalar::Util 'looks_like_number';

my $RESP_DATA = [
  '+OK',
  '+OK',
  ':10',

  '$3',
  'bar',

  '*2',
  '$3',
  'foo',
  '$3',
  'bar',
  
  "-ERR unknown command 'some'",

  '+OK',
  '+QUEUED',
  '+QUEUED',
  '+QUEUED',
  '+QUEUED',
  
  "-ERR unknown command 'some'",

  '*4',
  '+OK',
  ':11',
  '$3',
  'bar',
  '*2',
  '$3',
  'foo',
  '$3',
  'bar',
  
  '+PONG',

  '*3',
  '$9',
  'subscribe',
  '$3',
  'ch1',
  ':1',
  '*3',
  '$9',
  'subscribe',
  '$3',
  'ch2',
  ':2',
  '*3',
  '$9',
  'subscribe',
  '$3',
  'ch3',
  ':3',
];

my $mock;

BEGIN {
  # Create mock object for AnyEvent::Handle
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
      
      $mock->{ 'on_read' } = undef;
      $mock->{ 'read_queue' } = [];
      $mock->{ 'destroyed' } = 0;

      return $mock;
    }
  );

  $mock->set_true( qw( 
    push_write
  ) );
  
  $mock->mock( 'on_read', sub {
    my $self = shift;
    my $cb = shift;

    $self->{ 'on_read' } = $cb;
    
    return 1;
  } );

  $mock->mock( 'push_read', sub { 
    my $self = shift;
    my @params = @_;

    push( @{ $self->{ 'read_queue' } }, \@params );

    return 1;
  } );
  
  $mock->mock( 'unshift_read', sub { 
    my $self = shift;
    my @params = @_;

    unshift( @{ $self->{ 'read_queue' } }, \@params );

    return 1;
  } );

  $mock->mock( 'destroy', sub {
    my $self = shift;

    $self->{ 'destroyed' } = 1;

    return 1;  
  } );

  $mock->mock( 'destroyed', sub {
    my $self = shift;

    return $self->{ 'destroyed' };
  } );

  # Test load
  use_ok( 'AnyEvent::RipeRedis' );
}

# Test methods
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

      if ( @{ $mock->{ 'read_queue' } } ) {
        
        while ( @{ $mock->{ 'read_queue' } } ) {
          my $read_op = $mock->{ 'read_queue' }->[ 0 ];
          my $type = $read_op->[ 0 ];
          
          my $cb;

          if ( $type eq 'line' ) {
            $cb = $read_op->[ 1 ];
          }
          elsif ( $type eq 'chunk' ) {
            $cb = $read_op->[ 2 ];
          }

          if ( my $data = shift( @{ $RESP_DATA } ) ) {
            shift( @{ $mock->{ 'read_queue' } } );
            $cb->( $mock, $data );
          }
        }
      }
      elsif ( defined( $mock->{ 'on_read' } ) && @{ $RESP_DATA } ) {
        $mock->{ 'on_read' }->();
      }
    }
    
    return 1;
  }
);


# Test constructor

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


    # Subscription
    
#    $r->subscribe( 'ch1', 'ch2', 'ch3', { 
#      cb => sub {
#        my $data = shift;
#
#        use Data::Dumper;
#        print Dumper( $data );
#
#        return 1;
#      }
#    } );

#    $r->unsubscribe( 'ch1', 'ch2', 'ch3', { 
#      cb => sub {
#        my $data = shift;
#
#        use Data::Dumper;
#        print Dumper( $data );
#        undef( $proc_read_timer );
#        $cv->send();
#
#        return 1;
#      }
#    } );

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
    
    is( defined( $msg ) && defined( $err_type ), 1, 'on_error' );

    return 1;
  }
} ] );


# Test command methods

$r->set( 'foo', 'bar', {
  cb => sub {
    my $resp = shift;

    is( $resp, 'OK', 'SET' );

    return 1;
  }
} );

$r->incr( 'counter', { 
  cb => sub {
    my $val = shift;

    is( $val, 10, 'INCR' );

    return 1;
  }
} );

$r->get( 'foo', {
  cb => sub {
    my $resp = shift;

    is( $resp, 'bar', 'GET' );
 
    return 1;
  }
} );

$r->lrange( 'list', 0, -1, { 
  cb => sub {
    my $resp = shift;
    
    is_deeply( $resp, [ qw( foo bar ) ], 'LRANGE' );

    return 1;
  }
} );

# Unknown command
$r->some( { 
  cb => sub {
    my $resp = shift;
    
    return 1;
  }
} );


# Test transaction

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

$r->incr( 'counter', { 
  cb => sub {
    my $val = shift;

    is( $val, 'QUEUED', 'INCR' );

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

# Unknown command
$r->some( { 
  cb => sub {
    my $resp = shift;
    
    return 1;
  }
} );

$r->exec( { 
  cb => sub {
    my $data = shift;
    
    is_deeply( $data, [ 'OK', 11, 'bar', [ qw( foo bar ) ] ], 'EXEC' );
    
    return 1;
  }
} );

# TODO 
# Test on_read method
# Test error handling and reconnect feature

$cv->recv();
