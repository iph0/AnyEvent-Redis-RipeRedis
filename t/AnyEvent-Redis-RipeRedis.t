# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl AnyEvent-Redis-RipeRedis.t'

use strict;
use warnings;

use Test::More tests => 5;
use Test::MockObject;
use AnyEvent;

my $t_class;
my $mock;

BEGIN {
  $mock = Test::MockObject->new( {} );

  $mock->fake_module(
    'AnyEvent::Handle',

    new => sub {
      my $proto = shift;
      my %params = @_;
      
      $mock->{ 'on_connect' } = $params{ 'on_connect' };
      $mock->{ 'on_connect_error' } = $params{ 'on_connect_error' };
      $mock->{ 'on_error' } = $params{ 'on_error' };
      $mock->{ 'on_eof' } = $params{ 'on_eof' };
      $mock->{ 'on_read' } = $params{ 'on_read' };

      $mock->{ 'rbuf' } = undef;
      $mock->{ 'read_queue' } = [];
      $mock->{ 'new_data_received' } = undef;
      
      $mock->{ '$proc_timer' } = AnyEvent->timer(
        after => 0,
        cb => sub {
          $mock->{ 'rbuf' } = <<DATA
+OK\r
+OK\r
:1234\r
-ERR unknown command 'incrr'\r
\$4\r
Test\r
*3\r
\$3\r
foo\r
\$3\r
bar\r
\$3\r
coo\r
DATA
;
          $mock->{ 'new_data_received' } = 1;

          $mock->_process();
          
          return;
        }
      );
      
      return $mock;
    }
  );

  $mock->set_true( 'push_write' );

  $mock->mock( 'unshift_read', sub {
    my $self = shift;
    my $cb = shift;
    
    unshift( @{ $self->{ 'read_queue' } }, $cb );
  } );

  $mock->mock( '_process', sub {
    my $self = shift;
    
    $self->{ 'on_connect' }->();
    
    if ( $self->{ 'new_data_received' } ) {
      $self->{ 'on_read' }->( $self );
    }

  } );

  $t_class = 'AnyEvent::Redis::RipeRedis';

  use_ok( $t_class );
}

can_ok( $t_class, 'new' );
can_ok( $t_class, 'AUTOLOAD' );
can_ok( $t_class, 'DESTROY' );

my $redis = $t_class->new( { 
  host => 'localhost',
  port => '6379',
  password => 'test_password',
  encoding => 'utf8',
  reconnect => 1,
  reconnect_after => 1,
  max_reconnect_attempts => 10,

  on_connect => sub { 
    my $attempt = shift;
    
    is( $attempt, 1, 'on_connect' );
  },
  
  on_stop_reconnect => sub {

  },
  
  on_error => sub {
    my $msg = shift;
    my $err_code = shift;
    
    is( $msg, "ERR unknown command 'incrr'", 'on_error' );
    is( $err_code, 4, 'Command error' );
  }
} );

isa_ok( $redis, $t_class );


my $cv = AnyEvent->condvar();

$redis->set( 'foo', 'Test', sub {
  my $data = shift;

  is( $data, 'OK', 'set (status reply)' );
} );

$redis->get( 'bar', sub {
  my $data = shift;

  is( $data, '1234', 'get (integer reply)' ); # Invalid example
} );

$redis->incrr( 'coo' );

$redis->get( 'foo', sub {
  my $data = shift;

  is( $data, 'Test', 'get (bulk reply)' );
} );

$redis->lrange( 'list', 0, -1, sub {
  my $data = shift;
  
  my $expected = [ qw( foo bar coo ) ];

  is_deeply( $data, $expected, 'lrange (multi-bulk reply)' );
} );


$cv->recv();
