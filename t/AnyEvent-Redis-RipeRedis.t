# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl AnyEvent-Redis-RipeRedis.t'

use strict;
use warnings;

use Test::More tests => 5;
use Test::MockObject;

my $t_class;

BEGIN {
  my $mock = Test::MockObject->new( {} );

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

      return $mock;
    }
  );

  $mock->set_true( 'push_write' );

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
        
  },
  
  on_stop_reconnect => sub {

  },
  
  on_error => sub {
    my $msg = shift;
    my $err_code = shift;
    
  }
} );

isa_ok( $redis, $t_class );
