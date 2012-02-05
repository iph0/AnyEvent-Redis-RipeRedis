# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl AnyEvent-Redis-RipeRedis.t'

use 5.010000;
use strict;
use warnings;

use Test::More tests => 25;
use Test::MockObject;
use AnyEvent;

my $t_class;

BEGIN {
  my $EOL = "\r\n";
  my $EOL_LEN = length( $EOL );
  my $STORAGE = {};

  my $mock = Test::MockObject->new( {} );

  ####
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
      $mock->{ 'new_in_rbuf' } = undef;
      $mock->{ 'read_queue' } = [];

      $mock->{ 'error_msgs' } = {
        wrong_args => "wrong number of arguments for '\%c' command",
        unknown_command => "unknown command '\%c'",
        not_integer => 'value is not an integer or out of range',
        wrong_value => 'Operation against a key holding the wrong kind of value'
      };

      $mock->{ 'start_timer' } = AnyEvent->timer(
        after => 0,
        cb => sub {
          $mock->_connect();

          undef( $mock->{ 'start_timer' } );
        }
      );
      
      $mock->{ 'destroyed' } = undef;
      
      return $mock;
    }
  );

  ####
  $mock->mock( 'push_write', sub {
    my $self = shift;
    my $cmd_str = shift;

    my $cmd = _parse_cmd( $cmd_str );

    ok( defined( $cmd ), 'Command serialization' );

    my $method = "_$cmd->{ 'name' }";

    if ( $self->can( $method ) ) {
      $self->$method( $cmd );
    }
    else {
      my $err_msg = $self->{ 'error_msgs' }->{ 'unknown_command' };
      $err_msg =~ s/%c/$cmd->{ 'name' }/go;

      $self->{ 'rbuf' } .= "-ERR $err_msg$EOL";
    }

    $mock->{ 'new_in_rbuf' } = 1;
  } );

  ####
  $mock->mock( 'unshift_read', sub {
    my $self = shift;
    my $cb = shift;

    unshift( @{ $self->{ 'read_queue' } }, $cb );
  } );

  ####
  $mock->mock( 'destroyed', sub {
    my $self = shift;

    return $self->{ 'destroyed' };
  } );

  ####
  $mock->mock( 'destroy', sub {
    my $self = shift;

    $self->{ 'destroyed' } = undef;
  } );

  ####
  $mock->mock( '_connect', sub {
    my $self = shift;

    $self->{ 'on_connect' }->();

    $self->{ 'proc_timer' } = AnyEvent->timer(
      after => 0,
      interval => 0.01,
      cb => sub {

        if ( $self->{ 'new_in_rbuf' } ) {

          while ( @{ $self->{ 'read_queue' } } ) {
            my $on_read_cb = $self->{ 'read_queue' }->[ 0 ];

            if ( !$on_read_cb->( $self ) ) {
              return;
            }

            shift( @{ $self->{ 'read_queue' } } );
          }

          $self->{ 'on_read' }->( $self );
        }
      }
    );
  } );

  ####
  $mock->mock( '_auth', sub {
    my $self = shift;
    my $cmd = shift;

    my $pass = $cmd->{ 'args' }->[ 0 ];

    if ( !defined( $pass ) ) {
      my $err_msg = $self->{ 'error_msgs' }->{ 'wrong_args' };
      $err_msg =~ s/%c/$cmd->{ 'name' }/go;

      $self->{ 'rbuf' } .= "-ERR $err_msg$EOL";

      return;
    }

    $self->{ 'rbuf' } .= "+OK$EOL";
  } );

  ####
  $mock->mock( '_ping', sub {
    my $self = shift;

    $self->{ 'rbuf' } .= "+PONG$EOL";
  } );

  ####
  $mock->mock( '_incr', sub {
    my $self = shift;
    my $cmd = shift;

    my $key = $cmd->{ 'args' }->[ 0 ];

    if ( !defined( $key ) ) {
      my $err_msg = $self->{ 'error_msgs' }->{ 'wrong_args' };
      $err_msg =~ s/%c/$cmd->{ 'name' }/go;

      $self->{ 'rbuf' } .= "-ERR $err_msg$EOL";

      return;
    }

    if ( !defined( $STORAGE->{ $key } ) ) {
      $STORAGE->{ $key } = 0;
    }

    ++$STORAGE->{ $key };

    $self->{ 'rbuf' } .= ":$STORAGE->{ $key }$EOL";
  } );

  ####
  $mock->mock( '_set', sub {
    my $self = shift;
    my $cmd = shift;

    my $key = $cmd->{ 'args' }->[ 0 ];
    my $val = $cmd->{ 'args' }->[ 1 ];

    if ( !defined( $key ) || !defined( $val ) ) {
      my $err_msg = $self->{ 'error_msgs' }->{ 'wrong_args' };
      $err_msg =~ s/%c/$cmd->{ 'name' }/go;

      $self->{ 'rbuf' } .= "-ERR $err_msg$EOL";

      return;
    }

    $STORAGE->{ $key } = $val;

    $self->{ 'rbuf' } .= "+OK$EOL";
  } );

  ####
  $mock->mock( '_get', sub {
    my $self = shift;
    my $cmd = shift;

    my $key = $cmd->{ 'args' }->[ 0 ];

    if ( !defined( $key ) ) {
      my $err_msg = $self->{ 'error_msgs' }->{ 'wrong_args' };
      $err_msg =~ s/%c/$cmd->{ 'name' }/go;

      $self->{ 'rbuf' } .= "-ERR $err_msg$EOL";

      return;
    }

    my $val = $STORAGE->{ $key };

    if ( defined( $val ) ) {
      my $bulk_len = length( $val );

      $self->{ 'rbuf' } .= "\$$bulk_len$EOL$val$EOL";
    }
    else {
      $self->{ 'rbuf' } .= "\$0$EOL";
    }
  } );

  ####
  $mock->mock( '_rpush', sub {
    my $self = shift;
    my $cmd = shift;

    my $key = $cmd->{ 'args' }->[ 0 ];
    my $val = $cmd->{ 'args' }->[ 1 ];

    if ( !defined( $key ) || !defined( $val ) ) {
      my $err_msg = $self->{ 'error_msgs' }->{ 'wrong_args' };
      $err_msg =~ s/%c/$cmd->{ 'name' }/go;

      $self->{ 'rbuf' } .= "-ERR $err_msg$EOL";

      return;
    }

    if ( !defined( $STORAGE->{ $key } ) ) {
      $STORAGE->{ $key } = [];
    }

    push( @{ $STORAGE->{ $key } }, $val );

    $self->{ 'rbuf' } .= "+OK$EOL";
  } );

  ####
  $mock->mock( '_lrange', sub {
    my $self = shift;
    my $cmd = shift;

    my $key = $cmd->{ 'args' }->[ 0 ];
    my $start = $cmd->{ 'args' }->[ 1 ];
    my $stop = $cmd->{ 'args' }->[ 2 ];

    if ( !defined( $key ) || !defined( $start ) || !defined( $stop ) ) {
      my $err_msg = $self->{ 'error_msgs' }->{ 'wrong_args' };
      $err_msg =~ s/%c/$cmd->{ 'name' }/go;

      $self->{ 'rbuf' } .= "-ERR $err_msg$EOL";

      return;
    }

    if ( $stop < 0 ) {
      $stop = scalar( @{ $STORAGE->{ $key } } ) + $stop;
    }

    my @list = @{ $STORAGE->{ $key } }[ $start .. $stop ];
    my $mbulk_len = scalar( @list );

    $self->{ 'rbuf' } .= "*$mbulk_len$EOL";

    foreach my $val ( @list ) {
      my $bulk_len = length( $val );

      $self->{ 'rbuf' } .= "\$$bulk_len$EOL$val$EOL";
    }
  } );

  ####
  $mock->mock( '_quit', sub {
    my $self = shift;

    $self->{ 'rbuf' } .= "+OK$EOL";
  } );

  ####
  sub _parse_cmd {
    my $cmd_str = shift;

    if ( !defined( $cmd_str ) || $cmd_str eq '' ) {
      return;
    }

    my $eol_pos = index( $cmd_str, $EOL );

    if ( $eol_pos <= 0 ) {
      return;
    }

    local $/ = $EOL;

    my $tkn = substr( $cmd_str, 0, $eol_pos + $EOL_LEN, '' );
    my $type = substr( $tkn, 0, 1, '' );
    chomp( $tkn );

    if ( $type ne '*' ) {
      return;
    }

    my $mbulk_len = $tkn;

    if ( $mbulk_len =~ m/\D/o || $mbulk_len == 0 ) {
      return;
    }

    my $args_remaining = $mbulk_len;

    my @args;
    my $bulk_eol_len;

    while ( $args_remaining ) {

      if ( $bulk_eol_len ) {
        my $arg = substr( $cmd_str, 0, $bulk_eol_len, '' );
        chomp( $arg );

        push( @args, $arg );

        undef( $bulk_eol_len );
        --$args_remaining;
      }
      else {
        my $eol_pos = index( $cmd_str, $EOL );

        if ( $eol_pos <= 0 ) {
          return;
        }

        my $tkn = substr( $cmd_str, 0, $eol_pos + $EOL_LEN, '' );
        my $type = substr( $tkn, 0, 1, '' );
        chomp( $tkn );

        if ( $type ne '$' ) {
          return;
        }

        $bulk_eol_len = $tkn + $EOL_LEN;
      }
    }

    my $cmd = {
      name => shift( @args ),
      args => \@args
    };

    return $cmd;
  }


  $t_class = 'AnyEvent::Redis::RipeRedis';

  use_ok( $t_class );
}

can_ok( $t_class, 'new' );
can_ok( $t_class, 'AUTOLOAD' );
can_ok( $t_class, 'DESTROY' );

my $cv = AnyEvent->condvar();

my $redis;

$redis = $t_class->new( {
  host => 'localhost',
  port => '6379',
  password => 'test_password',
  encoding => 'utf8',
  reconnect => 1,
  reconnect_after => 1,
  max_connect_attempts => 10,

  on_connect => sub {
    my $attempt = shift;

    is( $attempt, 1, 'on_connect' );

    $redis->ping( sub {
      my $resp = shift;

      is( $resp, 'PONG', 'ping (status reply)' );
    } );

    # Disconnect
    $redis->quit( sub {
      my $resp = shift;

      is( $resp, 'OK', 'quit (status reply)' );

      $cv->send();
    } );
  },

  on_stop_reconnect => sub {
    # TODO
  },

  on_redis_error => sub {
    my $msg = shift;

    ok( $msg =~ m/^ERR/o, $msg );
  },

  on_error => sub {
    my $msg = shift;

    # TODO
  }
} );

isa_ok( $redis, $t_class );

# Increment
$redis->incr( 'foo', sub {
  my $val = shift;

  is( $val, 1, 'incr (numeric reply)' );
} );


# Set value
$redis->set( 'bar', 'Some string', sub {
  my $resp = shift;

  is( $resp, 'OK', 'set (status reply)' );
} );


# Get value
$redis->get( 'bar', sub {
  my $val = shift;

  is( $val, 'Some string', 'get (bulk reply)' );
} );


# Push values
for ( my $i = 1; $i <= 3; $i++ ) {
  $redis->rpush( 'list', "element_$i", sub {
    my $resp = shift;

    is( $resp, 'OK', 'rpush (status reply)' );
  } );
}

# Get list of values
$redis->lrange( 'list', 0, -1, sub {
  my $list = shift;

  my $expected = [ qw(
    element_1
    element_2
    element_3
  ) ];

  is_deeply( $list, $expected, 'lrange (multi-bulk reply)' );
} );

$cv->recv();
