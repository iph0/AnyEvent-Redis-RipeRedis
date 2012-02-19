package Test::AnyEvent::RedisHandle;

use strict;
use warnings;

use fields qw(
  storage
  is_auth
  transaction_began
  commands_queue
);

our $VERSION = '0.0200001';

use Test::MockObject;
use AnyEvent;

my $PASSWORD = 'test';

my %COMMANDS = (
  auth => {
    validate => *_validate_auth,
    exec => *_exec_auth
  },

  ping => {
    exec => *_exec_ping
  },

  incr => {
    validate => *_validate_incr,
    exec => *_exec_incr
  },

  set => {
    validate => *_validate_set,
    exec => *_exec_set
  },

  get => {
    validate => *_validate_get,
    exec => *_exec_get
  },

  rpush => {
    validate => *_validate_rpush,
    exec => *_exec_rpush
  },

  lpush => {
    validate => *_validate_lpush,
    exec => *_exec_lpush
  },

  lrange => {
    validate => *_validate_lrange,
    exec => *_exec_lrange
  },

  multi => {
    exec => *_exec_multi
  },

  exec => {
    exec => *_exec_exec
  },

  quit => {
    exec => *_exec_quit
  },
);

my %ERR_MESSAGES = (
  protocol_error => 'Protocol error',
  invalid_pass => 'invalid password',
  not_permitted => 'operation not permitted',
  wrong_args => "wrong number of arguments for '\%c' command",
  unknown_cmd => "unknown command '\%c'",
  not_integer => 'value is not an integer or out of range',
  wrong_value => 'Operation against a key holding the wrong kind of value'
);

my $EOL = "\r\n";
my $EOL_LENGTH = length( $EOL );


# Create mock object

my $mock = Test::MockObject->new( {} );

####
$mock->fake_module(
  'AnyEvent::Handle',

  new => sub {
    my $proto = shift;
    my %params = @_;

    $mock->{ on_connect } = $params{ on_connect };
    $mock->{ on_connect_error } = $params{ on_connect_error };
    $mock->{ on_error } = $params{ on_error };
    $mock->{ on_eof } = $params{ on_eof };
    $mock->{ on_read } = $params{ on_read };

    $mock->{ rbuf } = '';
    $mock->{ continue_read } = undef;
    $mock->{ read_queue } = [];
    $mock->{ destroyed } = undef;

    $mock->{ redis_h } = Test::AnyEvent::RedisHandle->new();

    $mock->{ _start } = AnyEvent->timer(
      after => 0,
      cb => sub {
        $mock->_connect();

        undef( $mock->{ _start } );
      }
    );

    return $mock;
  }
);

####
$mock->mock( 'push_write', sub {
  my $self = shift;
  my $cmd_str = shift;

  my $resp_str = $self->{ redis_h }->process_command( $cmd_str );

  $self->{ rbuf } .= $resp_str;

  if ( !$self->{ continue_read } ) {
    $self->{ continue_read } = 1;
  }

  return;
} );

####
$mock->mock( 'unshift_read', sub {
  my $self = shift;
  my $cb = shift;

  unshift( @{ $self->{ read_queue } }, $cb );

  $self->{ continue_read } = 1;

  return;
} );

####
$mock->mock( 'destroyed', sub {
  my $self = shift;

  return $self->{ destroyed };
} );

####
$mock->mock( 'destroy', sub {
  my $self = shift;

  $self->{ destroyed } = undef;

  return;
} );

####
$mock->mock( '_connect', sub {
  my $self = shift;

  $self->{ on_connect }->();

  my $curr_on_read_cb;

  $self->{ read } = AnyEvent->timer(
    after => 0,
    interval => 0.001,
    cb => sub {

      if ( $self->{ continue_read } ) {

        if ( @{ $self->{ read_queue } } && !defined( $curr_on_read_cb ) ) {
          $curr_on_read_cb = shift( @{ $self->{ read_queue } } );
        }

        if ( defined( $curr_on_read_cb ) ) {

          if ( !$curr_on_read_cb->( $self ) ) {
            $self->{ continue_read } = undef;

            return;
          }

          $curr_on_read_cb = undef;
        }
        else {
          $self->{ continue_read } = undef;
          $self->{ on_read }->( $self );
        }
      }
    }
  );

  return;
} );


# Constructro
sub new {
  my $proto = shift;

  my $class = ref( $proto ) || $proto;
  my $self = fields::new( $class );

  $self->{ storage } = {};
  $self->{ is_auth } = 0;
  $self->{ transaction_began } = undef;
  $self->{ commands_queue } = [];

  return $self;
}


# Public methods

####
sub process_command {
  my $self = shift;
  my $cmd_str = shift;

  my $cmd = $self->_parse_command( $cmd_str );

  my $resp;

  if ( defined( $cmd ) ) {

    if ( exists( $COMMANDS{ $cmd->{ name } } ) ) {

      $resp = eval {
        $self->_exec_command( $cmd );
      };

      if ( $@ ) {
        $resp = $@;
      }
    }
    else {
      ( my $msg = $ERR_MESSAGES{ unknown_cmd } ) =~ s/%c/$cmd->{ name }/go;

      $resp = {
        type => '-',
        data => $msg
      };
    }
  }
  else {
    $resp = {
      type => '-',
      data => $ERR_MESSAGES{ protocol_error }
    };
  }

  return $self->_serialize_response( $resp );
}


# Private methods

####
sub _parse_command {
  my $self = shift;
  my $cmd_str = shift;

  if ( !defined( $cmd_str ) || $cmd_str eq '' ) {
    return;
  }

  my $eol_pos = index( $cmd_str, $EOL );

  if ( $eol_pos <= 0 ) {
    return;
  }

  my $token = substr( $cmd_str, 0, $eol_pos, '' );
  my $type = substr( $token, 0, 1, '' );
  substr( $cmd_str, 0, $EOL_LENGTH, '' );

  if ( $type ne '*' ) {
    return;
  }

  my $mbulk_len = $token;

  if ( $mbulk_len =~ m/[^0-9]/o || $mbulk_len == 0 ) {
    return;
  }

  my $args_remaining = $mbulk_len;

  my @args;
  my $bulk_len;

  while ( $args_remaining ) {

    if ( $bulk_len ) {
      my $arg = substr( $cmd_str, 0, $bulk_len, '' );
      substr( $cmd_str, 0, $EOL_LENGTH, '' );

      push( @args, $arg );

      undef( $bulk_len );
      --$args_remaining;
    }
    else {
      my $eol_pos = index( $cmd_str, $EOL );

      if ( $eol_pos <= 0 ) {
        return;
      }

      my $token = substr( $cmd_str, 0, $eol_pos, '' );
      my $type = substr( $token, 0, 1, '' );
      substr( $cmd_str, 0, $EOL_LENGTH, '' );

      if ( $type ne '$' ) {
        return;
      }

      $bulk_len = $token;
    }
  }

  my $cmd = {
    name => shift( @args ),
    args => \@args
  };

  return $cmd;
}

####
sub _exec_command {
  my $self = shift;
  my $cmd = shift;

  if ( !$self->{ is_auth } && $cmd->{ name } ne 'auth' ) {
    return {
      type => '-',
      data => $ERR_MESSAGES{ not_permitted }
    };
  }

  my $cmd_h = $COMMANDS{ $cmd->{ name } };

  if ( exists( $cmd_h->{ validate } ) ) {
    $cmd_h->{ validate }->( $self, $cmd );
  }

  if ( $self->{ transaction_began } && $cmd->{ name } ne 'exec' ) {
    push( @{ $self->{ commands_queue } }, $cmd );

    return {
      type => '+',
      data => 'QUEUED'
    };
  }

  return $cmd_h->{ exec }->( $self, $cmd );
}

####
sub _serialize_response {
  my $self = shift;
  my $resp = shift;

  if ( $resp->{ type } eq '+' || $resp->{ type } eq ':' ) {
    return "$resp->{ type }$resp->{ data }$EOL";
  }
  elsif ( $resp->{ type } eq '-' ) {
    return "$resp->{ type }ERR $resp->{ data }$EOL";
  }
  elsif ( $resp->{ type } eq '$' ) {

    if ( defined( $resp->{ data } ) && $resp->{ data } ne ''  ){
      my $bulk_len = length( $resp->{ data } );

      return "$resp->{ type }$bulk_len$EOL$resp->{ data }$EOL";
    }

    return "$resp->{ type }-1$EOL";
  }
  elsif ( $resp->{ type } eq '*' ) {

    my $mbulk_len = scalar( @{ $resp->{ data } } );

    if ( $mbulk_len > 0 ) {
      my $data_szd = "*$mbulk_len$EOL";

      foreach my $val ( @{ $resp->{ data } } ) {

        if ( ref( $val ) eq 'HASH' ) {
          $data_szd .= $self->_serialize_response( $val );
        }
        else {
          my $bulk_len = length( $val );
          $data_szd .= "\$$bulk_len$EOL$val$EOL";
        }
      }

      return $data_szd;
    }

    return "*0$EOL";
  }
}


# Command methods

####
sub _validate_auth {
  my $cmd = pop;

  my $pass = $cmd->{ args }->[ 0 ];

  if ( !defined( $pass ) ) {
    ( my $msg = $ERR_MESSAGES{ wrong_args } ) =~ s/%c/$cmd->{ name }/go;

    die {
      type => '-',
      data => $msg
    };
  }

  return 1;
}

####
sub _exec_auth {
  my $self = shift;
  my $cmd = shift;

  my $pass = $cmd->{ args }->[ 0 ];

  if ( $pass ne $PASSWORD ) {
    return {
      type => '-',
      data => $ERR_MESSAGES{ invalid_pass }
    };
  }

  $self->{ is_auth } = 1;

  return {
    type => '+',
    data => 'OK'
  };
}

####
sub _exec_ping {
  return {
    type => '+',
    data => 'PONG'
  };
}

####
sub _validate_incr {
  my $cmd = pop;

  my $key = $cmd->{ args }->[ 0 ];

  if ( !defined( $key ) ) {
    ( my $msg = $ERR_MESSAGES{ wrong_args } ) =~ s/%c/$cmd->{ name }/go;

    die {
      type => '-',
      data => $msg
    };
  }
}

####
sub _exec_incr {
  my $self = shift;
  my $cmd = shift;

  my $key = $cmd->{ args }->[ 0 ];

  my $storage = $self->{ storage };

  if ( exists( $storage->{ $key } ) ) {

    if ( defined( $storage->{ $key } ) && ref( $storage->{ $key } ) ) {
      return {
        type => '-',
        data => $ERR_MESSAGES{ wrong_value }
      };
    }
    elsif ( $storage->{ $key } =~ m/[^0-9]/o ) {
      return {
        type => '-',
        data => $ERR_MESSAGES{ not_integer }
      };
    }
  }
  else {
    $storage->{ $key } = 0;
  }

  ++$storage->{ $key };

  return {
    type => ':',
    data => $storage->{ $key }
  };
}

####
sub _validate_set {
  my $cmd = pop;

  my $args = $cmd->{ args };
  my $key = $args->[ 0 ];
  my $val = $args->[ 1 ];

  if ( !defined( $key ) || !defined( $val ) ) {
    ( my $msg = $ERR_MESSAGES{ wrong_args } ) =~ s/%c/$cmd->{ name }/go;

    die {
      type => '-',
      data => $msg
    };
  }
}

####
sub _exec_set {
  my $self = shift;
  my $cmd = shift;

  my $args = $cmd->{ args };
  my $key = $args->[ 0 ];
  my $val = $args->[ 1 ];

  $self->{ storage }->{ $key } = $val;

  return {
    type => '+',
    data => 'OK'
  };
}

####
sub _validate_get {
  my $cmd = pop;

  my $key = $cmd->{ args }->[ 0 ];

  if ( !defined( $key ) ) {
    ( my $msg = $ERR_MESSAGES{ wrong_args } ) =~ s/%c/get/go;

    die {
      type => '-',
      data => $msg
    };
  }
}

####
sub _exec_get {
  my $self = shift;
  my $cmd = shift;

  my $key = $cmd->{ args }->[ 0 ];

  my $storage = $self->{ storage };

  if ( defined( $storage->{ $key } ) ) {

    if ( ref( $storage->{ $key } ) ) {
      return {
        type => '-',
        data => $ERR_MESSAGES{ wrong_value }
      };
    }

    return {
      type => '$',
      data => $storage->{ $key }
    };
  }

  return {
    type => '$',
    data => undef
  };
}

####
sub _validate_rpush {
  my $self = shift;
  my $cmd = shift;

  $self->_validate_push( 'r', $cmd );

  return 1;
}

####
sub _exec_rpush {
  my $self = shift;
  my $cmd = shift;

  return $self->_exec_push( 'r', $cmd );
}

####
sub _validate_lpush {
  my $self = shift;
  my $cmd = shift;

  $self->_validate_push( 'l', $cmd );

  return 1;
}

####
sub _exec_lpush {
  my $self = shift;
  my $cmd = shift;

  return $self->_exec_push( 'l', $cmd );
}

####
sub _validate_lrange {
  my $self = shift;
  my $cmd = shift;

  my $args = $cmd->{ args };
  my $key = $args->[ 0 ];
  my $start = $args->[ 1 ];
  my $stop = $args->[ 2 ];

  if ( !defined( $key ) || !defined( $start ) || !defined( $stop ) ) {
    ( my $msg = $ERR_MESSAGES{ wrong_args } ) =~ s/%c/lrange/go;

    die {
      type => '-',
      data => $msg
    };
  }
}

####
sub _validate_push {
  my $type = $_[ 1 ];
  my $cmd = $_[ 2 ];

  my $args = $cmd->{ args };
  my $key = $args->[ 0 ];
  my $val = $args->[ 1 ];

  if ( !defined( $key ) || !defined( $val ) ) {
    ( my $msg = $ERR_MESSAGES{ wrong_args } ) =~ s/%c/${type}push/go;

    die {
      type => '-',
      data => $msg
    };
  }
}

####
sub _exec_push {
  my $self = shift;
  my $type = shift;
  my $cmd = shift;

  my $args = $cmd->{ args };
  my $key = $args->[ 0 ];
  my $val = $args->[ 1 ];

  my $storage = $self->{ storage };

  if ( defined( $storage->{ $key } ) ) {

    if ( ref( $storage->{ $key } ) ne 'ARRAY' ) {
      return {
        type => '-',
        data => $ERR_MESSAGES{ wrong_value }
      };
    }
  }
  else {
    $storage->{ $key } = [];
  }

  if ( $type eq 'r' ) {
    push( @{ $storage->{ $key } }, $val );
  }
  else {
    unshift( @{ $storage->{ $key } }, $val );
  }

  return {
    type => '+',
    data => 'OK'
  };
}

####
sub _exec_lrange {
  my $self = shift;
  my $cmd = shift;

  my $args = $cmd->{ args };
  my $key = $args->[ 0 ];
  my $start = $args->[ 1 ];
  my $stop = $args->[ 2 ];

  if ( $start !~ m/^\-?[0-9]+$/o ) {
    $start = 0;
  }

  if ( $stop !~ m/^\-?[0-9]+$/o ) {
    $stop = 0;
  }

  my $storage = $self->{ storage };

  if ( defined( $storage->{ $key } ) ) {

    if ( ref( $storage->{ $key } ) ne 'ARRAY' ) {
      return {
        type => '-',
        data => $ERR_MESSAGES{ wrong_value }
      };
    }

    if ( $stop < 0 ) {
      $stop = scalar( @{ $storage->{ $key } } ) + $stop;
    }

    my @list = @{ $storage->{ $key } }[ $start .. $stop ];

    return {
      type => '*',
      data => \@list
    };
  }

  return {
    type => '*',
    data => []
  };
}

####
sub _exec_multi {
  my $self = shift;

  $self->{ transaction_began } = 1;

  return {
    type => '+',
    data => 'OK'
  };
}

sub _exec_exec {
  my $self = shift;

  my @data_list;

  if ( @{ $self->{ commands_queue } } ) {

    while ( my $cmd = shift( @{ $self->{ commands_queue } } ) ) {
      my $resp = $COMMANDS{ $cmd->{ name } }->{ exec }->( $self, $cmd );

      push( @data_list, $resp );
    }
  }

  $self->{ transaction_began } = 0;

  return {
    type => '*',
    data => \@data_list
  };
}

####
sub _exec_quit {
  return {
    type => '+',
    data => 'OK'
  };
}

1;
