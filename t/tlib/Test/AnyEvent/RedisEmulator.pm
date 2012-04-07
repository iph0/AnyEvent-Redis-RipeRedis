package Test::AnyEvent::RedisEmulator;

use 5.006000;
use strict;
use warnings;

use fields qw(
  storage
  is_auth
  transaction_began
  commands_queue
  subs
  subs_num
);

our $VERSION = '0.100003';

my $PASSWORD = 'test';
my %COMMANDS = (
  auth => {
    validate => *_validate_auth,
    exec => *_exec_auth,
  },

  ping => {
    exec => *_exec_ping,
  },

  incr => {
    validate => *_validate_incr,
    exec => *_exec_incr,
  },

  set => {
    validate => *_validate_set,
    exec => *_exec_set,
  },

  get => {
    validate => *_validate_get,
    exec => *_exec_get,
  },

  rpush => {
    validate => *_validate_push,
    exec => *_exec_push,
  },

  lpush => {
    validate => *_validate_push,
    exec => *_exec_push,
  },

  brpop => {
    validate => *_validate_bpop,
    exec => *_exec_bpop,
  },

  blpop => {
    validate => *_validate_bpop,
    exec => *_exec_bpop,
  },

  lrange => {
    validate => *_validate_lrange,
    exec => *_exec_lrange,
  },

  multi => {
    exec => *_exec_multi,
  },

  exec => {
    exec => *_exec_exec,
  },

  subscribe => {
    validate => *_validate_sub,
    exec => *_exec_sub,
  },

  psubscribe => {
    validate => *_validate_sub,
    exec => *_exec_sub,
  },

  unsubscribe => {
    validate => *_validate_sub,
    exec => *_exec_unsub,
  },

  punsubscribe => {
    validate => *_validate_sub,
    exec => *_exec_unsub,
  },

  quit => {
    exec => *_exec_quit,
  },
);

my %ERR_MESSAGES = (
  protocol_error => 'Protocol error',
  invalid_pass => 'invalid password',
  not_permitted => 'operation not permitted',
  wrong_args => "wrong number of arguments for '\%c' command",
  unknown_cmd => "unknown command '\%c'",
  not_integer => 'value is not an integer or out of range',
  wrong_value => 'Operation against a key holding the wrong kind of value',
  invalid_timeout => 'timeout is not an integer or out of range',
);

my $EOL = "\r\n";
my $EOL_LENGTH = length( $EOL );


# Constructro
sub new {
  my $proto = shift;

  my $self = fields::new( $proto );

  $self->{storage} = {};
  $self->{is_auth} = 0;
  $self->{transaction_began} = undef;
  $self->{commands_queue} = [];
  $self->{subs} = {};
  $self->{subs_num} = 0;

  return $self;
}


# Public methods

####
sub process_command {
  my __PACKAGE__ $self = shift;
  my $cmd_szd = shift;

  my $cmd = $self->_parse_command( $cmd_szd );

  my $resp;
  if ( defined( $cmd ) ) {
    if ( exists( $COMMANDS{$cmd->{name}} ) ) {
      $resp = eval {
        $self->_exec_command( $cmd );
      };
      if ( $@ ) {
        $resp = $@;
      }
    }
    else {
      ( my $msg = $ERR_MESSAGES{unknown_cmd} )
          =~ s/%c/$cmd->{name}/go;
      $resp = {
        type => '-',
        data => $msg,
      };
    }
  }
  else {
    $resp = {
      type => '-',
      data => $ERR_MESSAGES{protocol_error},
    };
  }

  my $resp_szd;
  if ( ref( $resp ) ne 'ARRAY' ) {
    $resp_szd = $self->_serialize_response( $resp );
  }
  else {
    $resp_szd = '';
    foreach my $resp_el ( @{$resp} ) {
      $resp_szd .= $self->_serialize_response( $resp_el );
    }
  }

  return $resp_szd;
}


# Private methods

####
sub _parse_command {
  my __PACKAGE__ $self = shift;
  my $cmd_szd = shift;

  if ( !defined( $cmd_szd ) || $cmd_szd eq '' ) {
    return;
  }
  my $eol_pos = index( $cmd_szd, $EOL );
  if ( $eol_pos <= 0 ) {
    return;
  }
  my $token = substr( $cmd_szd, 0, $eol_pos, '' );
  my $type = substr( $token, 0, 1, '' );
  substr( $cmd_szd, 0, $EOL_LENGTH, '' );
  if ( $type ne '*' ) {
    return;
  }
  my $m_bulk_len = $token;
  if ( $m_bulk_len =~ m/[^0-9]/o || $m_bulk_len == 0 ) {
    return;
  }
  my $args = $self->_parse_m_bulk( $cmd_szd, $m_bulk_len );
  my $cmd = {
    name => shift( @{$args} ),
    args => $args,
  };

  return $cmd;
}

####
sub _parse_m_bulk {
  my $m_bulk_len = pop;
  my $cmd_szd = pop;

  my @args;
  my $bulk_len;
  my $args_remaining = $m_bulk_len;
  while ( $args_remaining ) {
    if ( $bulk_len ) {
      my $arg = substr( $cmd_szd, 0, $bulk_len, '' );
      substr( $cmd_szd, 0, $EOL_LENGTH, '' );
      push( @args, $arg );
      undef( $bulk_len );
      --$args_remaining;
    }
    else {
      my $eol_pos = index( $cmd_szd, $EOL );
      if ( $eol_pos <= 0 ) {
        return;
      }
      my $token = substr( $cmd_szd, 0, $eol_pos, '' );
      my $type = substr( $token, 0, 1, '' );
      substr( $cmd_szd, 0, $EOL_LENGTH, '' );
      if ( $type ne '$' ) {
        return;
      }
      $bulk_len = $token;
    }
  }

  return \@args;
}

####
sub _exec_command {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;

  if ( !$self->{is_auth} && $cmd->{name} ne 'auth' ) {
    return {
      type => '-',
      data => $ERR_MESSAGES{not_permitted},
    };
  }

  my $cmd_h = $COMMANDS{$cmd->{name}};

  if ( exists( $cmd_h->{validate} ) ) {
    $cmd_h->{validate}->( $self, $cmd );
  }

  if ( $self->{transaction_began} && $cmd->{name} ne 'exec' ) {
    push( @{$self->{commands_queue}}, $cmd );

    return {
      type => '+',
      data => 'QUEUED',
    };
  }

  return $cmd_h->{exec}->( $self, $cmd );
}

####
sub _serialize_response {
  my __PACKAGE__ $self = shift;
  my $resp = shift;

  if ( $resp->{type} eq '+' || $resp->{type} eq ':' ) {
    return "$resp->{type}$resp->{data}$EOL";
  }
  elsif ( $resp->{type} eq '-' ) {
    return "$resp->{type}ERR $resp->{data}$EOL";
  }
  elsif ( $resp->{type} eq '$' ) {
    if ( defined( $resp->{data} ) && $resp->{data} ne ''  ){
      my $bulk_len = length( $resp->{data} );
      return "$resp->{type}$bulk_len$EOL$resp->{data}$EOL";
    }

    return "$resp->{type}-1$EOL";
  }
  elsif ( $resp->{type} eq '*' ) {
    if ( !defined( $resp->{data} ) || $resp->{data} eq '' ) {
      return "*-1$EOL";
    }
    my $m_bulk_len = scalar( @{$resp->{data}} );
    if ( $m_bulk_len > 0 ) {
      my $data_szd = "*$m_bulk_len$EOL";
      foreach my $val ( @{$resp->{data}} ) {
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
  my @args = @{$cmd->{args}};
  my $pass = shift( @args );

  if ( !defined( $pass ) || $pass eq '' ) {
    ( my $msg = $ERR_MESSAGES{wrong_args} )
        =~ s/%c/$cmd->{name}/go;

    die {
      type => '-',
      data => $msg,
    };
  }

  return 1;
}

####
sub _exec_auth {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;
  my @args = @{$cmd->{args}};
  my $pass = shift( @args );

  if ( $pass ne $PASSWORD ) {
    return {
      type => '-',
      data => $ERR_MESSAGES{invalid_pass},
    };
  }

  $self->{is_auth} = 1;

  return {
    type => '+',
    data => 'OK',
  };
}

####
sub _exec_ping {
  return {
    type => '+',
    data => 'PONG',
  };
}

####
sub _validate_incr {
  my $cmd = pop;
  my @args = @{$cmd->{args}};
  my $key = shift( @args );

  if ( !defined( $key ) || $key eq '' ) {
    ( my $msg = $ERR_MESSAGES{wrong_args} )
        =~ s/%c/$cmd->{name}/go;

    die {
      type => '-',
      data => $msg,
    };
  }

  return 1;
}

####
sub _exec_incr {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;
  my @args = @{$cmd->{args}};
  my $key = shift( @args );

  my $storage = $self->{storage};
  if ( defined( $storage->{$key} ) ) {
    if ( ref( $storage->{$key} ) ) {
      return {
        type => '-',
        data => $ERR_MESSAGES{wrong_value},
      };
    }
    elsif ( $storage->{$key} =~ m/[^0-9]/o ) {
      return {
        type => '-',
        data => $ERR_MESSAGES{not_integer},
      };
    }
  }
  else {
    $storage->{$key} = 0;
  }

  $storage->{$key}++;

  return {
    type => ':',
    data => $storage->{$key},
  };
}

####
sub _validate_set {
  my $cmd = pop;
  my @args = @{$cmd->{args}};
  my $key = shift( @args );
  my $val = shift( @args );

  if (
    !defined( $key ) || $key eq ''
      || !defined( $val ) || $val eq ''
      ) {
    ( my $msg = $ERR_MESSAGES{wrong_args} )
        =~ s/%c/$cmd->{name}/go;

    die {
      type => '-',
      data => $msg,
    };
  }

  return 1;
}

####
sub _exec_set {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;
  my @args = @{$cmd->{args}};
  my $key = shift( @args );
  my $val = shift( @args );

  $self->{storage}{$key} = $val;

  return {
    type => '+',
    data => 'OK',
  };
}

####
sub _validate_get {
  my $cmd = pop;
  my @args = @{$cmd->{args}};
  my $key = shift( @args );

  if ( !defined( $key ) || $key eq '' ) {
    ( my $msg = $ERR_MESSAGES{wrong_args} )
        =~ s/%c/$cmd->{name}/go;

    die {
      type => '-',
      data => $msg,
    };
  }

  return 1;
}

####
sub _exec_get {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;
  my @args = @{$cmd->{args}};
  my $key = shift( @args );

  my $storage = $self->{storage};
  if ( !defined( $storage->{$key} ) ) {
    return {
      type => '$',
      data => undef,
    };
  }
  elsif ( ref( $storage->{$key} ) ) {
    return {
      type => '-',
      data => $ERR_MESSAGES{wrong_value},
    };
  }

  return {
    type => '$',
    data => $storage->{$key},
  };
}

####
sub _validate_push {
  my $cmd = pop;
  my @args = @{$cmd->{args}};
  my $key = shift( @args );
  my $val = shift( @args );

  if (
    !defined( $key ) || $key eq ''
      || !defined( $val ) || $val eq ''
      ) {
    ( my $msg = $ERR_MESSAGES{wrong_args} )
        =~ s/%c/$cmd->{name}/go;

    die {
      type => '-',
      data => $msg,
    };
  }

  return 1;
}

####
sub _exec_push {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;
  my @args = @{$cmd->{args}};
  my $key = shift( @args );
  my $val = shift( @args );

  my $storage = $self->{storage};
  if ( defined( $storage->{$key} ) ) {
    if ( ref( $storage->{$key} ) ne 'ARRAY' ) {
      return {
        type => '-',
        data => $ERR_MESSAGES{wrong_value},
      };
    }
  }
  else {
    $storage->{$key} = [];
  }

  if ( index( $cmd->{name}, 'r' ) == 0 ) {
    push( @{$storage->{$key}}, $val );
  }
  else {
    unshift( @{$storage->{$key}}, $val );
  }

  return {
    type => '+',
    data => 'OK',
  };
}

####
sub _validate_bpop {
  my $cmd = pop;
  my @args = @{$cmd->{args}};
  my $timeout = pop( @args );
  my @keys = @args;

  if (
    scalar( @keys ) == 0
      || !defined( $timeout ) || $timeout eq ''
      ) {
    ( my $msg = $ERR_MESSAGES{wrong_args} )
        =~ s/%c/$cmd->{name}/go;

    die {
      type => '-',
      data => $msg,
    };
  }
  elsif ( $timeout =~ m/[^0-9]/o ) {
    die {
      type => '-',
      data => $ERR_MESSAGES{invalid_timeout},
    };
  }

  return 1;
}

####
sub _exec_bpop {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;
  my @args = @{$cmd->{args}};
  my $timeout = pop( @args ); # Timeout will be ignored
  my @keys = @args;
  my $storage = $self->{storage};

  foreach my $key ( @keys ) {
    if ( !defined( $storage->{$key} ) ) {
      next;
    }
    elsif ( ref( $storage->{$key} ) ne 'ARRAY' ) {
      return {
        type => '-',
        data => $ERR_MESSAGES{wrong_value},
      };
    }

    my $val;

    if ( index( $cmd->{name}, 'br' ) == 0 ) {
      $val = pop( @{$storage->{$key}} );
    }
    else {
      $val = shift( @{$storage->{$key}} );
    }

    return {
      type => '$',
      data => $val,
    };
  }

  return {
    type => '*',
    data => undef,
  };
}

####
sub _validate_lrange {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;
  my @args = @{$cmd->{args}};
  my $key = shift( @args );
  my $start = shift( @args );
  my $stop = shift( @args );

  if (
    !defined( $key ) || $key eq ''
      || !defined( $start ) || $start eq ''
      || !defined( $stop ) || $stop eq ''
      ) {
    ( my $msg = $ERR_MESSAGES{wrong_args} )
        =~ s/%c/$cmd->{name}/go;

    die {
      type => '-',
      data => $msg,
    };
  }

  return 1;
}

####
sub _exec_lrange {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;
  my @args = @{$cmd->{args}};
  my $key = shift( @args );
  my $start = shift( @args );
  my $stop = shift( @args );
  if ( $start !~ m/^\-?[0-9]+$/o ) {
    $start = 0;
  }
  if ( $stop !~ m/^\-?[0-9]+$/o ) {
    $stop = 0;
  }

  my $storage = $self->{storage};
  if ( !defined( $storage->{$key} ) ) {
    return {
      type => '*',
      data => [],
    };
  }
  elsif ( ref( $storage->{$key} ) ne 'ARRAY' ) {
    return {
      type => '-',
      data => $ERR_MESSAGES{wrong_value},
    };
  }

  if ( $stop < 0 ) {
    $stop = scalar( @{$storage->{$key}} ) + $stop;
  }

  my @list = @{$storage->{$key}}[ $start .. $stop ];

  return {
    type => '*',
    data => \@list,
  };
}

####
sub _exec_multi {
  my __PACKAGE__ $self = shift;

  $self->{transaction_began} = 1;

  return {
    type => '+',
    data => 'OK',
  };
}

sub _exec_exec {
  my __PACKAGE__ $self = shift;

  my @data_list;
  if ( @{$self->{commands_queue}} ) {
    while ( my $cmd = shift( @{$self->{commands_queue}} ) ) {
      my $resp = $COMMANDS{$cmd->{name}}{exec}->( $self, $cmd );
      push( @data_list, $resp );
    }
  }

  $self->{transaction_began} = 0;

  return {
    type => '*',
    data => \@data_list,
  };
}

####
sub _validate_sub {
  my $cmd = pop;
  my @ch_proto = @{$cmd->{args}};

  if ( scalar( @ch_proto ) == 0 ) {
    ( my $msg = $ERR_MESSAGES{wrong_args} )
        =~ s/%c/$cmd->{name}/go;

    die {
      type => '-',
      data => $msg,
    };
  }

  return 1;
}

####
sub _exec_sub {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;
  my @ch_proto = @{$cmd->{args}};

  my @data;
  foreach my $ch_proto ( @ch_proto ) {
    if ( !exists( $self->{subs}{$ch_proto} ) ) {
      $self->{subs}{$ch_proto} = 1;
      ++$self->{subs_num};
    }
    push( @data, {
      type => '*',
      data => [
        $cmd->{name},
        $ch_proto,
        $self->{subs_num},
      ],
    } );

    # Send message to channels
    my $msg = 'test';
    if ( index( $cmd->{name}, 'p' ) == 0 ) {
      ( my $ch_name = $ch_proto ) =~ s/\*$/some/o;
      push( @data, {
        type => '*',
        data => [
          'pmessage',
          $ch_proto,
          $ch_name,
          $msg,
        ],
      } );
    }
    else {
      push( @data, {
        type => '*',
        data => [
          'message',
          $ch_proto,
          $msg,
        ],
      } );
    }
  }

  return \@data;
}

####
sub _exec_unsub {
  my __PACKAGE__ $self = shift;
  my $cmd = shift;
  my @ch_proto = @{$cmd->{args}};

  my @data;
  foreach my $ch_proto ( @ch_proto ) {
    if ( exists( $self->{subs}{$ch_proto} ) ) {
      delete( $self->{subs}{$ch_proto} );
      --$self->{subs_num};
    }
    push( @data, {
      type => '*',
      data => [
        $cmd->{name},
        $ch_proto,
        $self->{subs_num},
      ],
    } );
  }

  return \@data;
}

####
sub _exec_quit {
  return {
    type => '+',
    data => 'OK',
  };
}

1;
