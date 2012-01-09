package AnyEvent::RipeRedis;

# TODO clean tail white spaces

use 5.010000;
use strict;
use warnings;

use fields qw(
  host
  port
  password
  encoding
  reconnect
  reconnect_after
  max_reconnect_attempts
  on_connect
  on_auth
  on_stop_reconnect
  on_error
  handle
  reconnect_attempt
  commands_queue
  watched_keys
  transaction_cursor
  sub_lock
  subs
);

our $VERSION = '0.300010';

use AnyEvent;
use AnyEvent::Handle;
use Encode qw( find_encoding is_utf8 );
use Scalar::Util 'looks_like_number';
use Carp 'croak';

# Available error codes
our $ERR_CONNECT = 1;
our $ERR_HANDLE = 2;
our $ERR_EOF = 3;
our $ERR_COMMAND = 4;
our $ERR_PROCESS = 5;

# Available subscription types
my $SUB_GENERIC = 1;
my $SUB_PATTERN = 2;

my $EOL = "\r\n";


# Constructor
sub new {
  my $proto = shift;
  my $opts = shift;

  if ( ref( $opts ) ne 'HASH' ) {
    $opts = {};
  }

  my $class = ref( $proto ) || $proto;
  my $self = fields::new( $class );

  $self->{ 'host' } = $opts->{ 'host' } || 'localhost';
  $self->{ 'port' } = $opts->{ 'port' } || '6379';
  $self->{ 'password' } = $opts->{ 'password' };

  if ( defined( $opts->{ 'encoding' } ) ) {
    $self->{ 'encoding' } = find_encoding( $opts->{ 'encoding' } );

    if ( !defined( $self->{ 'encoding' } ) ) {
      croak "Encoding \"$opts->{ 'encoding' }\" not found.";
    }
  }

  $self->{ 'reconnect' } = $opts->{ 'reconnect' };

  if ( $self->{ 'reconnect' } ) {

    if ( defined( $opts->{ 'reconnect_after' } ) ) {

      if ( looks_like_number( $opts->{ 'reconnect_after' } )
        && $opts->{ 'reconnect_after' } > 0 ) {

        $self->{ 'reconnect_after' } = $opts->{ 'reconnect_after' };
      }
      else {
        croak '"reconnect_interval" must be a positive number';
      }
    }
    else {
      $self->{ 'reconnect_after' } = 5;
    }

    if ( defined( $opts->{ 'max_reconnect_attempts' } ) ) {

      if ( looks_like_number( $opts->{ 'max_reconnect_attempts' } )
        && $opts->{ 'max_reconnect_attempts' } > 0 ) {

        $self->{ 'max_reconnect_attempts' } = $opts->{ 'max_reconnect_attempts' };
      }
      else {
        croak '"max_reconnect_attempts" must be a positive number';
      }
    }
  }

  if ( defined( $opts->{ 'on_connect' } ) ) {

    if ( ref( $opts->{ 'on_connect' } ) eq 'CODE' ) {
      $self->{ 'on_connect' } = $opts->{ 'on_connect' };
    }
    else {
      croak '"on_connect" callback must be a CODE reference';
    }
  }

  if ( defined( $opts->{ 'on_auth' } ) ) {

    if ( ref( $opts->{ 'on_auth' } ) eq 'CODE' ) {
      $self->{ 'on_auth' } = $opts->{ 'on_auth' };
    }
    else {
      croak '"on_auth" callback must be a CODE reference';
    }
  }

  if ( defined( $opts->{ 'on_stop_reconnect' } ) ) {

    if ( ref( $opts->{ 'on_stop_reconnect' } ) eq 'CODE' ) {
      $self->{ 'on_stop_reconnect' } = $opts->{ 'on_stop_reconnect' };
    }
    else {
      croak '"on_stop_reconnect" callback must be a CODE reference';
    }
  }

  if ( defined( $opts->{ 'on_error' } ) ) {

    if ( ref( $opts->{ 'on_error' } ) eq 'CODE' ) {
      $self->{ 'on_error' } = $opts->{ 'on_error' };
    }
    else {
      croak '"on_error" callback must be a CODE reference';
    }
  }
  else {
    $self->{ 'on_error' } = sub { warn "$_[ 0 ]\n" };
  }

  $self->{ 'handle' } = undef;
  $self->{ 'reconnect_attempt' } = 0;
  $self->{ 'commands_queue' } = [];
  $self->{ 'watched_keys' } = [];
  $self->{ 'transaction_cursor' } = undef;
  $self->{ 'sub_lock' } = undef;
  $self->{ 'subs' } = {};

  $self->_connect();

  return $self;
}


# Public methods

####
sub execute_command {
  my $self = shift;
  my $cmd_name = shift;

  my $params = {};

  if ( ref( $_[ -1 ] ) eq 'HASH' ) {
    $params = pop( @_ );
  }

  my @args = @_;

  if ( !defined( $params->{ 'failover' } ) ) {
    $params->{ 'failover' } = 1;
  }

  my $cmd = {
    name => $cmd_name,
    args => \@args,
    failover => $params->{ 'failover' }
  };

  if ( defined( $params->{ 'cb' } ) ) {

    if ( ref( $params->{ 'cb' } ) ne 'CODE' ) {
      croak 'Callback must be a CODE reference';
    }

    $cmd->{ 'cb' } = $params->{ 'cb' };
  }

  if ( $cmd_name eq 'multi' ) {
    $self->{ 'sub_lock' } = 1;
  }
  elsif ( $cmd_name eq 'exec' ) {
    undef( $self->{ 'sub_lock' } );
  }
  elsif ( $cmd_name eq 'subscribe' || $cmd_name eq 'psubscribe'
      || $cmd_name eq 'unsubscribe' || $cmd_name eq 'punsubscribe' ) {

    if ( $self->{ 'sub_lock' } ) {
      croak "Command \"$cmd_name\" not allowed in this context."
          . " First, the transaction must be completed.";
    }

    $cmd->{ 'resp_cnt' } = scalar( @args );

    if ( $cmd_name eq 'subscribe' || $cmd_name eq 'psubscribe' ) {

      if ( !defined( $params->{ 'on_message' } ) ) {
        croak '"on_message" callback must be specified';
      }
      elsif ( ref( $params->{ 'on_message' } ) ne 'CODE' ) {
        croak '"on_message" callback must be a CODE reference';
      }

      $cmd->{ 'on_message' } = $params->{ 'on_message' };
    }
  }
  else {

    if ( defined( $params->{ 'on_exec' } ) ) {

      if ( ref( $params->{ 'on_exec' } ) ne 'CODE' ) {
        croak '"on_exec" callback must be a CODE reference';
      }

      $cmd->{ 'on_exec' } = $params->{ 'on_exec' };
    }
  }

  $self->_push_command( $cmd );

  return 1;
}

####
sub reconnect {
  my $self = shift;

  $self->disconnect();

  my $after = ( $self->{ 'reconnect_attempt' } > 0 ) ? $self->{ 'reconnect_after' } : 0;

  my $timer;

  $timer = AnyEvent->timer(
    after => $after,
    cb => sub {
      undef( $timer );

      ++$self->{ 'reconnect_attempt' };

      $self->_connect();
    }
  );

  return 1;
}

####
sub disconnect {
  my $self = shift;

  if ( defined( $self->{ 'handle' } ) && !$self->{ 'handle' }->destroyed() ) {
    $self->{ 'handle' }->destroy();
  }

  $self->{ 'handle' } = undef;
  undef( $self->{ 'transaction_cursor' } );

  return 1;
}


# Private methods

####
sub _connect {
  my $self = shift;

  $self->{ 'handle' } = AnyEvent::Handle->new(
    connect => [ $self->{ 'host' }, $self->{ 'port' } ],
    keepalive => 1,

    on_connect => sub {
      return $self->_post_connect();
    },

    on_connect_error => sub {
      $self->{ 'on_error' }->( $_[ 1 ], $ERR_CONNECT );
      $self->_attempt_to_reconnect();
    },

    on_error => sub {
      $self->{ 'on_error' }->( $_[ 2 ], $ERR_HANDLE );
      $self->_attempt_to_reconnect();
    },

    on_eof => sub {
      $self->{ 'on_error' }->( 'EOF detected', $ERR_EOF );
      $self->_attempt_to_reconnect();
    },

    on_read => sub {
      my $hdl = shift;

      return $self->_on_read( $hdl );
    }
  );

  my @cmd_q;

  if ( @{ $self->{ 'commands_queue' } } ) {
    @cmd_q = grep { $_->{ 'failover' } } @{ $self->{ 'commands_queue' } };
    $self->{ 'commands_queue' } = [];
  }

  if ( defined( $self->{ 'password' } ) && $self->{ 'password' } ne '' ) {
    $self->_push_command( {
      name => 'auth',
      args => [  $self->{ 'password' } ],

      cb => sub {

        if ( defined( $self->{ 'on_auth' } ) ) {
          my $data = shift;

          $self->{ 'on_auth' }->( $data );
        }
      }
    } );
  }

  my $watched_keys = $self->{ 'watched_keys' };
  $self->{ 'watched_keys' } = [];

  if ( @{ $self->{ 'watched_keys' } } ) {
    $self->_push_command( {
      name => 'watch',
      args => $self->{ 'watched_keys' }
    } );
  }

  my $subs = $self->{ 'subs' };
  $self->{ 'subs' } = {};

  foreach my $ch_name ( keys( %{ $subs } ) ) {
    my $sub = $subs->{ $ch_name };

    my $cmd_name = ( $sub->{ 'sub_type' } == $SUB_GENERIC ) ? 'subscribe' : 'psubscribe';

    $self->_push_command( {
      name => $cmd_name,
      args => [ $ch_name ],
      on_message => $sub->{ 'on_message' },
      resp_cnt => 1,
      failover => 1
    } );
  }

  foreach my $cmd ( @cmd_q ) {
    $self->_push_command( $cmd );
  }
}

####
sub _post_connect {
  my $self = shift;

  $self->{ 'reconnect_attempt' } = 0;

  if ( defined( $self->{ 'on_connect' } ) ) {
    $self->{ 'on_connect' }->( $self->{ 'reconnect_attempt' } );
  }
}

####
sub _push_command {
  my $self = shift;
  my $cmd = shift;

  push( @{ $self->{ 'commands_queue' } }, $cmd );

  if ( defined( $self->{ 'handle' } ) ) {
    my $cmd_str = $self->_serialize_command( $cmd );
    $self->{ 'handle' }->push_write( $cmd_str );
  }
}

####
sub _attempt_to_reconnect {
  my $self = shift;

  if ( $self->{ 'reconnect' } && ( !defined( $self->{ 'max_reconnect_attempts' } )
    || $self->{ 'reconnect_attempt' } < $self->{ 'max_reconnect_attempts' } ) ) {

    $self->reconnect();
  }
  else {

    if ( defined( $self->{ 'on_stop_reconnect' } ) ) {
      $self->{ 'on_stop_reconnect' }->();
    }
  }
}

####
sub _serialize_command {
  my $self = shift;
  my $cmd = shift;

  my $bulk_len = scalar( @{ $cmd->{ 'args' } } ) + 1;
  my $cmd_str = "*$bulk_len$EOL";

  foreach my $tkn ( $cmd->{ 'name' }, @{ $cmd->{ 'args' } } ) {

    if ( $self->{ 'encoding' } && is_utf8( $tkn ) ) {
      $tkn = $self->{ 'encoding' }->encode( $tkn );
    }

    if ( defined( $tkn ) ) {
      my $tkn_len =  length( $tkn );
      $cmd_str .= "\$$tkn_len$EOL$tkn$EOL";
    }
    else {
      $cmd_str .= "\$-1$EOL";
    }
  }

  return $cmd_str;
}

####
sub _on_read {
  my $self = shift;
  my $hdl = shift;

  $hdl->push_read(
    line => $self->_prepare_resp_cb(
      sub {
        return $self->_prcoess_response( @_ );
      }
    )
  );
}

####
sub _prepare_resp_cb {
  my $self = shift;
  my $cb = shift;

  return sub {
    my $hdl = shift;
    my $str = shift;

    if ( !defined( $str ) || $str eq '' ) {
      $cb->();
    }

    my $type = substr( $str, 0, 1 );
    my $val = substr( $str, 1 );

    if ( $type eq '+' || $type eq ':' ) {
      $cb->( $val );
    }
    elsif ( $type eq '-' ) {
      $cb->( $val, 1 );
    }
    elsif ( $type eq '$' ) {
      my $bulk_len = $val;

      if ( $bulk_len >= 0 ) {
        $hdl->unshift_read( chunk => $bulk_len + length( $EOL ), sub {
          my $data = $_[ 1 ];

          local $/ = $EOL;
          chomp( $data );

          if ( $self->{ 'encoding' } ) {
            $data = $self->{ 'encoding' }->decode( $data );
          }

          $cb->( $data );
        } );
      }
      else {
        $cb->();
      }
    }
    elsif ( $type eq '*' ) {
      my $args_num = $val;

      if ( $args_num > 0 ) {
        my @mbulk_data = ();
        my $mbulk_len = 0;

        my $arg_cb = sub {
          my $data = shift;

          push( @mbulk_data, $data );

          if ( ++$mbulk_len == $args_num ) {
            $cb->( \@mbulk_data );
          }
        };

        for ( my $i = 0; $i < $args_num; $i++ ) {
          $hdl->unshift_read( line => $self->_prepare_resp_cb( $arg_cb ) );
        }
      }
      elsif ( $args_num == 0 ) {
        $cb->( [] );
      }
      else {
        $cb->();
      }
    }
    else {
      $self->{ 'on_error' }->( "Unexpected response type: \"$type\"", $ERR_PROCESS );
      $self->_attempt_to_reconnect();
    }
  };
}

####
sub _prcoess_response {
  my $self = shift;
  my $data = shift;
  my $err_ocurred = shift;

  if ( $err_ocurred ) {
    return $self->_process_error( $data );
  }

  if ( %{ $self->{ 'subs' } } ) {

    if ( $self->_looks_like_sub_msg( $data ) ) {
      $self->{ 'subs' }->{ $data->[ 1 ] }->{ 'on_message' }->( $data->[ 2 ] );

      return 1;
    }
    elsif ( $self->_looks_like_psub_msg( $data ) ) {
      $self->{ 'subs' }->{ $data->[ 1 ] }->{ 'on_message' }->( $data->[ 3 ] );

      return 1;
    }
  }

  if ( !@{ $self->{ 'commands_queue' } } ) {
    $self->{ 'on_error' }->( 'Do not know how process response. '
        . 'Queue of commands is empty', $ERR_PROCESS );

    return;
  }

  my $cmd;

  if ( defined( $self->{ 'transaction_cursor' } ) ) {
    $cmd = $self->{ 'commands_queue' }->[ $self->{ 'transaction_cursor' } ];
  }
  else {
    $cmd = $self->{ 'commands_queue' }->[ 0 ];
  }

  if ( $cmd->{ 'name' } eq 'multi' ) {
    $self->{ 'transaction_cursor' } = 1;

    if ( exists( $cmd->{ 'cb' } ) ) {
      $cmd->{ 'cb' }->( $data );

      delete( $cmd->{ 'cb' } );
    }
  }
  elsif ( $cmd->{ 'name' } eq 'exec' ) {
    $self->_process_exec_cmd( $cmd, $data );
  }
  elsif ( $cmd->{ 'name' } eq 'subscribe' || $cmd->{ 'name' } eq 'psubscribe'
      || $cmd->{ 'name' } eq 'unsubscribe' || $cmd->{ 'name' } eq 'punsubscribe' ) {

    $self->_process_subscr( $cmd, $data );
  }
  elsif ( $cmd->{ 'name' } eq 'quit' ) {
    $self->disconnect();
  }
  else {

    if ( defined( $self->{ 'transaction_cursor' } ) ) {

      if ( exists( $cmd->{ 'cb' } ) ) {
        $cmd->{ 'cb' }->( $data );

        delete( $cmd->{ 'cb' } );
      }

      ++$self->{ 'transaction_cursor' };
    }
    else {

      if ( $cmd->{ 'name' } eq 'watch' ) {
        push( @{ $self->{ 'watched_keys' } }, @{ $cmd->{ 'args' } } );
      }
      elsif ( $cmd->{ 'name' } eq 'unwatch' ) {
        $self->{ 'watched_keys' } = [];
      }

      if ( exists( $cmd->{ 'cb' } ) ) {
        $cmd->{ 'cb' }->( $data );
      }

      shift( @{ $self->{ 'commands_queue' } } );
    }
  }
}

####
sub _process_error {
  my $self = shift;
  my $err_msg = shift;

  if ( defined( $self->{ 'transaction_cursor' } ) ) {
    splice( @{ $self->{ 'commands_queue' } }, $self->{ 'transaction_cursor' }, 1 );
  }
  else {
    shift( @{ $self->{ 'commands_queue' } } );
  }

  $self->{ 'on_error' }->( $err_msg, $ERR_COMMAND );
}

####
sub _process_exec_cmd {
  my $self = shift;
  my $cmd = shift;
  my $data = shift;

  my $data_cursor = 0;

  while ( my $trn_cmd = shift( @{ $self->{ 'commands_queue' } } ) ) {

    if ( $trn_cmd->{ 'name' } eq 'multi' ) {
      next;
    }
    elsif ( $trn_cmd->{ 'name' } eq 'exec' ) {
      last;
    }
    else {

      if ( $cmd->{ 'name' } eq 'watch' ) {
        push( @{ $self->{ 'watched_keys' } }, @{ $cmd->{ 'args' } } );
      }
      elsif ( $cmd->{ 'name' } eq 'unwatch' ) {
        $self->{ 'watched_keys' } = [];
      }

      if ( exists( $trn_cmd->{ 'on_exec' } ) ) {
        $trn_cmd->{ 'on_exec' }->( $data->[ $data_cursor ] );
      }
    }

    ++$data_cursor;
  }

  if ( exists( $cmd->{ 'cb' } ) ) {
    $cmd->{ 'cb' }->( $data );
  }

  undef( $self->{ 'transaction_cursor' } );
  $self->{ 'watched_keys' } = [];
}

####
sub _process_subscr {
  my $self = shift;
  my $cmd = shift;
  my $data = shift;

  if ( !( ref( $data ) eq 'ARRAY' && scalar( @{ $data } ) == 3
      && $data->[ 0 ] eq $cmd->{ 'name' } && !ref( $data->[ 1 ] )
      && $data->[ 1 ] ~~ $cmd->{ 'args' } && looks_like_number( $data->[ 2 ] ) ) ) {

    $self->{ 'on_error' }->( "Unexpected response to a \"$cmd->{ 'name' }\" command", $ERR_PROCESS );

    return;
  }

  if ( $cmd->{ 'name' } eq 'subscribe' ) {
    $self->{ 'subs' }->{ $data->[ 1 ] } = {
      sub_type => $SUB_GENERIC,
      on_message => $cmd->{ 'on_message' }
    }
  }
  elsif ( $cmd->{ 'name' } eq 'psubscribe' ) {
    $self->{ 'subs' }->{ $data->[ 1 ] } = {
      sub_type => $SUB_PATTERN,
      on_message => $cmd->{ 'on_message' }
    }
  }
  else {
    delete( $self->{ 'subs' }->{ $data->[ 1 ] } );
  }

  --$cmd->{ 'resp_cnt' };

  if ( !exists( $cmd->{ 'resp_cnt' } ) || $cmd->{ 'resp_cnt' } == 0 ) {
    shift( @{ $self->{ 'commands_queue' } } );
  }

  if ( exists( $cmd->{ 'cb' } ) ) {
    $cmd->{ 'cb' }->( $data->[ 2 ] );
  }
}

####
sub _looks_like_sub_msg {
  my $data = $_[ 1 ];

  return ref( $data ) eq 'ARRAY' && scalar( @{ $data } ) == 3
      && $data->[ 0 ] eq 'message' && !ref( $data->[ 1 ] ) && !ref( $data->[ 2 ] );
}

####
sub _looks_like_psub_msg {
  my $data = $_[ 1 ];

  return ref( $data ) eq 'ARRAY' && scalar( @{ $data } ) == 4
      && $data->[ 0 ] eq 'pmessage' && !ref( $data->[ 1 ] ) && !ref( $data->[ 2 ] )
      && !ref( $data->[ 3 ] );
}

####
sub AUTOLOAD {
  our $AUTOLOAD;

  my $cmd_name = $AUTOLOAD;
  $cmd_name =~ s/^.+:://o;
  $cmd_name = lc( $cmd_name );

  my $sub = sub {
    my $self = shift;

    return $self->execute_command( $cmd_name, @_ );
  };

  do {
    no strict 'refs';

    *{ $AUTOLOAD } = $sub;
  };

  goto &{ $sub };
}


####
sub DESTROY {
  my $self = shift;

  $self->disconnect();
};

1;
__END__

=head1 NAME

AnyEvent::RipeRedis - Non-blocking Redis client with self reconnect feature on
loss connection

=head1 SYNOPSIS

  use AnyEvent::RipeRedis;

=head1 DESCRIPTION

This module is an AnyEvent user, you need to make sure that you use and run a
supported event loop.

AnyEvent::RipeRedis is non-blocking Redis client with self reconnect feature on
loss connection. If connection lost or some socket error is occurr, module try
re-connect, and re-execute not finished commands.

Module requires Redis 1.2 or higher.

=head1 METHODS

=head1 SEE ALSO

Redis, AnyEvent::Redis, AnyEvent

=head1 AUTHOR

Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2011, Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>. All rights reserved.

This module is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut
