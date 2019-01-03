package Net::Async::Redis;
# ABSTRACT: Redis support for IO::Async
use strict;
use warnings;

no indirect;

use parent qw(IO::Async::Notifier);

our $VERSION = '2.000';

=head1 NAME

Net::Async::Redis - talk to Redis servers via L<IO::Async>

=head1 SYNOPSIS

    use Net::Async::Redis;
    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    $loop->add(my $redis = Net::Async::Redis->new);
    $redis->connect->then(sub {
        $redis->get('some_key')
    })->then(sub {
        my $value = shift;
        return Future->done($value) if $value;
        $redis->set(some_key => 'some_value')
    })->on_done(sub {
        print "Value: " . shift;
    })->get;

    # ... or with Future::AsyncAwait
    await $redis->connect;
    my $value = await $redis->get('some_key');
    $value ||= await $redis->set(some_key => 'some_value');
    print "Value: $value";

=head1 DESCRIPTION

See L<Net::Async::Redis::Commands> for the full list of commands.

This is intended to be a near-complete low-level client module for asynchronous Redis
support. See L<Net::Async::Redis::Server> for a (limited) Perl server implementation.
It is an unofficial Perl port and not endorsed by the Redis server maintainers in any
way.

=head2 Supported features

Current features include:

=over 4

=item * all commands as of 5.0 RC (July 2018)

=item * L<pub/sub support|https://redis.io/topics/pubsub>, see L</Subscriptions>

=item * L<pipelining|https://redis.io/topics/pipelining>, see L</Pipelining>

=item * L<transactions|https://redis.io/topics/transactions>, see L</Transactions>

=item * L<streams|https://redis.io/topics/streams-intro> and consumer groups, see L</Streams>

=back

=head2 Connecting

As with any other L<IO::Async::Notifier>-based module, you'll need to
add this to an L<IO::Async::Loop>:

    my $loop = IO::Async::Loop->new;
    $loop->add(
        my $redis = Net::Async::Redis->new
    );

then connect to the server:

    $redis->connect
        ->then(sub {
            # You could achieve a similar result by passing client_name in
            # constructor or ->connect parameters
            $redis->client_setname("example client")
        })->get;

=head2 Key-value handling

One of the most common Redis scenarios is as a key/value store. The L</get> and L</set>
methods are typically used here:

 $redis->set(some_key => 'some value')
  ->then(sub {
   $redis->get('some_key')
  })->on_done(sub {
   my ($value) = @_;
   print "Read back value [$value]\n";
  })->retain;

See the next section for more information on what these methods are actually returning.

=head2 Requests and responses

Requests are implemented as methods on the L<Net::Async::Redis> object.
These typically return a L<Future> which will resolve once ready:

    my $future = $redis->incr("xyz")
        ->on_done(sub {
            print "result of increment was " . shift . "\n"
        });

For synchronous code, call C<< ->get >> on that L<Future>:

    print "Database has " . $redis->dbsize->get . " total keys\n";

This means you can end up with C<< ->get >> being called on the result of C<< ->get >>,
note that these are two very different methods:

 $redis
  ->get('some key') # this is being called on $redis, and is issuing a GET request
  ->get # this is called on the returned Future, and blocks until the value is ready

Typical async code would not be expected to use the L<Future/get> method extensively;
often only calling it in one place at the top level in the code.

=head2 Error handling

Since L<Future> is used for deferred results, failure is indicated
by a failing Future with L<failure category|Future/FAILURE-CATEGORIES>
of C<redis>.

The L<Future/catch> feature may be useful for handling these:

 $redis->lpush(key => $value)
     ->catch(
         redis => sub { warn "probably an incorrect type, cannot push value"; Future->done }
     )->get;

=cut

use mro;
use Class::Method::Modifiers;
use curry::weak;
use Syntax::Keyword::Try;
use IO::Async::Stream;
use Ryu::Async;
use URI;
use URI::redis;

use Log::Any qw($log);

use List::Util qw(pairmap);

use Net::Async::Redis::Multi;
use Net::Async::Redis::Subscription;
use Net::Async::Redis::Subscription::Message;

use Net::Async::Redis::Connection;
use Net::Async::Redis::Connection::Subscription;

UNITCHECK {
    # Apply after all other code is compiled for this module: we may want
    # to provide our own implementation in some cases.
    require Net::Async::Redis::Commands;
    Net::Async::Redis::Commands->import;
}

=head1 METHODS

B<NOTE>: For a full list of the Redis methods supported by this module,
please see L<Net::Async::Redis::Commands> or the official Redis documentation
in L<https://redis.io/commands>.

=cut

=head1 METHODS - Subscriptions

See L<https://redis.io/topics/pubsub> for more details on this topic.
There's also more details on the internal implementation in Redis here:
L<https://making.pusher.com/redis-pubsub-under-the-hood/>.

=cut

=head2 psubscribe

Subscribes to a pattern.

Example:

 # Subscribe to 'info::*' channels, i.e. any message
 # that starts with the C<info::> prefix, and prints them
 # with a timestamp.
 $redis_connection->psubscribe('info::*')
    ->then(sub {
        my $sub = shift;
        $sub->map('payload')
            ->each(sub {
             print localtime . ' ' . $_ . "\n";
            })->retain
    })->get;
 # this will block until the subscribe is confirmed. Note that you can't publish on
 # a connection that's handling subscriptions due to Redis protocol restrictions.
 $other_redis_connection->publish('info::example', 'a message here')->get;

=cut

around psubscribe => sub {
    my ($code, $self, $pattern) = @_;

    $self->{psubscribe_request}{$pattern} //=
        $self->$code($pattern)
            ->then(sub {
                $self->{pubsub} //= 0;
                $self->{pending_subscription_pattern_channel}{$pattern} //= $self->future('pattern_subscription[' . $pattern . ']');
            })->on_ready(sub {
                delete $self->{pending_subscription_pattern_channel}{$pattern}
            })->then(sub {
                Future->done(
                    $self->{subscription_pattern_channel}{$pattern} //= Net::Async::Redis::Subscription->new(
                        redis   => $self,
                        type    => 'pattern',
                        channel => $pattern
                    )
                );
            })
};

=head2 subscribe

Subscribes to one or more channels.

Resolves to a L<Net::Async::Redis::Subscription> instance.

Example:

 # Subscribe to 'notifications' channel,
 # print the first 5 messages, then unsubscribe
 $redis->subscribe('notifications')
    ->then(sub {
        my $sub = shift;
        $sub->events
	    ->map('payload')
            ->take(5)
            ->say
            ->completed
    })->then(sub {
        $redis->unsubscribe('notifications')
    })->get

=cut

around subscribe => sub {
    my ($code, $self, @channels) = @_;
    $self->$code(@channels)
        ->then(sub {
            local $log->{context}{redis} = {
                remote   => $self->endpoint,
                local    => $self->local_endpoint,
                name     => $self->client_name,
                database => $self->database,
            } if $log->is_debug;
            $log->tracef('Marking as pubsub mode');
            $self->{pubsub} //= 0;
            Future->wait_all(
                map {
                    $self->{pending_subscription_channel}{$_} //= $self->future('subscription[' . $_ . ']')
                } @channels
            )
        })->then(sub {
            Future->done(
                @{$self->{subscription_channel}}{@channels}
            )
        })
};

=head1 METHODS - Transactions

=head2 multi

Executes the given code in a Redis C<MULTI> transaction.

This will cause each of the requests to be queued on the server, then applied in a single
atomic transaction.

Note that the commands will resolve only after the transaction is committed: for example,
when the L</set> command is issued, Redis will return C<QUEUED> but the L<Future> representing
the response will only be marked as done once the C<EXEC> command is applied and we have the
result back.

Example:

 $redis->multi(sub {
  my $tx = shift;
  $tx->incr('some::key')->on_done(sub { print "Final value for incremented key was " . shift . "\n"; });
  $tx->set('other::key => 'test data')
 })->then(sub {
  my ($success, $failure) = @_;
  return Future->fail("Had $failure failures, expecting everything to succeed") if $failure;
  print "$success succeeded\m";
  return Future->done;
 })->retain;

=cut

around multi => sub {
    use Scalar::Util qw(reftype);
    use namespace::clean qw(reftype);
    my ($orig, $self, $code) = @_;
    die 'Need a coderef' unless $code and reftype($code) eq 'CODE';
    local $log->{context}{redis} = {
        remote   => $self->endpoint,
        local    => $self->local_endpoint,
        name     => $self->client_name,
        database => $self->database,
    } if $log->is_debug;
    my $multi = Net::Async::Redis::Multi->new(
        redis => $self,
    );
    my $task = sub {
        local $self->{_is_multi} = 1;
        $self->$orig
            ->then(sub {
                $multi->exec($code)
            })
    };
    my @pending = @{$self->{pending_multi}};

    $log->tracef('Have %d pending MULTI transactions', 
        0 + @pending
    );
    push @{$self->{pending_multi}}, $self->loop->new_future;
    return $task->() unless @pending;
    return Future->wait_all(
        @pending
    )->then($task);
};

around [qw(discard exec)] => sub {
    my ($code, $self, @args) = @_;
    local $self->{_is_multi} = 1;
    my $f = $self->$code(@args);
    (shift @{$self->{pending_multi}})->done;
    $f
};

=head1 METHODS - Generic

=head2 keys

=cut

around keys => sub : method {
    my ($code, $self, $match) = @_;
    $match //= '*';
    return $self->$code($match);
};

=head2 watch_keyspace

=cut

sub watch_keyspace {
    my ($self, $pattern, $code) = @_;
    $pattern //= '*';
    my $sub = '__keyspace@*__:' . $pattern;
    (
        $self->{have_notify} ||= $self->config_set(
            'notify-keyspace-events', 'Kg$xe'
        )
    )->then(sub {
        $self->psubscribe($sub)
    })->on_done(sub {
        shift->events
            ->each(sub {
                my $data = $_;
                return unless $data eq $sub;
                my ($k, $op) = map $_->{data}, @{$data->{data}}[2, 3];
                $k =~ s/^[^:]+://;
                $code->($op => $k);
            })
    })->retain
}

sub endpoint { shift->{endpoint} }

sub local_endpoint { shift->{local_endpoint} }

=head1 METHODS - Connection

=head2 connect

Attempts to connect to a server.

Will use the L</configure>d parameters if available, but as a convenience
can be passed additional parameters which will then be applied as if you
had called L</configure> with those beforehand. This also means that they
will be preserved for subsequent L</connect> calls.

=cut

sub connect : method {
    use Scalar::Util qw(blessed);
    use namespace::clean qw(blessed);
    my ($self, %args) = @_;
    # There's occasional situations where we may want deferred
    # values here - one example being Net::Async::Redis::Server->uri
    # which only resolves after the listener is ready.
    Future->needs_all(
        map {
            my $k = $_;
            $args{$_}->transform(done => sub {
                return $k => shift
            })
        } grep {
            blessed($args{$_}) && $args{$_}->isa('Future')
        } keys %args
    )->then(sub {
        my %extra = @_;
        @args{keys %extra} = values %extra;
        $self->configure(%args) if %args;
        my $uri = $self->uri->clone;
        my $auth = $uri->password;
        $self->{connection} //= $self->loop->connect(
            service => $uri->port // 6379,
            host    => $uri->host,
            socktype => 'stream',
        )->then(sub {
            my ($sock) = @_;
            $self->{endpoint} = join ':', $sock->peerhost, $sock->peerport;
            $self->{local_endpoint} = join ':', $sock->sockhost, $sock->sockport;

            my $proto = $self->protocol;
            my $stream = IO::Async::Stream->new(
                handle    => $sock,
                on_closed => $self->curry::weak::notify_close,
            );
            my $conn = Net::Async::Redis::Connection->new(
                stream => $stream,
            );
            $stream->configure(
                on_read   => sub {
                    $proto->parse($_[1]);
                    0
                }
            );
            $self->add_child($conn);
            return $self->auth($auth) if defined $auth;
            return Future->done;
        })
    })
}

=head2 connected

Returns the connection L<Future>, and will attempt to initiate a connection
if one is not already in progress.

See L</connect>.

=cut

sub connected { my ($self) = @_; $self->{connected} //= $self->connect }

=head2 pipeline_depth

Number of requests awaiting responses before we start queuing.
This defaults to an arbitrary value of 100 requests.

Note that this does not apply when in L<transaction|METHODS - Transactions> (C<MULTI>) mode,
since those commands only complete once the transaction is executed or discarded.

See L<https://redis.io/topics/pipelining> for more details on this concept.

=cut

sub pipeline_depth { shift->{pipeline_depth} //= 100 }

=head1 METHODS - Internal

=cut

=head2 stream

Represents the L<IO::Async::Stream> instance for the active Redis connection.

=cut

# sub stream { shift->{stream} }

sub notify_close {
    my ($self) = @_;
    local $log->{context}{redis} = {
        remote   => $self->endpoint,
        local    => $self->local_endpoint,
        name     => $self->client_name,
        database => $self->database,
    };
    $log->tracef('Disconnect received from remote');
    if(my $stream = delete $self->{stream}) {
        $stream->configure(on_read => sub { 0 });
        $stream->close_when_empty;
    }
    $_->[1]->fail('Server connection is no longer active', redis => 'disconnected') for splice @{$self->{pending}};
    $self->maybe_invoke_event(disconnect => );
}

sub command_label {
    my ($self, @cmd) = @_;
    return join ' ', @cmd if $cmd[0] eq 'KEYS';
    return $cmd[0];
}

our %ALLOWED_SUBSCRIPTION_COMMANDS = (
    SUBSCRIBE    => 1,
    PSUBSCRIBE   => 1,
    UNSUBSCRIBE  => 1,
    PUNSUBSCRIBE => 1,
    PING         => 1,
    QUIT         => 1,
);

sub execute_command {
    my ($self, @cmd) = @_;
    local $log->{context}{redis} = {
        remote   => $self->endpoint,
        local    => $self->local_endpoint,
        name     => $self->client_name,
        database => $self->database,
        command  => $cmd[0],
    } if $log->is_debug;

    # First, the rules: pubsub or plain
    my $is_sub_command = exists $ALLOWED_SUBSCRIPTION_COMMANDS{$cmd[0]};
    return Future->fail(
        'Currently in pubsub mode, cannot send regular commands until unsubscribed',
        redis =>
            0 + (keys %{$self->{subscription_channel}}),
            0 + (keys %{$self->{subscription_pattern_channel}})
    ) if exists $self->{pubsub} and not $is_sub_command;

    my $f = $self->loop->new_future->set_label($self->command_label(@cmd));
    $log->debugf("Will have to wait for %d MULTI tx", 0 + @{$self->{pending_multi}}) unless $self->{_is_multi};
    my $code = sub {
        local $log->{context}{redis} = {
            remote   => $self->endpoint,
            local    => $self->local_endpoint,
            name     => $self->client_name,
            database => $self->database,
            command  => $cmd[0],
        } if $log->is_debug;
        my $cmd = join ' ', @cmd;
        $log->tracef('Outgoing [%s]', $cmd);
        push @{$self->{pending}}, [ $cmd, $f ];
        $log->tracef("Pipeline depth now %d", 0 + @{$self->{pending}});
        $self->stream->write(
            $self->protocol->encode_from_client(@cmd)
        )->then(sub {
            $f
        })
    };
    return $code->() if $self->{stream} and ($self->{is_multi} or 0 == @{$self->{pending_multi}});
    return (
        $self->{_is_multi}
        ? $self->connected
        : Future->wait_all(
            $self->connected,
            @{$self->{pending_multi}}
        )
    )->then($code);
}

sub ryu {
    my ($self) = @_;
    $self->{ryu} ||= do {
        $self->add_child(
            my $ryu = Ryu::Async->new
        );
        $ryu
    }
}

sub future {
    my ($self) = @_;
    return $self->loop->new_future(@_);
}

sub protocol {
    my ($self) = @_;
    $self->{protocol} ||= do {
        require Net::Async::Redis::Protocol;
        Net::Async::Redis::Protocol->new(
            handler => $self->curry::weak::on_message
        )
    };
}

sub connection { shift->{connection} }
sub stream { shift->{connection}->stream }

sub on_message {
    my ($self, @data) = @_;
    local $log->{context}{redis} = {
        remote   => $self->endpoint,
        local    => $self->local_endpoint,
        name     => $self->client_name,
        database => $self->database,
    };
    try {
        $log->tracef('Incoming message: %s', \@data);
        $self->connection->process($_) for @data;
    } catch {
        $log->errorf('Failed to process message %s - %s', \@data, $@);
    }
}

sub host { shift->uri->host }

sub port { shift->uri->port }

sub database { shift->{database} }

sub client_name { shift->{client_name} }

sub uri { shift->{uri} //= URI->new('redis://localhost') }

sub configure {
    my ($self, %args) = @_;
    $self->{pending_multi} //= [];
    my $uri = $self->uri;

    # We combine any previous URI information with the new args and store the result URI->new($self->{uri}) 
    $uri = delete $args{uri} if exists $args{uri};
    $uri = URI->new($uri) unless ref $uri;
    $uri->host(delete $args{host}) if exists $args{host};
    $uri->port(delete $args{port}) if exists $args{port};
    $self->{uri} = $uri;

    # There's also some metadata that we'll pull from the URI, but allow %args to override
    $self->{database} = $uri->database if defined $uri->database;

    for (qw(auth database pipeline_depth client_name)) {
        $self->{$_} = delete $args{$_} if exists $args{$_};
    }

    $self->next::method(%args)
}

1;

__END__

=head1 SEE ALSO

Some other Redis implementations on CPAN:

=over 4

=item * L<Mojo::Redis2> - nonblocking, using the L<Mojolicious> framework, semi-actively maintained,
implemented to address some design issues with L<Mojo::Redis> (main points appear to be connection
and error handling)

=item * L<MojoX::Redis> - changelog mentions that this was obsoleted by L<Mojo::Redis>, although there
have been new versions released since then

=item * L<Mojo::Redis> - looks like development has restarted on this (June 2018)

=item * L<Redis> - synchronous (blocking) implementation, handles pub/sub and autoreconnect

=item * L<RedisDB> - another synchronous (blocking) implementation, handles pub/sub and autoreconnect

=item * L<Cache::Redis> - wrapper around L<RedisDB>

=item * L<Redis::Fast> - wraps C<hiredis>, faster than L<Redis>

=item * L<Redis::Jet> - also XS-based, docs mention C<very early development stage> but appears to support
pipelining and can handle newer commands via C<< ->command >>.

=back

=head1 AUTHOR

Tom Molesworth <TEAM@cpan.org>

=head1 LICENSE

Copyright Tom Molesworth 2015-2018. Licensed under the same terms as Perl itself.

