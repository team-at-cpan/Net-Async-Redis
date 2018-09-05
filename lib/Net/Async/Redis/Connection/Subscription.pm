package Net::Async::Redis::Connection::Subscription;

use strict;
use warnings;

## VERSION

use parent qw(Net::Async::Redis::Connection);

=head1 NAME

Net::Async::Redis::Connection::Subscription

=head1 DESCRIPTION

Represents a connection that's currently in pub/sub mode.

=cut

use Log::Any qw($log);

our %ALLOWED_COMMANDS = (
    SUBSCRIBE    => 1,
    PSUBSCRIBE   => 1,
    UNSUBSCRIBE  => 1,
    PUNSUBSCRIBE => 1,
    PING         => 1,
    QUIT         => 1,
);

sub process {
    my ($self, $type, @details) = @_;
    return $log->errorf('Unknown message type [%s]', $type)
        unless my $code = $self->can('handle_' . lc($type));
    return $self->$code(@details);
}

sub handle_message {
    my ($self, $channel, $payload) = @_;
    if(my $sub = $self->{subscription_channel}{$channel}) {
        $sub->events->emit(
            Net::Async::Redis::Subscription::Message->new(
                type         => 'message',
                channel      => $channel,
                payload      => $payload,
                redis        => $self,
                subscription => $sub
            )
        );
    } else {
        local $log->{context}{redis} = {
            remote   => $self->endpoint,
            local    => $self->local_endpoint,
            name     => $self->client_name,
            database => $self->database,
        };
        $log->warnf('Have message for unknown channel [%s]', $channel);
    }
    return;
}

sub handle_pmessage {
    my ($self, $pattern, $channel, $payload) = @_;
    if(my $sub = $self->{subscription_pattern_channel}{$pattern}) {
        $sub->events->emit(
            Net::Async::Redis::Subscription::Message->new(
                type         => 'pmessage',
                pattern      => $pattern,
                channel      => $channel,
                payload      => $payload,
                redis        => $self,
                subscription => $sub
            )
        );
    } else {
        local $log->{context}{redis} = {
            remote   => $self->endpoint,
            local    => $self->local_endpoint,
            name     => $self->client_name,
            database => $self->database,
        };
        $log->warnf('Have message for unknown channel [%s]', $channel);
    }
    return;
}

sub handle_punsubscribe {
    my ($self, $channel, $payload) = @_;
    my $k = 'subscription_pattern_channel';
    --$self->{pubsub};
    if(my $sub = delete $self->{$k}{$channel}) {
        $log->tracef('Removed subscription for [%s]', $channel);
    } else {
        local $log->{context}{redis} = {
            remote   => $self->endpoint,
            local    => $self->local_endpoint,
            name     => $self->client_name,
            database => $self->database,
        };
        $log->warnf('Have unsubscription for unknown channel [%s]', $channel);
    }
}

sub handle_psubscribe {
    my ($self, $channel, $payload) = @_;
    my $k = 'subscription_pattern_channel';
    $log->tracef('Have %s subscription for [%s]', (exists $self->{$k}{$channel} ? 'existing' : 'new'), $channel);
    ++$self->{pubsub};
    $self->{$k}{$channel} //= Net::Async::Redis::Subscription->new(
        redis   => $self,
        type    => 'static',
        channel => $channel
    );
    $self->{'pending_' . $k}{$channel}->done;
}

1;

__END__

=head1 AUTHOR

Tom Molesworth <TEAM@cpan.org>

=head1 LICENSE

Copyright Tom Molesworth 2015-2018. Licensed under the same terms as Perl itself.

