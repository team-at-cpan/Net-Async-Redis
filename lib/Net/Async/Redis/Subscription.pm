package Net::Async::Redis::Subscription;

use strict;
use warnings;

=head1 NAME

Net::Async::Redis - talk to Redis servers via L<IO::Async>

=head1 SYNOPSIS

=cut

use Scalar::Util qw(weaken);

sub new {
    my ($class, %args) = @_;
    weaken($args{redis} // die 'Must be provided a Net::Async::Redis instance');
    bless \%args, $class;
}

sub events {
    my ($self) = @_;
    $self->{events} ||= do {
        my $ryu = $self->redis->ryu->source(
            label => $self->channel
        );
        $ryu->completed->on_ready(sub {
            weaken $self
        });
        $ryu
    };
}

sub redis { shift->{redis} }
sub channel { shift->{channel} }

sub DESTROY {
    my ($self) = @_;
    return if ${^GLOBAL_PHASE} eq 'DESTRUCT' or not my $ev = $self->{events};
    $ev->completion->done unless $ev->completion->is_ready;
}

1;

