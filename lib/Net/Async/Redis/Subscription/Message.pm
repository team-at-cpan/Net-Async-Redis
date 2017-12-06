package Net::Async::Redis::Subscription::Message;

use strict;
use warnings;

=head1 NAME

Net::Async::Redis::Subscription::Message - represents a single message

=head1 SYNOPSIS

=cut

use Scalar::Util qw(weaken);

sub new {
    my ($class, %args) = @_;
    weaken($args{redis} // die 'Must be provided a Net::Async::Redis instance');
    weaken($args{subscription} // die 'Must be provided a Net::Async::Redis::Subscription instance');
    bless \%args, $class;
}

sub redis { shift->{redis} }
sub subscription { shift->{subscription} }
sub channel { shift->subscription->channel }
sub type { shift->{type} }
sub payload { shift->{payload} }

sub DESTROY {
    my ($self) = @_;
    return if ${^GLOBAL_PHASE} eq 'DESTRUCT' or not my $ev = $self->{events};
    $ev->completion->done unless $ev->completion->is_ready;
}

1;

