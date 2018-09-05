package Net::Async::Redis::Connection;

use strict;
use warnings;

use mro;

# VERSION

use parent qw(IO::Async::Notifier);

=head1 NAME

Net::Async::Redis::Connection

=head1 DESCRIPTION

Represents a connection to a Redis server.

=cut

=head2 on_message

Called for each incoming message.

Passes off the work to L</handle_pubsub_message> or the next queue
item, depending on whether we're dealing with subscriptions at the moment.

=cut

sub process {
    my ($self, $data) = @_;

    my $next = shift @{$self->{pending}} or die "No pending handler";
    $next->[1]->done($data);
}

sub configure {
    my ($self, %args) = @_;
    for my $k (qw(stream)) {
        $self->{$k} = delete $args{$k} if exists $args{$k};
    }
    return $self->next::method(%args);
}

sub stream { shift->{stream} }

sub _add_to_loop {
    my ($self) = @_;
    $self->add_child($self->{stream} or die 'must have a stream before adding to the loop');
    Scalar::Util::weaken($self->{stream});
}

1;

__END__

=head1 AUTHOR

Tom Molesworth <TEAM@cpan.org>

=head1 LICENSE

Copyright Tom Molesworth 2015-2018. Licensed under the same terms as Perl itself.

