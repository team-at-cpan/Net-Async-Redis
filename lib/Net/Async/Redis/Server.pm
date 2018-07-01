package Net::Async::Redis::Server;

use strict;
use warnings;

use parent qw(IO::Async::Notifier);

# VERSION

=head1 NAME

Net::Async::Redis::Server - basic server implementation

=head1 DESCRIPTION

Best to wait until the 2.000 release for this one.

=cut

use mro qw(c3);
no indirect;

use URI::redis;
use Net::Async::Redis::Server::Connection;

sub _add_to_loop {
    my ($self, $loop) = @_;
    $self->add_child(
        $self->{listener} = IO::Async::Listener->new(
            on_stream => $self->curry::weak::on_stream
        )
    );
    $self->{uri} = $self->{listener}->listen(
        service => $self->port,
        socktype => 'stream',
        host => $self->host,
    )->transform(done => sub {
        URI->new('redis://' . $self->host . ':' . shift->read_handle->sockport);
    });
    Scalar::Util::weaken($self->{listener});
}

sub host { shift->{host} //= '0.0.0.0' }
sub port { shift->{port} //= 0 }

sub listener { shift->{listener} }

sub on_stream {
    my ($self, $server, $stream) = @_;
    ...
}

sub uri { shift->{uri} // die 'must add ' . __PACKAGE__ . ' to a loop before calling any methods' }

sub configure {
    my ($self, %args) = @_;
    for (qw(auth host port)) {
        $self->{$_} = delete $args{$_} if exists $args{$_};
    }
    $self->next::method(%args);
}

1;

__END__

=head1 AUTHOR

Tom Molesworth <TEAM@cpan.org>

=head1 LICENSE

Copyright Tom Molesworth 2015-2018. Licensed under the same terms as Perl itself.

