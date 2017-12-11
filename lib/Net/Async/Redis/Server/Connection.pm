package Net::Async::Redis::Server;

use strict;
use warnings;

use parent qw(IO::Async::Notifier);

=head1 NAME

Net::Async::Redis::Server::Connection - represents a single connection to a server

=cut

use strict;
use warnings;

use Net::Async::Redis::Commands;

sub AUTOLOAD {
    my ($self, @args) = @_;
    my ($method) = our $AUTOLOAD =~ /::([^:]+)$/;
    if(Net::Async::Redis::Commands->can($method)) {
        my $cmd = uc $method;
        $cmd =~ tr/_/ /;
        return $self->request->reply(ERR => 'Unimplemented command ' . $cmd);
    }
    return $self->request->reply(ERR => 'Unknown command ' . $cmd);
}

1;


