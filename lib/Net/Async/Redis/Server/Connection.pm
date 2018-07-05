package Net::Async::Redis::Server::Connection;

use strict;
use warnings;

use parent qw(IO::Async::Notifier);

# VERSION

=head1 NAME

Net::Async::Redis::Server::Connection - represents a single connection to a server

=head1 DESCRIPTION

Used internally by L<Net::Async::Redis::Server>.

=cut

use strict;
use warnings;

use Net::Async::Redis::Server::Database;
use Net::Async::Redis::Commands;

use Log::Any qw($log);

sub AUTOLOAD {
    my ($self, @args) = @_;
    my ($method) = our $AUTOLOAD =~ /::([^:]+)$/;
    my $cmd = uc $method;
    if(Net::Async::Redis::Commands->can($method)) {
        $cmd =~ tr/_/ /;
        return $self->request->reply(ERR => 'Unimplemented command ' . $cmd);
    }
    return $self->request->reply(ERR => 'Unknown command ' . $cmd);
}

sub request { }

sub stream { shift->{stream} }

sub on_close {
    my ($self) = @_;
    $log->infof('Closing server connection');
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

sub on_read {
    my ($self, $stream, $buffref, $eof) = @_;
    $log->infof('Read %d bytes of data, EOF = %d', length($$buffref), $eof ? 1 : 0);
    $self->protocol->decode($buffref);
    0
}

sub on_message {
    my ($self, $msg) = @_;
    $log->infof('Had message %s', $msg);
    my ($command, @data) = @$msg;
    my $db = $self->db;
    my $code = $db->can(lc $command);
    $log->tracef('Database method: %s', $code);
    (
        $code 
        ? $code->($db, @data)
        : Future->done(qq{ERR unknown command '$command'})
    )->then(sub {
        my $data = shift;
        $self->stream->write(
            $self->protocol->encode($data)
        )
    })->retain;
}

sub db {
    my ($self) = @_;
    $self->{db} //= Net::Async::Redis::Server::Database->new
}

sub configure {
    my ($self, %args) = @_;
    if(exists $args{stream}) {
        my $stream = delete $args{stream};
        $self->add_child($stream);
        Scalar::Util::weaken($self->{stream} = $stream);
        $stream->configure(
            on_closed => $self->curry::weak::on_close,
            on_read   => $self->curry::weak::on_read,
        );
    }
    for (qw(server)) {
        Scalar::Util::weaken($self->{$_} = delete $args{$_}) if exists $args{$_};
    }
    for (qw(protocol)) {
        $self->{$_} = delete $args{$_} if exists $args{$_};
    }
    $self->next::method(%args);
}

1;


