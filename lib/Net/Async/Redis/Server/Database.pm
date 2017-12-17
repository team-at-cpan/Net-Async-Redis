package Net::Async::Redis::Server::Database;

use strict;
use warnings;

sub set {
    my ($self, $k, $v, @args) = @_;
    my %opt;
    while(@args) {
        my $cmd = shift @args;
        if($cmd eq 'EX') {
            $opt{ttl} = 1000 * shift @args;
        } elsif($cmd eq 'PX') {
            $opt{ttl} = shift @args;
        } elsif($cmd eq 'NX') {
            die 'Cannot set NX and XX' if exists $opt{xx};
            $opt{nx} = 1;
        } elsif($cmd eq 'XX') {
            die 'Cannot set NX and XX' if exists $opt{nx};
            $opt{xx} = 1;
        } else {
            die 'Invalid input: ' . $cmd
        }
    }
    return Future->done(undef) if not exists $self->{keys}{$k} and $opt{xx};
    return Future->done(undef) if exists $self->{keys}{$k} and $opt{nx};
    $self->{keys}{$k} = $v;
    if(exists $opt{ttl}) {
        $self->{expiry}{$k} = $opt{ttl} + $self->time;
    }
    return Future->done('OK');
}

sub get {
    my ($self, $k) = @_;
    return Future->done($self->{keys}{$k});
}

1;

