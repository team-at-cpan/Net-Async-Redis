package Net::Async::Redis::Server::Database;

use strict;
use warnings;

# VERSION

=head1 NAME

Net::Async::Redis::Server::Database - implementation for database-related Redis commands

=head1 DESCRIPTION

See L<Net::Async::Redis::Commands> for the full list of commands.
Not all of them will be implemented yet.

=cut

use Math::Random::Secure ();

sub set : method {
    my ($self, $k, $v, @args) = @_;
    my %opt;
    my %seen;
    while(@args) {
        my $cmd = shift @args;
        if($cmd eq 'EX') {
            $opt{ttl} = 1000 * shift @args;
        } elsif($cmd eq 'PX') {
            $opt{ttl} = shift @args;
        } elsif($cmd eq 'NX') {
            die 'syntax error' if exists $opt{xx};
            $opt{nx} = 1;
        } elsif($cmd eq 'XX') {
            die 'syntax error' if exists $opt{nx};
            $opt{xx} = 1;
        } else {
            die 'syntax error ' . $cmd
        }
    }
    return Future->done('OK') if not exists $self->{keys}{$k} and $opt{xx};
    return Future->done('OK') if exists $self->{keys}{$k} and $opt{nx};
    $self->{keys}{$k} = $v;
    if(exists $opt{ttl}) {
        $self->{expiry}{$k} = $opt{ttl} + $self->time;
    }
    return Future->done('OK');
}

sub expiry_check {
    my ($self, @keys) = @_;
    my $time = $self->time;
    for my $k (@keys) {
        next unless exists $self->{expiry}{$k};
        next if $self->{expiry}{$k} >= $time;
        delete $self->{expiry}{$k};
        delete $self->{keys}{$k};
    }
    return;
}

sub get : method {
    my ($self, $k) = @_;
    $self->expiry_check($k);
    return Future->done($self->{keys}{$k});
}

sub auth : method {
    my ($self, $auth) = @_;
    return Future->done('OK');
}

sub echo : method {
    my ($self, $string) = @_;
    return Future->done($string);
}

sub ping : method {
    my ($self, $string) = @_;
    return Future->done($string // 'PONG');
}

sub quit : method {
    my ($self, $string) = @_;
    $self->connection->close;
    return Future->done('OK');
}

sub del : method {
    my ($self, @keys) = @_;
    $self->expiry_check(@keys);
    my $count = grep defined, delete @{$self->{keys}}{@keys};
    delete @{$self->{expiry}}{@keys};
    return Future->done($count);
}

sub exists : method {
    my ($self, @keys) = @_;
    $self->expiry_check(@keys);
    return Future->done(0 + grep defined, @{$self->{keys}}{@keys});
}

sub expire : method {
    my ($self, $k, $ttl) = @_;
    return $self->pexpire($k => 1000 * $ttl);
}

sub expireat : method {
    my ($self, $k, $time) = @_;
    return $self->pexpireat($k => 1000 * $time);
}

sub pexpire : method {
    my ($self, $k, $ttl) = @_;
    $self->expiry_check($k);
    return Future->done(0) unless exists $self->{keys}{$k};
    $self->{expiry}{$k} = $ttl + $self->time;
    return Future->done(1);
}

sub pexpireat : method {
    my ($self, $k, $time) = @_;
    $self->expiry_check($k);
    return Future->done(0) unless exists $self->{keys}{$k};
    $self->{expiry}{$k} = $time;
    return Future->done(1);
}

sub keys : method {
    my ($self, $pattern) = @_;
    $self->expiry_check(keys %{$self->{keys}});
    $pattern = '*' unless defined($pattern) and length($pattern);
    $pattern = qr/^\Q$pattern\E$/;
    $pattern =~ s{\\\*}{.*}g;
    return Future->done(grep { $_ =~ $pattern } sort keys %{$self->{keys}});
}

sub persist : method {
    my ($self, $k) = @_;
    $self->expiry_check(keys %{$self->{keys}});
    return Future->done(0) unless exists $self->{keys}{$k};
    return Future->done(0) unless exists $self->{expiry}{$k};
    delete $self->{expiry}{$k};
    return Future->done(1);
}

sub randomkey : method {
    my ($self) = @_;
    return Future->done((keys %{$self->{keys}})[Math::Random::Secure::irand keys %{$self->{keys}}]);
}

sub lpush : method {
    my ($self, $k, @values) = @_;
    unshift @{$self->{keys}{$k}}, @values;
    return Future->done('OK');
}

sub rpush : method {
    my ($self, $k, @values) = @_;
    push @{$self->{keys}{$k}}, @values;
    return Future->done('OK');
}

sub brpoplpush : method {
    my ($self, $src, $dst, $timeout) = @_;
    my $v = pop @{$self->{keys}{$src}};
    unshift @{$self->{keys}{$dst}}, $v;
    return Future->done($v);
}

1;

