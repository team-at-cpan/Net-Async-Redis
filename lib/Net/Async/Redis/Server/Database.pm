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
use Scalar::Util ();
use Log::Any qw($log);

sub new {
    my ($class, %args) = @_;
    Scalar::Util::weaken($args{server});
    bless \%args, $class
}

sub set : method {
    my ($self, $k, $v, @args) = @_;
    my %opt;
    my %seen;
    OPTIONS:
    while(@args) {
        my $cmd = shift @args;
        if($cmd eq 'EX') {
            die 'cannot provide EX/PX multiple times' if exists $opt{ttl};
            $opt{ttl} = 1000 * shift @args;
            next OPTIONS;
        } elsif($cmd eq 'PX') {
            die 'cannot provide EX/PX multiple times' if exists $opt{ttl};
            $opt{ttl} = shift @args;
            next OPTIONS;
        } elsif($cmd eq 'NX') {
            die 'cannot provide NX/XX multiple times' if exists $opt{xx} or exists $opt{nx};
            $opt{nx} = 1;
            next OPTIONS;
        } elsif($cmd eq 'XX') {
            die 'cannot provide NX/XX multiple times' if exists $opt{xx} or exists $opt{nx};
            $opt{xx} = 1;
            next OPTIONS;
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

sub server { shift->{server} }

sub time : method { shift->server->time }

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

    $pattern =~ s{\?}{.}g;
    $pattern =~ s{\*}{.*}g;
    $pattern = qr/^$pattern$/;
    return Future->done([ grep { /$pattern/ } sort keys %{$self->{keys}} ]);
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
    return Future->done(0 + @{$self->{keys}{$k}});
}

sub llen : method {
    my ($self, $k) = @_;
    return Future->done(0 + @{$self->{keys}{$k}});
}

sub lpop : method {
    my ($self, $k) = @_;
    $self->expiry_check($k);
    my $v = shift @{$self->{keys}{$k}};
    unless(@{$self->{keys}{$k}}) {
        delete $self->{keys}{$k};
        delete $self->{expiry}{$k};
    }
    return Future->done($v);
}

sub rpop : method {
    my ($self, $k) = @_;
    $self->expiry_check($k);
    my $v = pop @{$self->{keys}{$k}};
    unless(@{$self->{keys}{$k}}) {
        delete $self->{keys}{$k};
        delete $self->{expiry}{$k};
    }
    return Future->done($v);
}

sub rpush : method {
    my ($self, $k, @values) = @_;
    push @{$self->{keys}{$k}}, @values;
    return Future->done(0 + @{$self->{keys}{$k}});
}

sub brpoplpush : method {
    my ($self, $src, $dst, $timeout) = @_;
    my $v = pop @{$self->{keys}{$src}};
    unshift @{$self->{keys}{$dst}}, $v;
    return Future->done($v);
}

sub client_list {
    my ($self) = @_;
    # id=110 addr=172.17.0.1:47974 fd=8 name= age=1 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=client
    my @keys = qw(
        id addr fd name age idle flags db sub psub multi
        qbuf qbuf-free obl oll omem events cmd
    );
    my @clients;
    for my $client ($self->{server}->clients) {
        my $info = $client->info;
        push @clients, join ' ', map { $_ . '=' . ($info->{$_} // '') } @keys;
    }
    return Future->done(\@clients);
}

=head1 METHODS - Top-level commands

These are commands that consist of two or more words.

=cut

sub client {
    my ($self, $next, @details) = @_;
    $next = lc $next;
    return Future->done(qq{ERR unknown command 'CLIENT $next'})
        unless my $code = $self->can('client_' . $next);
    $self->$code(@details);
}

sub debug {
    my ($self, $next, @details) = @_;
    $next = lc $next;
    return Future->done(qq{ERR unknown command 'DEBUG $next'})
        unless my $code = $self->can('debug_' . $next);
    $self->$code(@details);
}

sub cluster {
    my ($self, $next, @details) = @_;
    $next = lc $next;
    return Future->done(qq{ERR unknown command 'CLUSTER $next'})
        unless my $code = $self->can('cluster_' . $next);
    $self->$code(@details);
}

sub memory {
    my ($self, $next, @details) = @_;
    $next = lc $next;
    return Future->done(qq{ERR unknown command 'MEMORY $next'})
        unless my $code = $self->can('memory_' . $next);
    $self->$code(@details);
}

1;

