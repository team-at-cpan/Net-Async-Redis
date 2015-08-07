package Net::Async::Redis;
# ABSTRACT: redis support for IO::Async
use strict;
use warnings;

use parent qw(IO::Async::Notifier);

our $VERSION = '0.001';

=head1 NAME

Net::Async::Redis - talk to Redis servers via IO::Async

=head1 SYNOPSIS

=head1 DESCRIPTION

=cut

use curry::weak;
use IO::Async::Stream;
use Protocol::Redis;
use JSON::MaybeXS;

=head1 METHODS

=cut

sub protocol {
	shift->{protocol} //= do {
		my $proto = Protocol::Redis->new(api => 1);
		warn "Established $proto\n";
		my $json = JSON::MaybeXS->new->pretty(1);
		$proto->on_message(sub {
			my ($redis, $data) = @_;
			warn "got message: " . $json->encode($data);
		});
		$proto
	}
}

sub keys : method {
	my ($self, $match) = @_;
	warn "Check for keys\n";
	$self->stream->write(
		"KEYS $match\x0D\x0A"
	)
}

sub stream { shift->{stream} }

sub scan {
	my ($self, %args) = @_;
	my $code = $args{each};
	$args{batch} //= $args{count};
}

sub del {
	my ($self, @args) = @_;
}

sub connect {
	my ($self, %args) = @_;
	$self->{connection} //= $self->loop->connect(
		service => 6379,
		host    => "localhost",
		socktype => 'stream',
	)->then(sub {
		my ($sock) = @_;
		my $stream = IO::Async::Stream->new(
			handle => $sock,
			on_closed => $self->curry::weak::notify_close,
			on_read => sub {
				my ($stream, $buffref, $eof) = @_;
				my $len = length($$buffref);
				$self->debug_printf("have %d bytes of data from redis", $len);
				$self->protocol->parse(substr $$buffref, 0, $len, '');
				0
			}
		);
		Scalar::Util::weaken(
			$self->{stream} = $stream
		);
		$self->add_child($stream);
		Future->done
	})
}

sub notify_close {
	my ($self) = @_;
	$self->configure(on_read => sub { 0 });
	$self->maybe_invoke_event(disconnect => );
}

1;

__END__

=head1 SEE ALSO

=head1 AUTHOR

Tom Molesworth <cpan@perlsite.co.uk>

=head1 LICENSE

Copyright Tom Molesworth 2015. Licensed under the same terms as Perl itself.

