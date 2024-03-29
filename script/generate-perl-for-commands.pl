#!/usr/bin/env perl 
use strict;
use warnings;

use IO::Async::Loop;
use Net::Async::HTTP;
use Path::Tiny;
use HTML::TreeBuilder;
use Template;
use List::Util qw(first);
use List::UtilsBy qw(extract_by sort_by);

use Log::Any qw($log);
use Log::Any::Adapter qw(Stderr), log_level => 'trace';

my $loop = IO::Async::Loop->new;
$loop->add(
    my $ua = Net::Async::HTTP->new
);

my $data = do {
    my $path = path('commands.html');
    $path->exists ? $path->slurp_utf8 : do {
        my $resp = $ua->GET('https://redis.io/commands')->get;
        $path->spew_utf8(my $txt = $resp->decoded_content);
        $txt
    }
};
my $html = HTML::TreeBuilder->new(no_space_compacting => 1);
$html->parse($data);
$html->eof;
my %commands_by_group;
my @commands;
my %key_finder = (
    PUBLISH   => 1,
    SUBSCRIBE => 1,
    # subscribing to one node is enough in the current cluster design
    PSUBSCRIBE => 1,
    # This one is not detected correctly - thankfully it's always XINFO [CONSUMER|GROUP|STREAM] key
    XINFO     => 2,
);
for my $cmd ($html->look_down(_tag => 'span', class => 'command')) {
    my ($txt) = $cmd->parent->attr('href') =~ m{/commands/([\w-]+)$} or die "failed on " . $cmd->as_text;
    $txt =~ tr/-/_/;
    my @children = $cmd->content_list;
    my $command = join('', extract_by { !ref($_) } @children);
    s/^\s+//, s/\s+$// for $command;

    my $group = $cmd->parent->parent->attr('data-group') or die 'no group for ' . $cmd->as_text;
    push @{$commands_by_group{$group}}, my $info = {
        group   => $group,
        method  => $txt,
        command => $command,
        args    => [ map { s/\h+$//r } map { s/^\h+//r } grep { /\S/ } split /\n/, join '', map { $_->as_text } $cmd->parent->look_down(_tag => 'span', class => 'args') ],
        summary => join("\n", map { $_->as_text } $cmd->parent->look_down(_tag => 'span', class => 'summary')),
    };
    my @args = $info->{args}->@*;
    if(defined(my $idx = first { $args[$_] =~ /\bkey\b/ } 0..$#args)) {
        $key_finder{$info->{command}} //= (0 + split(' ', $command)) + $idx;
    }
    $info->{summary} =~ s{\.$}{};
    $log->debugf("Adding command %s", $info);
}

# This one's more complicated... so we assign manually for now
$key_finder{XGROUP} = 2;

my %partial_commands;
for my $group (sort keys %commands_by_group) {
    $log->infof('%s', $group);
    $commands_by_group{$group} = [ sort_by { $_->{method} } $commands_by_group{$group}->@* ];
    for my $command ($commands_by_group{$group}->@*) {
        $log->infof(' * %s - %s', $command->{method}, $command->{summary});
        $partial_commands{$command->{method}} = $command->{method};
        my @partial;
        for (split '_', $command->{method}) {
            push @partial, $_;
            my $name = join '_', @partial;
            $partial_commands{$name} //= $command->{method};
            $key_finder{uc($name =~ s{_}{ }gr)} //= $key_finder{$command->{command}} + 1 if exists $key_finder{$command->{command}};
        }
    }
}

# These are methods which need to call the real _ version
delete @partial_commands{grep { $partial_commands{$_} eq $_ } keys %partial_commands};

my $tt = Template->new;
$tt->process(\q{[% -%]
package Net::Async::Redis::Commands;

use strict;
use warnings;

# VERSION

=head1 NAME

Net::Async::Redis::Commands - mixin that defines the Redis commands available

=head1 DESCRIPTION

This is autogenerated from the list of commands available in L<https://redis.io/commands>.

It is intended to be loaded by L<Net::Async::Redis> to provide methods
for each available Redis command.

=cut

=head1 PACKAGE VARIABLES

=head2 KEY_FINDER

This maps the argument index for the C<key> information in each command.

=cut

our %KEY_FINDER = (
[% FOR command IN key_finder.keys.sort -%]
    '[% command %]' => [% key_finder.item(command) %],
[% END -%]
);

[% FOR group IN commands.keys.sort -%]
=head1 METHODS - [% group.ucfirst %]

[%  FOR command IN commands.item(group) -%]
=head2 [% command.method %]

[% command.summary %].

[%   IF command.args.size -%]
=over 4

[%    FOREACH arg IN command.args -%]
=item * [% arg %]

[%    END -%]
=back

[%   END -%]
L<https://redis.io/commands/[% command.method.lower.replace('_', '-') %]>

=cut

sub [% command.method %] : method {
    my ($self, @args) = @_;
    $self->execute_command(qw([% command.command %]) => @args)
}

[%  END -%]
[% END -%]

=head1 METHODS - Legacy

These take a subcommand as a parameter and construct the method name by
combining the main command with subcommand - for example, C<< ->xgroup(CREATE => ...) >>
would call C<< ->xgroup_create >>.

=cut

[%  FOR method IN partial_commands.keys.sort -%]
=head2 [% method %]

=cut

sub [% method %] : method {
    my ($self, $cmd, @args) = @_;
    $cmd =~ tr/ /_/;
    my $method = "[% method %]_" . lc($cmd);
    return $self->$method(@args);
}

[% END -%]
1;

__END__

=head1 AUTHOR

Tom Molesworth <TEAM@cpan.org>

=head1 LICENSE

This was autogenerated from the official Redis documentation, which is published
under the L<Creative Commons Attribution-ShareAlike license|https://github.com/redis/redis-doc/blob/master/LICENSE>.

The Perl code is copyright Tom Molesworth 2015-2021, and licensed under the same
terms as Perl itself.

}, {
    commands      => \%commands_by_group,
    key_finder    => \%key_finder,
    partial_commands => \%partial_commands,
}) or die $tt->error;

