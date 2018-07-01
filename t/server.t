use strict;
use warnings;

use Test::More;
use Test::Fatal;

use Net::Async::Redis;
use Net::Async::Redis::Server;
use IO::Async::Loop;

use Log::Any::Adapter qw(TAP), log_level => 'trace';

my $loop = IO::Async::Loop->new;
$loop->add(
    my $server = Net::Async::Redis::Server->new(
        host => 'localhost',
        port => 0
    )
);
$loop->add(
    my $client = Net::Async::Redis->new
);
is(exception {
    $client->connect(
        # uri => $server->uri
    )->get;
}, undef, 'connection is successful');

ok($client->ping->get, 'can ping');
ok($client->set(xyz => 123)->get, 'can set a value');
is($client->get(xyz => )->get, 123, 'can get that same value');

done_testing;

