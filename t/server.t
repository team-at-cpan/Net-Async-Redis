use strict;
use warnings;

use Test::More;
use Test::Fatal;
use Test::Deep;

use Net::Async::Redis;
use Net::Async::Redis::Server;
use IO::Async::Loop;

use Log::Any::Adapter qw(TAP);

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
        uri => $server->uri
    )->get;
}, undef, 'connection is successful');

ok($client->ping->get, 'can ping');
ok($client->set(xyz => 123)->get, 'can set a value');
is($client->get(xyz => )->get, 123, 'can get that same value');
is($client->get(xyz => )->get, 123, 'still the same value on a subsequent request');
ok($client->set(xyz => 345, PX => 75)->get, 'can set a new value');
is($client->get(xyz => )->get, 345, 'read the new value');
$loop->delay_future(after => 0.080)->get;
is($client->get(xyz => )->get, undef, 'value expires when it should');

done_testing;

