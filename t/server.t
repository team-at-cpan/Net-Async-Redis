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

subtest 'basic ping' => sub {
    ok($client->ping->get, 'can ping');
    done_testing;
};

subtest 'get/set/expiry' => sub {
    cmp_deeply($client->keys('xyz*')->get, [], 'key not in keyspace at the start');
    is($client->exists('xyz')->get, 0, 'key does not exist at the start');
    ok($client->set(xyz => 123)->get, 'can set a value');
    cmp_deeply($client->keys('xyz*')->get, [qw(xyz)], 'key is now listed');
    is($client->get(xyz => )->get, 123, 'can get that same value');
    is($client->get(xyz => )->get, 123, 'still the same value on a subsequent request');
    ok($client->set(xyz => 345, PX => 75)->get, 'can set a new value');
    is($client->get(xyz => )->get, 345, 'read the new value');
    $loop->delay_future(after => 0.080)->get;
    is($client->get(xyz => )->get, undef, 'value expires when it should');
    is($client->del(xyz => )->get, 0, 'can delete without error when key does not exist');
    cmp_deeply($client->keys('xyz*')->get, [], 'key is no longer listed');
    done_testing;
};

subtest 'list handling' => sub {
    is($client->lpush(some_list => 'xxx')->get, 1, 'can push a value');
    is($client->llen(some_list => )->get, 1, 'length is correct');
    is($client->rpop(some_list => )->get, 'xxx', 'can pop that element');
    is($client->del(some_list => )->get, 0, 'delete the key');
    done_testing;
};

subtest 'client commands' => sub {
    note explain $client->client_list->get;
    done_testing;
};

done_testing;

