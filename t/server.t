use strict;
use warnings;

use Test::More;

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
$client->connect(uri => $server->uri)->get;

done_testing;


