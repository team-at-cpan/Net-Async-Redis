use strict;
use warnings;

use Test::More;
use Test::Deep;

use Net::Async::Redis;
use IO::Async::Loop;

plan skip_all => 'set NET_ASYNC_REDIS_HOST env var to test, but be prepared for it to *wipe that redis instance*' unless exists $ENV{NET_ASYNC_REDIS_HOST};

# If we have ::TAP, use it - but no need to list it as a dependency
eval {
    require Log::Any::Adapter;
    Log::Any::Adapter->import(qw(TAP));
};

my $loop = IO::Async::Loop->new;
my $timeout = $loop->delay_future(after => 15)->on_done(sub {
    BAIL_OUT('timeout');
});
$loop->add(my $redis = Net::Async::Redis->new);
ok(my $f = $redis->connect(
    host => $ENV{NET_ASYNC_REDIS_HOST} // '127.0.0.1',
    port => $ENV{NET_ASYNC_REDIS_PORT} // '6379',
), 'connect');
isa_ok($f, 'Future');
$loop->await($f);
ok($redis->stream, 'have a stream');
isa_ok($redis->stream, 'IO::Async::Stream');
# Switch to database index 1, since that might be a tiny bit safer for
# cases where people are running this against a real, live, filled-with-important-data
# instance of Redis
$redis->select(1)->get;
$redis->flushdb;
my @keys = $redis->keys('*')->get;
note "Had " . @keys . " keys back";
note " * $_" for @keys;
note "Set key";
$redis->set(xyz => 'test')->get;
note "Get key";
is($redis->get('xyz')->get, 'test');
note "Delete key";
is($redis->del('xyz')->get, 1, 'deleted a single key');
note "Get key";
cmp_deeply([ $redis->exists('xyz')->get ], [ 0 ], 'no longer exists');

subtest 'redis database parameter' => sub {
    $loop->add(my $redis = Net::Async::Redis->new);
    ok(my $f = $redis->connect(
        host => $ENV{NET_ASYNC_REDIS_HOST} // '127.0.0.1',
        port => $ENV{NET_ASYNC_REDIS_PORT} // '6379',
        database => 2,
    ), 'connect');
    isa_ok($f, 'Future');
    $loop->await($f);
    my $id = "client_db_test_$$";
    $redis->client_setname($id)->get;
    my $client_info = $redis->client_list->get;
    my (%info) = map { /(\w+)=(\S+)\s*/g } grep { /name=\Q$id/ } split /\n/, $client_info;
    is($info{db}, 2, 'database was selected correctly');
    done_testing;
};
done_testing;


