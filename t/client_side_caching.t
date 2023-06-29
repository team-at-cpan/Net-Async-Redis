use strict;
use warnings;

use Test::More;
use Test::Deep;
use Test::Fatal;

use Future::AsyncAwait;
use Net::Async::Redis;
use IO::Async::Loop;

plan skip_all => 'set NET_ASYNC_REDIS_HOST or NET_ASYNC_REDIS_URI env var to test' unless exists $ENV{NET_ASYNC_REDIS_HOST} or exists $ENV{NET_ASYNC_REDIS_URI};

# If we have ::TAP, use it - but no need to list it as a dependency
eval {
    require Log::Any::Adapter;
    Log::Any::Adapter->import(qw(TAP));
};

my $loop = IO::Async::Loop->new;
sub redis {
    my ($msg, %args) = @_;
    $loop->add(my $redis = Net::Async::Redis->new(%args));
    my $exception = exception {
        Future->wait_any(
            $redis->connect(
                host => $ENV{NET_ASYNC_REDIS_HOST} // '127.0.0.1',
                port => $ENV{NET_ASYNC_REDIS_PORT} // '6379',
            ),
            $loop->timeout_future(after => 5)
        )->get
    };
    plan skip_all => 'clientside caching not supported in this Redis instance' if $exception and $exception =~ /does not support .*caching/i;
    is($exception, undef, 'can connect' . ($msg ? " for $msg" : ''));
    return $redis;
}

my $main = redis('main connection', client_side_cache_size => 100);
my $secondary = redis('secondary connection');

await $main->client_side_connection;
await $main->client_side_cache_ready;
my $f = $main->get('some_key');
ok(!($f->is_ready), '->get returns pending future');
$f->cancel;
await $main->set('some_key', 123);
is((await $main->get('some_key')), 123, 'key was set correctly');
ok($main->get('some_key')->is_done, '->get now returns immediate future');
await $main->set('some_key', 456);
is((await $main->get('some_key')), 456, 'key change picked up correctly');
cmp_deeply([ await Future->needs_all(
    $main->get('some_key'),
    $main->get('some_key'),
    $main->get('some_key'),
    $main->get('some_key'),
) ], bag(qw(456) x 4), 'multiple requests all return the same value');

done_testing;

