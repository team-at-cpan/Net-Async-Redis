use strict;
use warnings;

use Test::More;
use Test::Fatal;

use Net::Async::Redis;
use IO::Async::Loop;
use Variable::Disposition qw(dispose);

plan skip_all => 'set NET_ASYNC_REDIS_HOST or NET_ASYNC_REDIS_URI env var to test' unless exists $ENV{NET_ASYNC_REDIS_HOST} or exists $ENV{NET_ASYNC_REDIS_URI};

# If we have ::TAP, use it - but no need to list it as a dependency
eval {
    require Log::Any::Adapter;
    Log::Any::Adapter->import(qw(TAP));
};

my $loop = IO::Async::Loop->new;
sub redis {
    my ($msg) = @_;
    $loop->add(my $redis = Net::Async::Redis->new);
    is(exception {
        Future->needs_any(
            $redis->connect(
                (exists $ENV{NET_ASYNC_REDIS_HOST} ? (host => $ENV{NET_ASYNC_REDIS_HOST}) : ()),
                (exists $ENV{NET_ASYNC_REDIS_URI} ? (uri => $ENV{NET_ASYNC_REDIS_URI}) : ()),
            ),
            $loop->timeout_future(after => 5)
        )->get
    }, undef, 'can connect' . ($msg ? " for $msg" : ''));
    return $redis;
}

my $subscriber = redis('subscriber');
my $publisher = redis('publisher');

subtest 'standard subscribe' => sub {
    is($publisher->publish('test::nowhere', 'message')->get, 0, 'have no subscribers on initial publish');
    isa_ok(my $sub = $subscriber->subscribe('test::somewhere')->get, 'Net::Async::Redis::Subscription');
    is(exception {
        $subscriber->ping->get
    }, undef, 'can still ping after subscribe');
    like(exception {
        $subscriber->get('test::random_key')->get;
    }, qr/pubsub/, 'but cannot GET while subscribed');
    is(exception {
        Future->wait_any(
            $sub->unsubscribe,
            $loop->timeout_future(after => 5)
        )->get;
    }, undef, 'can unsubscribe without fuss');
    ok(!$sub->subscribed, 'not subscribed any more');
    is(exception {
        dispose($sub);
    }, undef, 'can dispose of subscription, nothing else hanging on to it');

    done_testing;
};

done_testing;

