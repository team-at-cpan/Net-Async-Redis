use strict;
use warnings;

use Test::More;
use Test::Fatal;

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
    my ($msg) = @_;
    $loop->add(my $redis = Net::Async::Redis->new);
    is(exception {
        Future->needs_any(
            $redis->connect(
                host => $ENV{NET_ASYNC_REDIS_HOST} // '127.0.0.1',
                port => $ENV{NET_ASYNC_REDIS_PORT} // '6379',
            ),
            $loop->timeout_future(after => 5)
        )->get
    }, undef, 'can connect' . ($msg ? " for $msg" : ''));
    return $redis;
}

my $subscriber = redis('subscriber');
my $publisher = redis('publisher');
is($publisher->publish('test::nowhere', 'message')->get, 0, 'have no subscribers on initial publish');
isa_ok(my $sub = $subscriber->subscribe('test::somewhere')->get, 'Net::Async::Redis::Subscription');
is(exception {
    $subscriber->ping->get
}, undef, 'can still ping after subscribe');

if($subscriber->{protocol_level} eq 'resp2') {
    like(exception {
            note 'start';
        $subscriber->get('test::random_key')->get;
            note 'end';
    }, qr/pubsub/, 'but cannot GET while subscribed');
} else {
    is(exception {
        $subscriber->get('test::random_key')->get;
    }, undef, 'can call GET while subscribed');
}

$sub->events->each(sub {
    note "Event - " . $_->payload;
});
is(exception {
    $publisher->publish('test::somewhere' => 'example data')->get;
}, undef, 'can publish without problems');

# test that we detect and handle cases when redis closes the connection
my $redis1 = Net::Async::Redis->new();
$loop->add($redis1);
my $rfut = $redis1->connect(
        host => $ENV{NET_ASYNC_REDIS_HOST} // '127.0.0.1',
        port => $ENV{NET_ASYNC_REDIS_PORT} // '6379',
    )->then(
        sub { $redis1->subscribe('non-existent-topic') }
    );
$rfut->await;
my $sub_future;
my $f = Future->needs_any(
    $loop->timeout_future(after => 5),
    $rfut->then(
        async sub {
            my $s = shift->events->map('payload');
            $s->each(sub {});
            $sub_future = $s->completed;
            return $sub_future;
        }
    ),
);
$redis1->{stream}->close;
$f->await;
ok $sub_future->is_ready, "redis disconnect has been handled";

done_testing;


