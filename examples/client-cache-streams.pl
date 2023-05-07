#!/usr/bin/env perl
use strict;
use warnings;

use Net::Async::Redis;
use IO::Async::Loop;

use Future::AsyncAwait;
use Syntax::Keyword::Try;
use Future::Utils qw(fmap_void fmap_concat repeat);

use Log::Any qw($log);
use Log::Any::Adapter qw(Stdout), log_level => 'info';

my $loop = IO::Async::Loop->new;

$loop->add(
    my $redis = Net::Async::Redis->new(
        client_side_cache_size => 100,
    )
);

await $redis->connected;
$redis->clientside_cache_events
    ->each(sub {
        $log->infof('Key change detected for %s', $_)
    });
$log->infof('Set key');
await $redis->set('clientside.cached' => 1);
$log->infof('Get key');
await $redis->get('clientside.cached');
$log->infof('Apply more changes');
await $redis->set('clientside.cached' => 2);
await $redis->llen('clientside.cached.lpush');
await $redis->lpush('clientside.cached.lpush' => 1,2,3);
await $redis->hgetall('clientside.cached.hset');
await $redis->hset('clientside.cached.hset' => abc => 123);
await $redis->hset('clientside.cached.hset' => def => 123);
await $redis->hset('clientside.cached.hset' => ghi => 123);

await $redis->del(
    'example_stream',
);
await $redis->xgroup(
    CREATE => 'example_stream',
    primary_group => '0',
    'MKSTREAM'
);

$log->infof('Read group');
my ($item) = await $redis->xreadgroup(
    GROUP       => 'primary_group',
    $$,
    COUNT       => 1,
    STREAMS     => 'example_stream',
    '>'
);
$log->infof('item is %s', $item);

unless($item->@*) {
    my ($item) = await $redis->xinfo_stream(
        'example_stream',
    );
    $log->infof('xinfo is %s', $item);
}

$log->infof('xadd');
await $redis->xadd(
    example_stream => '*',
    key => 'value',
);
{
    my ($item) = await $redis->xreadgroup(
        GROUP       => 'primary_group',
        $$,
        COUNT       => 1,
        STREAMS     => 'example_stream',
        '>'
    );
    $log->infof('new item is %s', $item);
    $log->infof('ack %s', my $id = $item->[0][1][0][0]);
    $redis->xack(
        example_stream => 'primary_group',
        $id
    );
}
if(0) {
$log->infof('Read ');
my ($item) = await $redis->xread(
    COUNT       => 1,
    STREAMS     => 'example_stream',
    '$',
);
}
$log->infof('xadd');
await $redis->xadd(
    example_stream => '*',
    key => 'value',
);
$log->infof('Start loop');
$loop->run;

