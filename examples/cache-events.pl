#!/usr/bin/env perl
use strict;
use warnings;

use Net::Async::Redis;
use IO::Async::Loop;

use Future::AsyncAwait;
use Syntax::Keyword::Try;
use Future::Utils qw(fmap_void fmap_concat repeat);

use Log::Any qw($log);
use Log::Any::Adapter qw(Stdout), log_level => 'trace';

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
await $redis->set('clientside.cached' => 2);
$loop->run;

