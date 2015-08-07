use strict;
use warnings;

use Test::More;

use Net::Async::Redis;
use IO::Async::Loop;

my $loop = IO::Async::Loop->new;
$loop->add(my $redis = Net::Async::Redis->new);
ok(my $f = $redis->connect(), 'connect');
isa_ok($f, 'Future');
$loop->await($f);
ok($redis->stream, 'have a stream');
isa_ok($redis->stream, 'IO::Async::Stream');
my @keys = $redis->keys('*')->get;
note "Had " . @keys . " keys back";
note " * $_" for @keys;
$loop->run;

done_testing;


