use strict;
use warnings;

use Test::More;
use Test::Deep;
use Net::Async::Redis;

for my $case (
    [ [ qw(subscribe x y z) ] => [qw(x y z)] ],
    [ [ qw(get x) ] => [qw(x)] ],
    [ [ qw(mget x y z) ] => [qw(x y z)] ],
    [ [ qw(set x y) ] => [qw(x)] ],
    [ [ qw(xinfo stream x) ] => [qw(x)] ],
    [ [ qw(xadd x nomkstream * a b c d e f) ] => [qw(x)] ],
) {
    cmp_deeply(
        [ Net::Async::Redis->extract_keys_for_command($case->[0]) ],
        $case->[1],
        'keyspec for ' . join(' ', $case->[0]->@*)
    );
}

done_testing;


