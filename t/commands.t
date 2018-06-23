use strict;
use warnings;

use Test::More;

require_ok 'Net::Async::Redis::Commands';
subtest 'import successful' => sub {
    package Local::Import::Test;
    use Test::More;
    ok(!__PACKAGE__->can('get'), 'start off without a ->get');
    Net::Async::Redis::Commands->import;
    can_ok(__PACKAGE__, 'get') or note explain [ sort keys %Local::Import::Test:: ];
    done_testing();
};
done_testing();

