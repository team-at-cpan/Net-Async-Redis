requires 'parent', 0;
requires 'curry', 0;
requires 'Future', '>= 0.30';
requires 'Mixin::Event::Dispatch', '>= 1.006';
requires 'Protocol::Redis', 0;
requires 'IO::Async', 0;

on 'test' => sub {
	requires 'Test::More', '>= 0.98';
};

