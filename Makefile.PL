use 5.006000;

use ExtUtils::MakeMaker;

WriteMakefile(
  NAME => 'AnyEvent::Redis::RipeRedis',
  VERSION_FROM => 'lib/AnyEvent/Redis/RipeRedis.pm', # finds $VERSION
  MIN_PERL_VERSION => '5.006000',
  PREREQ_PM => {
    'AnyEvent' => 6.01,
    'Encode' => 0,
    'Scalar::Util' => 0,
    'Digest::SHA' => 0,
    'Carp' => 0,
  },
  BUILD_REQUIRES => {
    'Test::More' => 0,
    'Test::RedisServer' => 0,
  },
  META_MERGE => {
    resources => {
      license => 'http://dev.perl.org/licenses/',
      homepage => 'https://github.com/iph0/AnyEvent-Redis-RipeRedis',
      bugtracker => 'https://github.com/iph0/AnyEvent-Redis-RipeRedis/issues',
      repository => 'https://github.com/iph0/AnyEvent-Redis-RipeRedis',
    },
  },
  ($] >= 5.005 ? ## Add these new keywords supported since 5.005
    (ABSTRACT_FROM  => 'lib/AnyEvent/Redis/RipeRedis.pm', # retrieve abstract from module
     AUTHOR => 'Eugene Ponizovsky <ponizovsky@gmail.com>') : ()),
  LICENSE => 'perl',
);
