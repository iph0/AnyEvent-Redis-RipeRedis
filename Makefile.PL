use 5.008000;

use ExtUtils::MakeMaker;

WriteMakefile(
  NAME => 'AnyEvent::Redis::RipeRedis',
  VERSION_FROM => 'lib/AnyEvent/Redis/RipeRedis.pm', # finds $VERSION
  MIN_PERL_VERSION => '5.008000',
  PREREQ_PM => {
    'AnyEvent'     => 6.01,
    'Encode'       => 0,
    'Scalar::Util' => 0,
    'Digest::SHA'  => 0,
    'Carp'         => 0,
  },
  CONFIGURE_REQUIRES => {
    'ExtUtils::MakeMaker' => 6.64,
  },
  BUILD_REQUIRES => {
    'Test::More'        => 0.98,
    'Test::RedisServer' => 0.14,
    'Test::TCP'         => 2.02,
    'Test::Fatal'       => 0.013,
    version             => 0.77,
  },
  META_MERGE => {
    resources => {
      homepage   => 'https://github.com/iph0/AnyEvent-Redis-RipeRedis',
      bugtracker => 'https://github.com/iph0/AnyEvent-Redis-RipeRedis/issues',
      repository => 'https://github.com/iph0/AnyEvent-Redis-RipeRedis',
      license    => 'http://dev.perl.org/licenses/',
    },
  },
  ($] >= 5.005 ? ## Add these new keywords supported since 5.005
    (ABSTRACT_FROM => 'lib/AnyEvent/Redis/RipeRedis.pm', # retrieve abstract from module
     AUTHOR => 'Eugene Ponizovsky <ponizovsky@gmail.com>') : ()),
  LICENSE => 'perl',
);
