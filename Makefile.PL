use 5.006000;

use ExtUtils::MakeMaker;

####
WriteMakefile(
  NAME => 'AnyEvent::Redis::RipeRedis',
  VERSION_FROM => 'lib/AnyEvent/Redis/RipeRedis.pm', # finds $VERSION

  PREREQ_PM => {
    'AnyEvent' => 5.31,
    'Encode' => 2.23,
    'Scalar::Util' => 1.23,
    'Carp' => 0,
    'Test::More' => 0,
    'Test::MockObject' => 1.09,
  },

  ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
    (ABSTRACT_FROM  => 'lib/AnyEvent/Redis/RipeRedis.pm', # retrieve abstract from module
     AUTHOR => 'Eugene Ponizovsky <ponizovsky@gmail.com>') : ()),
);
