use 5.010000;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME => 'AnyEvent::RipeRedis',
    VERSION_FROM => 'lib/AnyEvent/RipeRedis.pm', # finds $VERSION
    
    PREREQ_PM => {
      'AnyEvent' => 0,
      'AnyEvent::Handle' => 0,
      'Encode' => 0,
      'Scalar::Util' => 0,
      'Carp' => 0
    },

    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/AnyEvent/RipeRedis.pm', # retrieve abstract from module
       AUTHOR => 'Eugene Ponizovsky <ponizovsky@gmail.com>') : ()),

    INSTALLPRIVLIB => '/www/lib',
    INSTALLSITELIB => '/www/lib',
    INSTALLVENDORLIB => '/www/lib'
);
