use ExtUtils::MakeMaker;

WriteMakefile(
    'NAME'		=> 'Class::DBI::Sybase',
    'VERSION_FROM'	=> 'Sybase.pm', 
    'PREREQ_PM'		=> { 
                             'Class::DBI' => 0.36, 
                             'Test::More' => 0.05, 
                           },
    ($] >= 5.005 ?    
      (ABSTRACT_FROM => 'Sybase.pm', 
       AUTHOR     => 'Dan Sully <daniel@cpan.org>') : ()),
);
