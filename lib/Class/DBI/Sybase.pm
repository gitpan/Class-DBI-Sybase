package Class::DBI::Sybase;

=head1 NAME

Class::DBI::Sybase - Extensions to Class::DBI for Sybase

=head1 SYNOPSIS

  package Music::DBI;
  use base 'Class::DBI::Sybase';
  Music::DBI->set_db('Main', 'dbi:Sybase:server=$server', 'username', 'password');

  package Artist;
  use base 'Music::DBI';
  __PACKAGE__->set_up_table('Artist');
  
  # ... see the Class::DBI documentation for details on Class::DBI usage

=head1 DESCRIPTION

This is an extension to Class::DBI that currently implements:

	* Automatic column name discovery.
	
Instead of setting Class::DBI as your base class, use this.

=head1 BUGS

DBD::Sybase currently has a bug where a statement handle can be marked as
active, even though it's not. We override sth_to_objects to call finish() on the handle.

=head1 AUTHOR

Dan Sully E<lt>daniel@cpan.orgE<gt>

=head1 SEE ALSO

L<Class::DBI>, L<DBD::Sybase>

=cut

use strict;
use base 'Class::DBI';

use vars qw($VERSION);
$VERSION = '0.1';

sub _die { require Carp; Carp::croak(@_); } 

sub set_up_table {
	my($class, $table) = @_;
	my $dbh = $class->db_Main();

	$class->table($table);

	# find the primary key and column names.
	my $sth = $dbh->prepare("sp_columns $table");
	   $sth->execute();

	my $col = $sth->fetchall_arrayref;
	   $sth->finish();

	_die('The "'. $class->table() . '" table has no primary key') unless $col->[0][3];

	$class->columns(All => map {$_->[3]} @$col);
	$class->columns(Primary => $col->[0][3]);
}

# Fixes a DBD::Sybase problem where the handle is still active.
sub sth_to_objects {
	my ($class, $sth, $args) = @_;

	$class->_croak("sth_to_objects needs a statement handle") unless $sth;

	unless (UNIVERSAL::isa($sth => "DBI::st")) {
		my $meth = "sql_$sth";
		$sth = $class->$meth();
	}

	$sth->finish() if $sth->{Active};

	return $class->SUPER::sth_to_objects($sth, $args);
}

1;

__END__
