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
	* Works with IDENTITY columns to auto-generate primary keys.
	
Instead of setting Class::DBI as your base class, use this.

=head1 BUGS

DBD::Sybase currently has a bug where a statement handle can be marked as
active, even though it's not. We override sth_to_objects to call finish() on the handle.

=head1 AUTHORS

Dan Sully E<lt>daniel@cpan.orgE<gt>

Michael Wojcikewicz E<lt>mike@perlpimps.comE<gt>

=head1 SEE ALSO

L<Class::DBI>, L<DBD::Sybase>

=cut

use strict;
use base qw(Class::DBI);

use vars qw($VERSION);

$VERSION = '0.3';

sub _die { require Carp; Carp::croak(@_); } 

# This is necessary to get the last ID back
__PACKAGE__->set_sql(MakeNewObj => <<'');
SET NOCOUNT ON
INSERT INTO __TABLE__ (%s)
VALUES (%s)
SELECT @@IDENTITY


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

	# now find the IDENTITY column
	$sth = $dbh->prepare("sp_help $table");
	$sth->execute();

	# the first two resultsets contain no info about finding the identity column
	$sth->fetchall_arrayref() for 1..2; 
	$col = $sth->fetchall_arrayref(); 

	# the 10th column contains a boolean denoting whether it's an IDENTITY
	my ($identity) = grep($_->[9] == 1, @$col);

	# store the IDENTITY column	
	$class->columns(IDENTITY => $identity->[0]) if $identity;
}

# Fixes a DBD::Sybase problem where the handle is still active.
sub sth_to_objects {
	my ($class, $sth, $args) = @_;

	$class->_croak("sth_to_objects needs a statement handle") unless $sth;

	unless (UNIVERSAL::isa($sth => "DBI::st")) {
		my $meth = "sql_$sth";
		$sth = $class->$meth();
	}

	$sth->finish() if $sth->{'Active'};

	return $class->SUPER::sth_to_objects($sth, $args);
}

sub _insert_row {
	my $self = shift;
	my $data = shift;

	my @identity_columns = $self->columns('IDENTITY');

	eval {
		my @columns = ();
		my @values  = ();

		# Omit the IDENTITY column to let it be Auto Generated
		for my $column (keys %$data) {

			unless ($column eq $identity_columns[0]) {
				push @columns, $column;
				push @values, $data->{$column};
			}
		}

		my $sth = $self->sql_MakeNewObj(
			join(', ', @columns),
			join(', ', map $self->_column_placeholder($_), @columns),
		);

		$self->_bind_param($sth, \@columns);
		$sth->execute(@values);

		my $id = $sth->fetchrow_arrayref()->[0];

		if (@identity_columns == 1 && !defined $data->{$identity_columns[0]}) {
			$data->{$identity_columns[0]} = $id;
		}
	};

	if ($@) {
		my $class = ref($self);

		return $self->_croak("Can't insert new $class: $@",
			'err'    => $@,
			'method' => 'create',
		);
	}

	return 1;
}

1;

__END__
