=head1 NAME

SQLite::VirtualTable::Pivot -- use SQLite's virtual tables to represent pivot tables.

=head1 SYNOPSIS

 $ export SQLITE_CURRENT_DB=/tmp/foo.db
 sqlite3 $SQLITE_CURRENT_DB
 sqlite> .load perlvtab.so
 sqlite> create table object_attributes (id integer, name varchar, value integer);
 sqlite> insert into object_attributes values ( 1, "length", 20 );
 sqlite> insert into object_attributes values ( 1, "color", "red" );
 sqlite> create virtual table object_pivot using perl
           ("SQLite::VirtualTable::Pivot", "object_attributes" );
 sqlite> select * from object_pivot;
    id|color|length
    1|red|20

=head1 DESCRIPTION

A pivot table is a table in which the distinct row values of a column
in one table are used as the names of the columns in another table.

Here's an example:

Given this table :

 Student Subject    Grade
 ------- -------    -----
 Joe     Reading    A
 Joe     Writing    B
 Joe     Arithmetic C
 Mary    Reading    B-
 Mary    Writing    A+
 Mary    Arithmetic C+

Pivoting using the columns "Student" and "Subject" and the value "Grade" would yield :

 Student Arithmetic Reading Writing
 ------- ---------- ------- ----------
 Joe     C          A       B
 Mary    C+         B-      A+

SQLite::VirtualTable::Pivot allows one to manipulate tables
of the first type (Entity-Attribute-Value tables) as though
they were of the second type.

=head1 METHODS

To create a table, use the following syntax :

    create virtual table object_pivot using perl
               ("SQLite::VirtualTable::Pivot", "base_table" );

Or, alternatively

    create virtual table object_pivot using perl
               ("SQLite::VirtualTable::Pivot", "base_table",
                "pivot_row", "pivot_column", "pivot_value" );

where pivot_row, pivot_column and pivot_value are three columns
in the base_table.  The distinct values of pivot_column will be
the names of the new columns in the pivot table.  (The values may
be sanitized to create valid column names.)

After creating the table, it may be used like any other database
table.

Note that if new values are added to the base table, it must be
dropped and re-created, in order for the new columns to appear
in the pivot table.

=head1 EXAMPLE 

 create table students (student, subject, grade, primary key (student,subject)); 
 insert into students values ("Joe", "Reading", "A");
 insert into students values ("Joe", "Writing", "B");
 insert into students values ("Joe", "Arithmetic", "C");
 insert into students values ("Mary", "Reading", "B-");
 insert into students values ("Mary", "Writing", "A+");
 insert into students values ("Mary", "Arithmetic", "C+");

 select load_extension("perlvtab.so");
 create virtual table roster using perl ("SQLite::VirtualTable::Pivot", "students", "student", "subject", "grade");
 select * from roster;

 Student Reading Writing Arithmetic
 ------- ------- ------- ----------
 Joe     A       B       C
 Mary    B-      A+      C+

 select student from roster where writing = "A+";
 Mary

=head1 TODO

    - re-use the existing database handle (requires changes
      to SQLite::VirtualTable)
    - allow modification of the data in the virtual table
    - more optimization

=head1 SEE ALSO

L<SQLite::VirtualTable>

=cut

package SQLite::VirtualTable::Pivot;
use DBI;
use DBIx::Simple;
use Data::Dumper;
use SQLite::VirtualTable::Util qw/unescape/;
use base 'SQLite::VirtualTable';
use base 'Class::Accessor::Contextual';
use SQLite::VirtualTable::Pivot::Cursor;
use Scalar::Util qw/looks_like_number/;
use strict;

our $VERSION = 0.01;

__PACKAGE__->mk_accessors(qw| indexes counts                     |);
__PACKAGE__->mk_accessors(qw| table columns vcolumns             |);
__PACKAGE__->mk_accessors(qw| pivot_row pivot_column pivot_value |);
__PACKAGE__->mk_accessors(qw| pivot_row_type |);

our $dbfile = $ENV{SQLITE_CURRENT_DB} or die "please set SQLITE_CURRENT_DB";
our $db;

#$ENV{TRACE} = 1;
#$ENV{DEBUG} = 1;
$ENV{NO_INFO} = 1;

sub info($)   { return if   $ENV{NO_INFO}; print STDERR "# $_[0]\n"; }
sub debug($)  { return unless $ENV{DEBUG}; print STDERR "# $_[0]\n"; }
sub trace($)  { return unless $ENV{TRACE}; print STDERR "# $_[0]\n"; }

sub _init_db {
    my %args = @_;
    our $db;
    return if defined($db) && !$args{force};
    $db = DBIx::Simple->connect( "dbi:SQLite:dbname=$dbfile", "", "" )
      or die DBIx::Simple->error;
    $db->dbh->do("PRAGMA temp_store = 2"); # use in-memory temp tables
}

sub CREATE {
    trace "(CREATE, got @_)";
    my ( $class, $module, $caller, $virtual_table, $other_table, @pivot_columns ) = @_;
    $other_table = unescape($other_table);
    _init_db();
    my ($pivot_row, $pivot_row_type, $pivot_column, $pivot_value );
    if (@pivot_columns == 3) {
        ($pivot_row, $pivot_column, $pivot_value ) = map unescape($_), @pivot_columns;
        if ($pivot_row =~ / /) {
            ($pivot_row,$pivot_row_type) = split / /, $pivot_row;
        }
    } else {
        my ($sql) = $db->select('sqlite_master', [ 'sql' ] ,{ name => $other_table })->list 
            or die "Could not find table '$other_table' ".$db->error;
        $sql =~ s/^[^\(]*\(//; # remove leading
        $sql =~ s/\)[^\)]*$//; # and trailing "CREATE" declaration, to get columns
        ($pivot_row, $pivot_column, $pivot_value ) = split ',', $sql;
        ($pivot_row_type) = $pivot_row =~ /^\s*\S* (.*)$/;
        for ($pivot_row, $pivot_column, $pivot_value ) {
            s/^\s*//;
            s/ .*$//;
        }
    }
    debug "pivot col is $pivot_column";
    my @columns = (
        $pivot_row,
        $db->query( sprintf(
                "SELECT DISTINCT(%s) FROM %s",
                $pivot_column, $other_table))->flat
    );
    debug "distinct values for $pivot_column in $other_table are @columns";
    my @vcolumns = @columns;  # virtual table column names
    for (@vcolumns) { # make column names legal
        tr/a-zA-Z0-9_//dc;
        $_ = "$pivot_column\_$_" unless $_=~/^[a-zA-Z]/;
    }

    $pivot_row_type ||= "varchar";
    bless {
        name           => $virtual_table,  # the virtual pivot table name
        table          => $other_table,    # the base table name
        columns        => \@columns,       # the base table distinct(pivot_column) values
        vcolumns       => \@vcolumns,      # the names of the virtual pivot table columns
        pivot_row      => $pivot_row,      # the name of the "pivot row" column in the base table
        pivot_row_type => $pivot_row_type, # the column affinity for the pivot row
        pivot_column   => $pivot_column,   # the name of the "pivot column" column in the base table
        pivot_value    => $pivot_value     # the name of the "pivot value" column in the base table
    }, $class;
}
*CONNECT = \&CREATE;

sub DECLARE_SQL {
    my $self = shift;
    return sprintf "CREATE TABLE %s (%s)", $self->table, join ',', $self->vcolumns;
}

our %OpMap = ( 'eq' => '=',  'lt' => '<',  'gt'    => '>',
               'ge' => '>=', 'le' => '<=', 'match' => 'like',);

sub _new_temp_table {
    my ($count) = $db->select('sqlite_temp_master','count(1)')->list;
    return sprintf("temp_%d_%d",$count + 1,$$);
}

sub _do_query {
    my ($self, $cursor, $constraints, $args) = @_;
    my @values = @$args; # bind values for constraints
    for my $constraint (@$constraints) {
        my $value = shift @values;
        my $temp_table = _new_temp_table();
        push @{ $cursor->temp_tables }, $temp_table;
        debug "creating temporary table $temp_table ";
        my $key = $self->pivot_row_type =~ /integer/i ? " INTEGER PRIMARY KEY" : "";
        $db->query( sprintf("CREATE TEMPORARY TABLE %s (%s $key)",
                             $temp_table, $self->pivot_row)
                  ) or die $db->error;

        my ($query,@bind);
        if ($constraint->{column_name} eq $self->pivot_row) {
            $query = sprintf( "INSERT INTO %s SELECT DISTINCT(%s) FROM %s WHERE %s %s ?",
                $temp_table, $self->pivot_row, $self->table, $self->pivot_row, $OpMap{$constraint->{operator}} );
            @bind = ($value);
        } else {
            # TODO distinct?
            $query = sprintf( "INSERT INTO %s SELECT %s FROM %s WHERE %s = ? AND %s %s ?",
                             $temp_table, $self->pivot_row, $self->table,
                             $self->pivot_column, $self->pivot_value,
                             $OpMap{$constraint->{operator}});
            @bind = ( $constraint->{column_name}, $value);
        }
        info "ready to run $query";
        $db->query($query, @bind ) or die $db->error;

        info ("temp table $temp_table is for $constraint->{column_name} $constraint->{operator} $value");
        info ("temp table $temp_table has : ".join ",", $db->select($temp_table,"*")->list);
    }
    debug "created ".scalar @{ $cursor->temp_tables }." temp table(s)";

    my $sql = sprintf( "SELECT a.%s, %s, %s FROM %s a",
                        $self->pivot_row,   $self->pivot_column,
                        $self->pivot_value, $self->table); 
    for my $temp_table ($cursor->temp_tables) {
        $sql .= sprintf( " INNER JOIN %s on %s.%s=a.%s ",
            $temp_table,      $temp_table,
            $self->pivot_row, $self->pivot_row
        );
    }
    $sql .= sprintf(" ORDER BY a.%s", $self->pivot_row);
    trace $sql;

    # TODO move into cursor.pm
    my (@current_row);
    $cursor->{sth} = $db->dbh->prepare( $sql) or die "error in $sql : $DBI::errstr";
    $cursor->sth->execute or die $DBI::errstr;
    $cursor->set( "last" => !( @current_row = $cursor->sth->fetchrow_array ) );
    $cursor->set( current_row => \@current_row );
    $cursor->set( first => 1 );
    $cursor->set( queued => {} );
    $cursor->set( last_row => [] );
    info "ran query, first row is : @current_row";
}

sub OPEN {
    my $self = shift;
    trace "(OPEN $self->{name})";
    return SQLite::VirtualTable::Pivot::Cursor->new({virtual_table => $self})->reset;
}

sub BEST_INDEX {
    my ($self,$constraints,$order_bys) = @_;
    # $order_bys is an arrayref of hashrefs with keys "column" and "direction".
    $self->{indexes} ||= [];
    $self->{counts}  ||= {};
    my $index_number = @{ $self->indexes };
    my $index_name = "index_".$index_number;
    my $cost;
    trace "(BEST_INDEX)";
    my $i = 0;
    my @index_constraints;
    for my $constraint (@$constraints) {
        next unless $constraint->{usable};
        $cost ||= 0;
        my $column_name = $self->{columns}[$constraint->{column}];
        debug "OPERATOR: ".$constraint->{operator}.
            ", USABLE: ".$constraint->{usable}.
            ", COLUMN: ".$column_name;
        $constraint->{arg_index} = $i++;  # index of this constraint as it comes through in @args to FILTER
        $constraint->{omit} = 1;
        push @index_constraints, {
            operator => $constraint->{operator},
            column_name => $column_name
          };
       unless (defined($self->counts->{$column_name})) {
            # TODO cache these (when creating the table?)
            ( $self->counts->{$column_name} ) =
              $db->select( $self->table, 'count(1)',
                { $self->pivot_column => $column_name } )->list;
       }
       my $this_cost = $self->counts->{$column_name};
       #debug "this cost is $this_cost";
       $cost += $this_cost;
    }
    push @{ $self->indexes }, { constraints => \@index_constraints, name => $index_name, cost => $cost };
    unless (defined($cost)) {
        ($cost) = $db->select($self->{table},'count(1)')->flat;
        debug "cost is num of rows which is $cost";
    }
    my $order_by_consumed = 0;
    if ( @$order_bys == 1 )
    {    # only consumed if we are ordering by the pivot_row in ascending order
        if (   $self->columns->[ $order_bys->[0]{column} ] eq $self->pivot_row
            && $order_bys->[0]{direction} == 1 ) {
            $order_by_consumed = 1;
        }
    }
    return ( $index_number, $index_name, $order_by_consumed, $cost );
}

sub FILTER {
    # called after OPEN, before NEXT
    my ($self, $cursor, $idxnum, $idxstr, @args) = @_; 
    trace "(FILTER $cursor)";
    debug "filter -- index chosen was $idxnum ($idxstr) ";
    my $constraints = $self->indexes->[$idxnum]{constraints};
    info "FILTER Is calling _do_query for $cursor";
    $cursor->reset;
    $self->_do_query( $cursor, $constraints, \@args );
    $self->NEXT($cursor);
}

sub EOF {
    my ($self, $cursor ) = @_;
    $cursor->done;
};

sub _row_values_are_equal {
    my $self = shift;
    my ($val1,$val2) = @_;
    return $val1==$val2 if $self->pivot_row_type =~ /integer/i;
    return $val1 eq $val2;
}

sub NEXT {
  my ($self,$cursor) = @_;
  trace "(NEXT $cursor)";
  $cursor->get_next_row;
}

sub COLUMN  {
  my ($self, $cursor, $n) = @_;
  my $value = $cursor->column_value( $self->{columns}[$n] );
  return looks_like_number($value) ? 0 + $value : $value;
}

sub ROWID {
    my ($self, $cursor) = @_;
    return $cursor->row_id;
}

sub CLOSE {
  my ($self,$cursor) = @_;
  trace "(CLOSE $cursor)";
  for ($cursor->temp_tables) {
      $db->query("drop table $_") or warn "error dropping $_: ".$db->error;
  }
}

sub DROP {

}

sub DISCONNECT {}

*DESTROY = \&DISCONNECT;

1;


