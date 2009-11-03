package SQLite::VirtualTable::Pivot::Cursor;

use base 'Class::Accessor::Contextual';
use Data::Dumper;

__PACKAGE__->mk_accessors(qw| first last done row_id queued |);
__PACKAGE__->mk_accessors(qw| sth current_row               |);
__PACKAGE__->mk_accessors(qw| temp_tables                   |);
__PACKAGE__->mk_accessors(qw| virtual_table                 |);
__PACKAGE__->mk_accessors(qw| id                            |);

sub info($)   { return if   $ENV{NO_INFO}; print STDERR "# $_[0]\n"; }

sub reset {
    my $self = shift;
    $self->set( first       => 1  );
    $self->set( done        => 0  );
    $self->set( queued      => {} );
    $self->set( "last"      => 0  );
    $self->set( current_row => [] );
    $self->set( last_row    => [] );
    $self->set( temp_tables => [] );
    return $self;
}

sub get_next_row {
  my $self = shift;
  $self->{queued}      = {};
  $self->{last_row}    ||= [];
  $self->{current_row} ||= [];
  return if $self->done;
  $self->{row_id}++;
  $self->{done} = $self->{last};
  my $row      = $self->{current_row};
  my $last_row = $self->{last_row};
  my $queued   = $self->{queued};
  $self->{last} = 0;
  while ($self->first || $self->virtual_table->_row_values_are_equal($row->[0],$last_row->[0])) {
    $self->set( first => 0 );
    if (@$row) {
        $queued->{ $row->[1] } = $row->[2];
        $queued->{$self->virtual_table->pivot_row} = $row->[0];
    }
    @$last_row = @$row;
    $self->{last} = !( @$row = $self->sth->fetchrow_array );
    last if $self->{last};
  }
  my $dumped = Dumper($self->queued);
  $dumped =~ s/\n|\s//g;
  info "called next, queued is now $dumped ";
  @$last_row = @$row;
}

sub column_value {
    my $self = shift;
    my $column_name = shift;
    info "column $column_name: $self->{queued}->{$column_name}";
    return $self->queued->{$column_name};
}

1;


