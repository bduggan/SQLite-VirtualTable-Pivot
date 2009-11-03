#!perl

use Test::More qw/no_plan/;
use File::Basename qw/dirname/;
use FindBin qw/$Bin/;
use File::Temp;
use IO::File;
use strict;

chdir $Bin or die $!;

$ENV{PERL5LIB} = join ':', @INC;

my @tests = "simple";

for my $in (<in/*.sql>) {
    my $dbfile = File::Temp->new();
    $ENV{SQLITE_CURRENT_DB} = "$dbfile";
    my $got = `sqlite3 $dbfile < $in` or die "command failed : $!";
    my $out = $in;
    $out =~ s/in/out/;
    $out =~ s/sql$/out/;
    die "missing $out" unless -e $out;
    my $cmp = join "", IO::File->new("<$out")->getlines;
    is $got, $cmp, "output for $in matches $out";
}


1;


