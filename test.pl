#!/usr/bin/perl -w
use strict;

$| = 1;

while(1) {
  my $cmd = <STDIN>;
  chomp $cmd;
  last if( $cmd eq 'stop' );
  print "Cmd: $cmd\n";
  
  for( my $i=1;$i<13;$i++ ) {
    sleep(1);
    print "$i\n";
  }
  print "--DONE--\n";
}
