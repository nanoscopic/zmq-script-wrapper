#!/usr/bin/perl -w
use strict;
use Java::Makefile;

my $jm = Java::Makefile->new( config_file => 'client_specs.xml' );
$jm->write_makefile("client_Makefile");

$jm = Java::Makefile->new( config_file => 'server_specs.xml' );
$jm->write_makefile("server_Makefile");

$jm = Java::Makefile->new( config_file => 'manager_specs.xml' );
$jm->write_makefile("manager_Makefile");
