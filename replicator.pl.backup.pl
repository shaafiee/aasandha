#!"c:\strawberry\perl\bin\perl.exe"

use DBI;
use Net::Ping;
use strict;

#--------------------------------------
our $source_dsn = 'runbaa';
our $source_db = 'nares';
our $source_user = 'Aasandha';
our $source_pass = '';
our $dest_dsn = 'runbaaold';
our $dest_db = 'nares';
our $dest_user = 'Aasandha';
our $dest_pass = '';

our $sourceIP = '192.168.22.3';
our $destinationIP = '192.168.22.11';
#--------------------------------------

our @starttime = localtime(time);

our $pinger = Net::Ping->new();
our %string_line;
our %key_field;

&ping_test;



our %tables = (
		CustomerDetails => [	"cRegNo|2",
					"cRegNo|2",
					"cTitle|2",
					"cTitleDhivehi|2",
					"cFirstName|2",
					"cFirstNameDhivehi|2",
					"cMiddleName|2",
					"cMiddleNameDhivehi|2",
					"cLastName|2",
					"cLastNameDhivehi|2",
					"cCMnName|2",
					"cCMnNameDhivehi|2",
					"cSex|1",
					"cBloodGroup|2",
					"bTempRegn|1",
					"cFtrFirstName|2",
					"cFtrFirstNameDhivehi|2",
					"cFtrMiddleName|2",
					"cFtrMiddleNameDhivehi|2",
					"cFtrLastName|2",
					"cFtrLastNameDhivehi|2",
					"cMtrFirstName|2",
					"cMtrFirstNameDhivehi|2",
					"cMtrMiddleName|2",
					"cMtrMiddleNameDhivehi|2",
					"cMtrLastName|2",
					"cMtrLastNameDhivehi|2",
					"cGFtrFirstName|2",
					"cGFtrFirstNameDhivehi|2",
					"cGFtrMiddleName|2",
					"cGFtrMiddleNameDhivehi|2",
					"cGFtrLastName|2",
					"cGFtrLastNameDhivehi|2",
					"iHomeid|1",
					"dDOBirth|2",
					"cPlcOfBirth|2",
					"cPlcOfBirthDhivehi|2",
					"cBCNo|2",
					"dRegDate|2",
					"dDODeath|2",
					"iUserID|1",
					"cLockStatus|1",
					"cBcLock|1"
				],
		CardIssuing => [	"SNNo|2",
					"SNNo|2",
					"cRegNo|2",
					"ColAdd|2",
					"ColcRegNo|2",
					"DateNo|2",
					"DateIssued|2",
					"Tell|1",
					"Location|1",
					"UserID|1",
					"Reprint|1",
					"Validity|1",
					"imgThumbImpression|2"
				],
		CustomerIdentity => [	"cRegNo|2",
					"cRegNo|2",
					"imgPhotograph|2",
					"imgSignature|2",
					"imgThumbImpression|2",
					"tdate|2",
					"cUserID|1"
				],
		Home =>	[		"iHomeid|1",
					"iHomeid|1",
					"iIslandid|1",
					"iDistrictid|1",
					"iUserID|1",
					"cHomeName|2",
					"cHomeNameDhivehi|2",
					"cRoadName|2",
					"cRoadNameDhivehi|2",
					"cBlockNo|2",
					"cHomeNo|2",
					"cHomeDetails|2",
					"bValidityBit|1"
				],
		Islands => [		"iIslandid|1",
					"iIslandid|1",
					"iAtollid|1",
					"cIslandName|2",
					"cIslandNameDhivehi|2",
					"bValidityBit|1"
				],
		Atolls => [		"iAtollid|1",
					"iAtollid|1",
					"cAtollName|2",
					"cAtollNameDhivehi|2",
					"bValidityBit|1"
				],
		Districts => [		"iDistrictid|1",
					"iDistrictid|1",
					"iIslandid|1",
					"cDistrictName|2",
					"cDistrictNameDhivehi|2",
					"bValidityBit|1"
				],
		Application => [	"ttid|1",
					"ttid|1",
					"cRegNo|2",
					"dAppliedDate|2",
					"iAppliedFor|1",
					"bVerificationStatus|1",
					"cVerifyRejectionReason|2",
					"dVerifiedDate|2",
					"iVerifiedBy|1",
					"bApprovalStatus|1",
					"cApprovalRejectionReason|2",
					"dApprovedDate|2",
					"iApprovedBy|1",
					"bPrintedStatus|1",
					"bValidityBit|1",
					"cuser|1",
					"location|1",
					"cPhotoBit|1",
					"dExpiryDate|2"
				],
		DeathUpdatedDates => [	"AutoID|1",
					"AutoID|1",
					"cRegNo|2",
					"dDoDeath|2",
					"dDoDeathUpdatedDate|2",
					"cUserID|1"
				],
		TempApplication => [	"Autoid|1",
					"Autoid|1",
					"cRegNo|2",
					"dAppliedDate|2",
					"iAppliedUser|1",
					"bVerificationStatus|1",
					"dVerifiedDate|2",
					"iVerifiedBy|1",
					"bApprovalStatus|1",
					"dApprovedDate|2",
					"iApprovedBy|1",
					"bPrintedStatus|1",
					"bValidityBit|1",
					"location|1"
				]
	);

our %id_field = (
		CustomerDetails => "AutoID",
		CardIssuing => "AutoID",
		CustomerIdentity => "AutoID",
		Home => "AutoID",
		Islands => "AutoID",
		Atolls => "AutoID",
		Districts => "AutoID",
		Application => "AutoID",
		DeathUpdatedDates => "AutoID2",
		TempApplication => "AutoID2"
	);

our %s_db;
our %d_db;

#our $s_db = DBI->connect("dbi:ODBC:".$source_dsn, "$source_user", "$source_pass") || die "Unable to open source database\n";
our $s_db = DBI->connect("dbi:ODBC:DSN=$source_dsn;UID=$source_user;PWD=$source_pass;Regional=No;") || die "Unable to open source database\n";
$s_db->{LongReadLen} = 10000000;
$s_db->{LongTruncOk} = 1;
$s_db->do("use $source_db");
#our $d_db = DBI->connect("dbi:ODBC:".$dest_dsn, "$dest_user", "$dest_pass") || die "Unable to open destination database\n";
our $d_db = DBI->connect("dbi:ODBC:DSN=$dest_dsn;UID=$dest_user;PWD=$dest_pass;Regional=No;") || die "Unable to open destination database\n";
$d_db->{LongReadLen} = 10000000;
$d_db->{LongTruncOk} = 1;
$d_db->do("use $dest_db");

foreach my $key_t (keys %id_field) {
	for my $counter (1..3) {
		#$s_db{$key_t."_$counter"} = DBI->connect("dbi:ODBC:".$source_dsn, "$source_user", "$source_pass") || die "Unable to open source database\n";
		$s_db{$key_t."_$counter"} = DBI->connect("dbi:ODBC:DSN=$source_dsn;UID=$source_user;PWD=$source_pass;Regional=No;") || die "Unable to open source database\n";
		$s_db{$key_t."_$counter"}->{LongReadLen} = 10000000;
		$s_db{$key_t."_$counter"}->{LongTruncOk} = 1;
		$s_db{$key_t."_$counter"}->do("use $source_db");
		#$d_db{$key_t."_$counter"} = DBI->connect("dbi:ODBC:".$dest_dsn, "$dest_user", "$dest_pass") || die "Unable to open destination database\n";
		$d_db{$key_t."_$counter"} = DBI->connect("dbi:ODBC:DSN=$dest_dsn;UID=$dest_user;PWD=$dest_pass;Regional=No;") || die "Unable to open destination database\n";
		$d_db{$key_t."_$counter"}->{LongReadLen} = 10000000;
		$d_db{$key_t."_$counter"}->{LongTruncOk} = 1;
		$d_db{$key_t."_$counter"}->do("use $dest_db");
	}
}



&identity_insert('ON');
&replicate;

my @endtime = localtime(time);
print "\n";
print join(' ', @starttime);
print "\n";
print join(' ', @endtime);
print "\n\n";

&identity_insert('OFF');
exit;

sub identity_insert {
	my $todo = shift;

	foreach my $kk (keys %tables) {	
		$d_db->do("SET IDENTITY_INSERT $kk $todo");
		for my $counter (1..3) {
			$d_db{$kk."_$counter"}->do("SET IDENTITY_INSERT $kk $todo");
		}
	}
}

sub ping_test {
	$pinger->ping($sourceIP) || die "Error reaching $sourceIP\n";
	$pinger->ping($destinationIP) || die "Error reaching $destinationIP\n";
}

sub replicate {
	foreach my $key (keys %tables) {
		my @pre_splits = split(/\|/, ${$tables{$key}}[0]);
		$key_field{$key} = $pre_splits[0];
		foreach my $counter (1..$#{$tables{$key}}) {
			my @splits = split(/\|/, ${$tables{$key}}[$counter]);
			$string_line{$key} = $string_line{$key}.$splits[0].",";
		}
		chop $string_line{$key};
	}

	&sync2('Atolls', 'Insert');
	&sync2('Atolls', 'Update');
	&sync2('Islands', 'Insert');
	&sync2('Islands', 'Update');
	&sync2('Districts', 'Insert');
	&sync2('Districts', 'Update');
	&sync2('Home', 'Insert');
	&sync2('Home', 'Update');
	&sync2('TempApplication', 'Insert');
	&sync2('TempApplication', 'Update');
	&sync2('Application', 'Insert');
	&sync2('Application', 'Update');
	&sync2('CustomerDetails', 'Insert');
	&sync2('CustomerDetails', 'Update');
	&sync2('CustomerIdentity', 'Insert');
	&sync2('CustomerIdentity', 'Update');
	&sync2('CardIssuing', 'Insert');
	&sync2('CardIssuing', 'Update');
	&sync2('DeathUpdatedDates', 'Insert');
	&sync2('DeathUpdatedDates', 'Update');

	#foreach my $key (keys %tables) {
		#if ($key eq 'CustomerIdentity' || $key eq 'CardIssuing') {
		#	&sync2($key, 'Insert');
		#	&sync2($key, 'Update');
		#} else {
		#	&sync($key, 'Insert');
		#	&sync($key, 'Update');
		#}
		print "\nFINISHED: ".$key."\n\n";

		#for my $counter (1..3) {
		#	$s_db{$key."_".$counter}->commit;
		#	$s_db{$key."_".$counter}->disconnect;
		#	$d_db{$key."_".$counter}->commit;
		#	$d_db{$key."_".$counter}->disconnect;
		#}
	#}
}

sub sync2 {
	my $table_now = shift;
	my $todo = shift;

	#print "select ".$id_field{$table_now}.", ".$key_field{$table_now}." from ".$table_now."_Temp".$todo."\n"; ###
	my $st_temp = $s_db{$table_now."_1"}->prepare("select ".$id_field{$table_now}.", ".$key_field{$table_now}." from ".$table_now."_Temp".$todo);
	my $rc_temp = $st_temp->execute;

	while (my @row = $st_temp->fetchrow_array) {
		my $st_exists = $d_db{$table_now."_1"}->prepare("select ".$string_line{$table_now}." from ".$table_now." where ".&key_check($table_now, $row[1]));

		my $rc_exists = $st_exists->execute;
		my $record_exists = 0;
		while (my @row_exists = $st_exists->fetchrow_array) {
			if ($#row_exists >= 0) {
				$record_exists = 1;
			}
		}

		if (($todo eq 'Insert' && $record_exists != 1) || $todo eq 'Update') {
			my $st_source = $s_db{$table_now."_2"}->prepare("select ".$string_line{$table_now}." from ".$table_now." where ".&key_check($table_now, $row[1]));
			my $rc_source = $st_source->execute;
			my @row_source = $st_source->fetchrow_array;
			my $_st;

			if ($#row_source >= 0) {
				#&ping_test;
				if ($todo eq 'Insert' || ($todo eq 'Update' && $record_exists ne '1')) {
					my $value_line;
					my $q_line;
					my @values;
					for my $_cnt (0..$#row_source) {
						my @_splits = split(/\|/, ${$tables{$table_now}}[$_cnt + 1]);
						$value_line = $value_line.$_splits[0].",";
						$q_line = $q_line."?,";
						$values[$_cnt] = $row_source[$_cnt];
					}
					chop $value_line;
					chop $q_line;
					$_st = $d_db{$table_now."_2"}->prepare("insert into ".$table_now." (".$string_line{$table_now}.") values (".$q_line.")");
					$_st->execute(@values);
				} else {
					my $value_line;
					my @values;
					for my $_cnt (0..$#row_source) {
						my @_splits = split(/\|/, ${$tables{$table_now}}[$_cnt + 1]);
						$value_line = $value_line.$_splits[0]."=?,";
						$values[$_cnt] = $row_source[$_cnt];
					}
					chop $value_line;
					$_st = $d_db{$table_now."_2"}->prepare("update ".$table_now." set ".$value_line." where ".&key_check($table_now, $row[1]));
					$_st->execute(@values);
				}
			}
		}
		#&ping_test;
		#$s_db{$table_now."_3"}->do("delete from ".$table_now."_Temp".$todo." where ".$id_field{$table_now}." = ".$row[0]);
	}
}

sub sync {
	my $table_now = shift;
	my $todo = shift;

	my $st_temp = $s_db{$table_now."_1"}->prepare("select ".$id_field{$table_now}.", ".$key_field{$table_now}." from ".$table_now."_Temp".$todo);
	#print "select ".$id_field{$table_now}.", ".$key_field{$table_now}." from ".$table_now."_Temp".$todo."\n\n"; ###
	my $rc_temp = $st_temp->execute;

	while (my @row = $st_temp->fetchrow_array) {
		#print "select ".$string_line{$table_now}." from ".$table_now." where ".&key_check($table_now, $row[1])."\n\n"; ###
		my $st_exists = $d_db{$table_now."_1"}->prepare("select ".$string_line{$table_now}." from ".$table_now." where ".&key_check($table_now, $row[1]));
		#if ($table_now eq 'CustomerIdentity') {
		#	print "select ".$string_line{$table_now}." from ".$table_now." where ".&key_check($table_now, $row[1])."\n\n"; ###
		#}
		my $rc_exists = $st_exists->execute;
		my $record_exists = 0;
		while (my @row_exists = $st_exists->fetchrow_array) {
			if ($#row_exists >= 0) {
				$record_exists = 1;
			}
		}

		if (($todo eq 'Insert' && $record_exists ne '1') || $todo eq 'Update') {
			my $st_source = $s_db{$table_now."_2"}->prepare("select ".$string_line{$table_now}." from ".$table_now." where ".&key_check($table_now, $row[1]));
			#if ($table_now eq 'CustomerIdentity') {
			#	print "select ".$string_line{$table_now}." from ".$table_now." where ".&key_check($table_now, $row[1])."\n\n"; ###
			#}
			my $rc_source = $st_source->execute;
			my @row_source = $st_source->fetchrow_array;

			if ($#row_source >= 0) {
				#&ping_test;
				my $value_line;
				if ($todo eq 'Insert' && $record_exists ne '1') {
					my $value_line;
					for my $counter (0..$#row_source) {
						$value_line = $value_line.&field_check($table_now, $counter + 1, $todo, $row_source[$counter]).",";
					}
					chop $value_line;
					$d_db{$table_now."_2"}->do("insert into ".$table_now." (".$string_line{$table_now}.") values (".$value_line.")");
					###print "insert into ".$table_now." (".$string_line{$table_now}.") values (".$value_line.")"."\n\n"; ###
					#if ($table_now eq 'CustomerIdentity') {
					#	print "insert into ".$table_now." (".$string_line{$table_now}.") values (".$value_line.")"."\n\n"; ###
					#}
				}

				if ($todo eq 'Update') {
					my $value_line;
					if ($record_exists ne '1') {
						for my $counter (0..$#row_source) {
							$value_line = $value_line.&field_check($table_now, $counter + 1, 'Insert', $row_source[$counter]).",";
						}
						chop $value_line;
						$d_db{$table_now."_2"}->do("insert into ".$table_now." (".$string_line{$table_now}.") values (".$value_line.")");
						#if ($table_now eq 'CustomerIdentity') {
						#	print "insert into ".$table_now." (".$string_line{$table_now}.") values (".$value_line.")"."\n\n"; ###
						#}
					} else {
						for my $counter (0..$#row_source) {
							$value_line = $value_line.&field_check($table_now, $counter + 1, $todo, $row_source[$counter]).",";
						}
						chop $value_line;
						$d_db{$table_now."_2"}->do("update ".$table_now." set ".$value_line." where ".&key_check($table_now, $row[1]));
						#my $donn = "update ".$table_now." set ".$value_line." where ".&key_check($table_now, $row[1]);
						
						#if ($donn =~ /BM/) {
						#	print "update ".$table_now." set ".$value_line." where ".&key_check($table_now, $row[1])."\n\n"; ###
						#}
					}
				}
			}
		}
		#&ping_test;
		#$s_db{$table_now."_3"}->do("delete from ".$table_now."_Temp".$todo." where ".$id_field{$table_now}." = ".$row[0]);
	}
}

sub field_check {
	my $table_now = shift;
	my $index = shift;
	my $todo = shift;
	my $value = shift;

	my @splits = split(/\|/, ${$tables{$table_now}}[$index]);
	if (length($value) <= 0) {
		if ($splits[1] ne '2') {
			$value = 'null';
		}
	}

	if ($splits[1] eq '2') {
		if ($todo eq 'Update') {
			if (length($value) <= 0) {
				return $splits[0]." = null";
			} else {
				$value =~ s/\'/\'\'/g;
				return $splits[0]." = '".$value."'";
			}
		} else {
			if (length($value) <= 0) {
				return "null";
			} else {
				$value =~ s/\'/\'\'/g;
				return "'".$value."'";
			}
		}
	} else {
		if ($todo eq 'Update') {
			return $splits[0]." = ".$value;
		} else {
			return $value;
		}
	}
}

sub key_check {
	my $table_now = shift;
	my $value = shift;

	my @splits = split(/\|/, ${$tables{$table_now}}[0]);
	if ($splits[1] eq '2') {
		$value =~ s/\'/\'\'/g;
		return $splits[0]." = '".$value."'";
	} else {
		return $splits[0]." = ".$value;
	}
}
