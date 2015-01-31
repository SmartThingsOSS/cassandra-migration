Simple application for migrating cassandra databases

### What it does

update gradle.properties with the appropriate connection information and location of migration scripts

```gradle run```

The script will loop through each file and do the following:
* generate an md5.
* lookup the file name in a columnfamily called 'migrations'
* if it's not found, run the script stored in the file and store the file name and md5 into 'migrations'
* if it's found, it compares the md5 to the one stored in migrations
	* if they match, report that the migration has already been run
	* if they don't match report that the migration cannot be run

```gradle markAll```

The script will loop through each file and do the following:
* generate an md5.
* lookup the file name in a columnfamily called 'migrations'
* if it's not found, store the file name and md5 into 'migrations' without running the script


#### Shell script support
To run from a unix shell script, first run ```gradle shadow``` to generate a jar file
run ```bin/migrate``` to do the same as gradle run
run ```bin/markAll``` to do the same as gradle markAll
Parameters for host, keyspace, username, password, etc are available.  run bin/migrate -h to see options

This is very limited right now.  Things that should/could be fixed in no particular order.
* Allow for migrating multiple keyspaces
* Better error handling for bad parameters
* Make the shell scripts work from any location, not just root dir




