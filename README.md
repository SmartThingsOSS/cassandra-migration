Simple application for migrating cassandra databases

### Installation
Extract zipfile into a directory

### Usage

Run bin/migrate -h to see options 

Basic usage steps are as follows:

Specify a directory that contains cql scripts plus all of your connection parameters.  The connection parameters default to cassandra localhost with a keyspace of 'test'

The script will loop through each file and do the following:
* generate an md5.
* lookup the file name in a table called 'migrations'
* if it's not found, run the script stored in the file and store the file name and md5 into 'migrations'
* if it's found, it compares the md5 to the one stored in migrations
	* if they match, report that the migration has already been run
	* if they don't match report that the migration cannot be run

If you've already run scripts you can use the -c option to mark them as 'complete'

If you wish to run a single file use the -f option and specify a file


#### Development status
This is very limited right now.  Things that should/could be fixed in no particular order.
* Allow for migrating multiple keyspaces
* Better error handling for bad parameters
* Add override parameter to ignore md5 mismatch

### Executing from source
update gradle.properties with the appropriate connection information and location of migration scripts

```gradle run```

The script will loop through each file and do the following:
* generate an md5.
* lookup the file name in a table called 'migrations'
* if it's not found, run the script stored in the file and store the file name and md5 into 'migrations'
* if it's found, it compares the md5 to the one stored in migrations
	* if they match, report that the migration has already been run
	* if they don't match report that the migration cannot be run

```gradle markAll```

The script will loop through each file and do the following:
* generate an md5.
* lookup the file name in a table called 'migrations'
* if it's not found, store the file name and md5 into 'migrations' without running the script




