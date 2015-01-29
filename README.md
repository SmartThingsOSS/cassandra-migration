Simple application for migrating cassandra databases

This is very limited right now.  Things that should/could be fixed in no particular order.
* Allow login credentials
* Change queries to use prepared statements
* Allow for migrating multiple keyspaces
* Throw exception when bad md5 is found
* Allow generating migration records for manually run scripts
* 

### What it does do

update gradle.properties with the appropriate connection information and location of migration scripts
call ```gradle run```
The script will loop through each file and do the following:
* generate an md5.
* lookup the file name in a columnfamily called 'migrations'
* if it's not found, run the script stored in the file and store the file name and md5 into 'migrations'
* if it's found, it compares the md5 to the one stored in migrations
	* if they match, report that the migration has already been run
	* if they don't match report that the migration cannot be run





