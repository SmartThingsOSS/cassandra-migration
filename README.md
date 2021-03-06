Simple application for migrating cassandra databases

[![Circle CI](https://circleci.com/gh/SmartThingsOSS/cassandra-migration/tree/master.svg?style=svg)](https://circleci.com/gh/SmartThingsOSS/cassandra-migration/tree/master)
[![codecov.io](https://codecov.io/github/SmartThingsOSS/cassandra-migration/coverage.svg?branch=master)](https://codecov.io/github/SmartThingsOSS/cassandra-migration?branch=master)
[![SmartThingsOSS](https://smartthingsoss.jfrog.io/ui/img/jfrog-platform.d5d5c2a5.svg)](https://smartthingsoss.jfrog.io/ui/repos/tree/General/libs-release-local%2Fsmartthings%2Fcassandra-migration)
### Gradle Resolution
```
repositories {
    maven {
      url "https://smartthingsoss.jfrog.io/artifactory/libs-release-local"
    }
}

dependencies {
    implementation "smartthings:cassandra-migration:${cassandraMigrationVersion}"
}
```

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


### Development status
This is limited right now.  Things that should/could be fixed in no particular order.
* Better error handling for bad parameters
* Better support for non-local environments
* Allow for migrating multiple keyspaces (though this can be handled by leaving keyspace migrations in separate directories)


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




