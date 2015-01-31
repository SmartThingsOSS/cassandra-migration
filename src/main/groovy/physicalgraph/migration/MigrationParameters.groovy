package physicalgraph.migration
class MigrationParameters {
	String host = 'localhost'
	int port = 9042
	String keyspace = 'test'
	String migrationsPath = '../migrations'
	String username
	String password

	MigrationParameters() {
		host = System.getProperty('host')?:'localhost'
		port = System.getProperty('port')?System.getProperty('port') as Integer:9042
		keyspace = System.getProperty('keyspace')?:'test'
		migrationsPath = System.getProperty('migrationPath')?:'../migrations'
		username = System.getProperty('username')
		password = System.getProperty('password')
	}

	String toString() {
		"""
		Host: $host
		Port: $port
		Keyspace: $keyspace
		Migrations Directory: $migrationsPath
		"""
	}

	File getMigrationsDir() {
		return new File(migrationsPath)
	}
}
