package physicalgraph.migration
class MigrationParameters {
	String host = 'localhost'
	int port = 9042
	String keyspace = 'test'
	String migrationsPath = '../migrations'
	File migrationFile
	String username
	String password
	Class handlerClass

	MigrationParameters() {
		host = System.getProperty('host')?:'localhost'
		port = System.getProperty('port')?System.getProperty('port') as Integer:9042
		keyspace = System.getProperty('keyspace')?:'test'
		migrationsPath = System.getProperty('migrationPath')?:'../migrations'
		String filePath = System.getProperty('migrationFile')
		if (filePath) {
			migrationFile = new File(filePath)
		}
		username = System.getProperty('username')
		password = System.getProperty('password')
		handlerClass = Class.forName(System.getProperty('handlerClass')?:'physicalgraph.migration.MigrationHandler')
	}

	String toString() {
		"""
		Host: $host
		Port: $port
		Keyspace: $keyspace
		Migrations Directory: $migrationsPath
		Migrations File: ${migrationFile?:'All'}
		"""
	}

	File getMigrationsDir() {
		return new File(migrationsPath)
	}
}
