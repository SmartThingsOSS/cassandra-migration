package physicalgraph.migration

import physicalgraph.cassandra.CassandraConnection
class MigrationExecutor {
	static void main(String[] args) {

		MigrationParameters parameters = new MigrationParameters()
		
		println "Running Cassandra Task $parameters"

		CassandraConnection connection = new CassandraConnection(parameters)
		def handler = parameters.handlerClass.newInstance(connection:connection, parameters:parameters)
		try {
			connection.connect()
			connection.keyspace = parameters.keyspace
			connection.setupMigration()
			if (parameters.migrationFile) {
				handler.handle(parameters.migrationFile)
			} else {
				parameters.migrationsDir.eachFile { file ->
					handler.handle(file)
				}
			}
		} finally {
			connection.close()
		}
	}
}
