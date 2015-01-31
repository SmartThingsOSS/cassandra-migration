package physicalgraph.migration

import physicalgraph.Util
import physicalgraph.cassandra.CassandraConnection
class Migrate {
	
	static void main(String[] args) {

		MigrationParameters parameters = new MigrationParameters()
		
		println "Running Cassandra Migrations $parameters"

		CassandraConnection connection = new CassandraConnection(parameters)
		try {
			connection.connect()
			connection.keyspace = parameters.keyspace
			connection.setupMigration()
			parameters.migrationsDir.eachFile { file ->
				String md5 = Util.calculateMd5(file.text)
				String existingMd5 = connection.getMigrationMd5(file.name)
				if (existingMd5) {
					if (md5 == existingMd5) {
						println "${file.name} was already run"
					} else {
						throw new Exception("ERROR! md5 of ${file.name} is different from the last time it was run!")
					}
				} else {
					println "Running migration ${file.name}"
					connection.runMigration(file, md5)
				}

			}
		} finally {
			connection.close()
		}
	}

}

