package physicalgraph.migration

import physicalgraph.Util
import physicalgraph.cassandra.CassandraConnection

class MarkAll {
	
	static void main(String[] args) {

		MigrationParameters parameters = new MigrationParameters()
		
		println "Marking Migrations as run $parameters"

		CassandraConnection connection = new CassandraConnection(parameters)
		try {
			connection.connect()
			connection.keyspace = parameters.keyspace
			connection.setupMigration()
			parameters.migrationsDir.eachFile { file ->
				String md5 = Util.calculateMd5(file.text)
				String existingMd5 = connection.getMigrationMd5(file.name)
				if (!existingMd5) {
					println "Marking migration ${file.name} as run!"
					connection.markMigration(file, md5)
				}
			}
		} finally {
			connection.close()
		}
	}
}

