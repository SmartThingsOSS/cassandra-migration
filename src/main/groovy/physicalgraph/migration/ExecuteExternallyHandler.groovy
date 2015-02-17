package physicalgraph.migration

import physicalgraph.Util
import physicalgraph.cassandra.CassandraConnection
class ExecuteExternallyHandler {
	
	CassandraConnection connection
	MigrationParameters parameters

	def handle(File file) {
		String md5 = Util.calculateMd5(file.text)
		String existingMd5 = connection.getMigrationMd5(file.name)
		if (existingMd5 && md5 == existingMd5) {
			println "${file.name} was already run"
		} else if (existingMd5 && !parameters.override){
			throw new Exception("ERROR! md5 of ${file.name} is different from the last time it was run!")
		} else {
			println "Running migration ${file.name}"

			connection.markMigration(file, md5)

			String command = "${parameters.location} -f ${file.absolutePath} -k ${parameters.keyspace} -h ${parameters.host}"
			println command
			command.execute()
		}
	}
}

