package physicalgraph.migration

import physicalgraph.Util
import physicalgraph.cassandra.CassandraConnection
class ExecuteExternallyHandler {
	
	CassandraConnection connection
	MigrationParameters parameters

	def handle(File file) {
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

			connection.markMigration(file, md5)

			List<String> statements = file.text.split(';')
			statements.each {
				if (it.trim()) {
					String command = "${parameters.location} -f ${file.absolutePath} -k ${parameters.keyspace}"
					println command
					command.execute()
				}
			}
		}
	}
}

