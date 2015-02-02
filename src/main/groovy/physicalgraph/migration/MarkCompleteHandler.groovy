package physicalgraph.migration

import physicalgraph.Util
import physicalgraph.cassandra.CassandraConnection

class MarkCompleteHandler {

	CassandraConnection connection
	MigrationParameters parameters

	def handle(File file) {
		String md5 = Util.calculateMd5(file.text)
		String existingMd5 = connection.getMigrationMd5(file.name)
		if (!existingMd5) {
			println "Marking migration ${file.name} as run!"
			connection.markMigration(file, md5)
		}
	}
}

