import physicalgraph.cassandra.CassandraConnection
import java.security.MessageDigest
class Migrate {
	
	static void main(String[] args) {

		String host, port, keyspace, migrationsPath
		(host, port, keyspace, migrationsPath) = args

		host = host?:'localhost'
		port = port?:'9042'
		keyspace = keyspace?:'test'
		migrationsPath = migrationsPath?:'../migrations'
		
		println """Running Cassandra Migrations
		Host: $host
		Port: $port
		Keyspace: $keyspace
		Migrations Directory: $migrationsPath
		"""


		File migrationsDir = new File(migrationsPath)

		CassandraConnection connection = new CassandraConnection()
		try {
			connection.connect(host, port as Integer)
			connection.keyspace = keyspace
			connection.setupMigration()
			migrationsDir.eachFile { file ->
				String md5 = calculateMd5(file.text)
				String existingMd5 = connection.getMigrationMd5(file.name)
				if (existingMd5) {
					if (md5 == existingMd5) {
						println "${file.name} was already run"
					} else {
						println "ERROR! md5 of ${file.name} is different from the last time it was run!"
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

	static String calculateMd5(String text) {
 
		String strippedText = ''
		text.eachLine {
			strippedText+=it.trim()
		}
		def digest = MessageDigest.getInstance("MD5")
		return new BigInteger(1,digest.digest(strippedText.getBytes())).toString(16).padLeft(32,"0")
	}
}

