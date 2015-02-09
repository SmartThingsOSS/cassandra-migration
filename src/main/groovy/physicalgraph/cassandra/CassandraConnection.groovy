package physicalgraph.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session

import physicalgraph.migration.MigrationParameters

public class CassandraConnection {
	Cluster cluster
	Session session
	String keyspace

	String host
	int port
	String username
	String password

	public CassandraConnection() {}
	public CassandraConnection(MigrationParameters parameters) {
		this.host = parameters.host
		this.port = parameters.port
		this.username = parameters.username
		this.password = parameters.password
	}

	void connect() {
		def builder = Cluster.builder()
			.addContactPoint(host)
			.withPort(port)

		if (username && password) {
			builder.withCredentials(username, password)
		}

		cluster = builder.build()
		session = cluster.connect()
	}

	void close() {
		cluster.close()
	}

	void setKeyspace(String keyspace) {
		this.keyspace = keyspace
		execute("use $keyspace")
	}

	def execute(String query) {
		return session.execute(query)
	}
	def execute(String query, Object... params) {
		return session.execute(query, params)
	}

	void setupMigration() {
		def existingMigration = execute("""
		SELECT columnfamily_name 
		FROM System.schema_columnfamilies 
		WHERE keyspace_name=? and columnfamily_name = 'migrations';""", keyspace)

		if (existingMigration.size() == 0) {
			execute(
				"""CREATE TABLE IF NOT EXISTS migrations (
						name text,
						sha text,
						PRIMARY KEY (name) 
				);
				"""
			)
		}
	}

	void runMigration(File file, String sha) {
		if(markMigration(file, sha)){
			List<String> statements = file.text.split(';')
			statements.each {
				if (it.trim()) {
					execute("$it;")
				}
			}
		} else {
			println "Not running ${file} as another process has already marked it."
		}


	}

	boolean markMigration(File file, String sha) {
		def result = execute("INSERT INTO migrations (name, sha) VALUES (?, ?) IF NOT EXISTS;", file.name, sha)
		//We know by having IF NOT EXISTS there will always be a row returned with a column of applied
		return result.all().get(0).getBool("applied")
	}

	String getMigrationMd5(String fileName) {
		def result = execute("SELECT sha FROM migrations WHERE name=?", fileName)
		if (result.isExhausted()) return null
		return result.all().first().getString('sha')
	}
}
