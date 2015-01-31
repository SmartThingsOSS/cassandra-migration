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
				"""CREATE TABLE migrations (
						name varchar,
						sha varchar,
						PRIMARY KEY (name) 
				);
				"""
			)
		}
	}

	void runMigration(File file, String sha) {
		markMigration(file, sha)
		execute(file.text)
	}
	void markMigration(File file, String sha) {
		execute("insert into migrations (name, sha) values (?, ?);", file.name, sha)
	}

	String getMigrationMd5(String fileName) {
		def result = execute("SELECT sha from migrations where name=?", fileName)
		if (result.isExhausted()) return null
		return result.all().first().getString('sha')
	}
}
