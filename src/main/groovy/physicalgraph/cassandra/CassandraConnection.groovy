package physicalgraph.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Host
import com.datastax.driver.core.Metadata
import com.datastax.driver.core.Session

public class CassandraConnection {
   Cluster cluster
   Session session
   String keyspace

   void connect(String host, int port) {
	  cluster = Cluster.builder().addContactPoint(host).withPort(port).build()
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

   void setupMigration() {
	   def existingMigration = execute("SELECT columnfamily_name FROM System.schema_columnfamilies WHERE keyspace_name='$keyspace' and columnfamily_name = 'migrations';")
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
	   execute("insert into migrations (name, sha) values ('${file.name}','$sha');")
	   execute(file.text)
   }

   String getMigrationMd5(String fileName) {
	   def result = execute("SELECT sha from migrations where name='$fileName'")
	   if (result.isExhausted()) return null
	   return result.all().first().getString('sha')
   }
}
