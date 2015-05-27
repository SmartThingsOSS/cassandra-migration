package st.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import groovy.lang.Closure;
import groovy.transform.CompileStatic;
import groovy.util.logging.Slf4j;
import org.codehaus.groovy.runtime.DefaultGroovyMethods;
import org.codehaus.groovy.runtime.ResourceGroovyMethods;
import org.codehaus.groovy.runtime.StringGroovyMethods;
import st.migration.MigrationParameters;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class CassandraConnection implements AutoCloseable {

	public CassandraConnection(MigrationParameters parameters) {
		this.host = parameters.getHost();
		this.port = parameters.getPort();
		this.username = parameters.getUsername();
		this.password = parameters.getPassword();
	}

	public void connect() {
		Cluster.Builder builder = Cluster.builder().addContactPoint(host).withPort(port);

		if (username != null && password != null) {
			builder.withCredentials(username, password);
		}

		cluster = builder.build();
		session = cluster.connect();
	}

	@Override
	public void close() {
		cluster.close();
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
		execute("use " + keyspace);
	}

	public ResultSet execute(String query) {
		return session.execute(query);
	}

	public ResultSet execute(String query, Object... params) {
		return session.execute(query, params);
	}

	public void setupMigration() {
		ResultSet existingMigration = execute("SELECT columnfamily_name " +
				"FROM System.schema_columnfamilies	" +
				"WHERE keyspace_name=? and columnfamily_name = 'migrations';",
			keyspace);

		if (DefaultGroovyMethods.size(existingMigration) == 0) {
			execute("CREATE TABLE IF NOT EXISTS migrations " +
				"(name text, sha text, " +
				"PRIMARY KEY (name));");
		}

	}

	public void runMigration(File file, String sha, boolean override) {
		if (markMigration(file, sha, override)) {
			List<String> statements = null;
			try {
				statements = Arrays.asList(ResourceGroovyMethods.getText(file).split(";"));
			} catch (IOException e) {
				e.printStackTrace();
			}
			DefaultGroovyMethods.each(statements, new Closure<ResultSet>(this, this) {
				public ResultSet doCall(String it) {
					if (StringGroovyMethods.asBoolean(it.trim())) {
						return execute(it + ";");
					}
					return null;
				}

				public ResultSet doCall() {
					return doCall(null);
				}

			});
		} else {
			DefaultGroovyMethods.println(this, "Not running " + String.valueOf(file) + " as another process has already marked it.");
		}

	}

	public void runMigration(File file, String sha) {
		runMigration(file, sha, false);
	}

	public boolean markMigration(File file, String sha, boolean override) {

		String ifClause = override ? "" : "IF NOT EXISTS";

		//We use the light weight transaction to make sure another process hasn't started the work, but only if we aren't overriding
		ResultSet result = execute("INSERT INTO migrations (name, sha) VALUES (?, ?) " + ifClause + ";", file.getName(), sha);

		//We know by having IF NOT EXISTS there will always be a row returned with a column of applied
		return ((boolean)(override ? true : result.all().get(0).getBool("applied")));
	}

	public boolean markMigration(File file, String sha) {
		return markMigration(file, sha, false);
	}

	public String getMigrationMd5(String fileName) {
		ResultSet result = execute("SELECT sha FROM migrations WHERE name=?", fileName);
		if (result.isExhausted()) {
			return null;
		}

		return DefaultGroovyMethods.first(result.all()).getString("sha");
	}

	public Cluster getCluster() {
		return cluster;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	private Cluster cluster;
	private Session session;
	private String keyspace;
	private String host;
	private int port;
	private String username;
	private String password;
}
