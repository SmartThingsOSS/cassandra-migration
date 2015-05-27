package st.migration;

import org.codehaus.groovy.runtime.DefaultGroovyMethods;
import org.codehaus.groovy.runtime.ProcessGroovyMethods;
import org.codehaus.groovy.runtime.ResourceGroovyMethods;
import st.cassandra.CassandraConnection;
import st.util.Util;

import java.io.File;
import java.io.IOException;

public class ExecuteExternallyHandler implements Handler {
	public ExecuteExternallyHandler(CassandraConnection cassandraConnection, MigrationParameters parameters) {
		this.connection = cassandraConnection;
		this.parameters = parameters;
	}

	public void handle(final File file) {
		String md5 = null;
		try {
			md5 = Util.calculateMd5(ResourceGroovyMethods.getText(file));
		} catch (IOException e) {
			e.printStackTrace();
		}
		String existingMd5 = connection.getMigrationMd5(file.getName());
		if (existingMd5 != null && md5 != null && md5.equals(existingMd5)) {
			DefaultGroovyMethods.println(this, file.getName() + " was already run");
		} else if (existingMd5 != null && !parameters.getOverride()) {
			throw new RuntimeException("ERROR! md5 of " + file.getName() + " is different from the last time it was run!");
		} else {
			DefaultGroovyMethods.println(this, "Running migration " + file.getName());

			connection.markMigration(file, md5);

			String command = parameters.getLocation() + " -f " + file.getAbsolutePath() + " -k " + parameters.getKeyspace() + " -h " + parameters.getHost();
			DefaultGroovyMethods.println(this, command);
			try {
				ProcessGroovyMethods.execute(command);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public CassandraConnection getConnection() {
		return connection;
	}

	public void setConnection(CassandraConnection connection) {
		this.connection = connection;
	}

	public MigrationParameters getParameters() {
		return parameters;
	}

	public void setParameters(MigrationParameters parameters) {
		this.parameters = parameters;
	}

	private CassandraConnection connection;
	private MigrationParameters parameters;
}
