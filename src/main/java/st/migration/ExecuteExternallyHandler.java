package st.migration;

import st.cassandra.CassandraConnection;
import st.util.Util;

import java.io.File;
import java.io.IOException;

public class ExecuteExternallyHandler implements Handler {
	public ExecuteExternallyHandler(CassandraConnection cassandraConnection, MigrationParameters parameters) {
		this.connection = cassandraConnection;
		this.parameters = parameters;
	}

	@Override
	public void handle(final String fileName, final String fileContents) {
		String md5 = Util.calculateMd5(fileContents);

		String existingMd5 = connection.getMigrationMd5(fileName);
		if (existingMd5 != null && md5 != null && md5.equals(existingMd5)) {
			System.out.println(fileName + " was already run");
		} else if (existingMd5 != null && !parameters.getOverride()) {
			throw new RuntimeException("ERROR! md5 of " + fileName + " is different from the last time it was run!");
		} else {
			System.out.println("Running migration " + fileName);

			connection.markMigration(fileName, md5);

			String command = parameters.getLocation() + " -k " + parameters.getKeyspace() + " -h " + parameters.getHost() + " -x \"" + fileContents + "\"";
			System.out.println(command);
			try {
				Runtime.getRuntime().exec(command);
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
