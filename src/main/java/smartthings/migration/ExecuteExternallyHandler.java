package smartthings.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.cassandra.CassandraConnection;
import smartthings.util.Util;

import java.io.IOException;

public class ExecuteExternallyHandler implements Handler {
	private Logger logger = LoggerFactory.getLogger(ExecuteExternallyHandler.class);

	public ExecuteExternallyHandler(CassandraConnection cassandraConnection, MigrationParameters parameters) {
		this.connection = cassandraConnection;
		this.parameters = parameters;
	}

	@Override
	public void handle(final String fileName, final String fileContents) {
		String md5 = Util.calculateMd5(fileContents);

		String existingMd5 = connection.getMigrationMd5(fileName);
		if (existingMd5 != null && md5 != null && md5.equals(existingMd5)) {
			logger.info(fileName + " was already run");
		} else if (existingMd5 != null && !parameters.getOverride()) {
			throw new CassandraMigrationException("ERROR! md5 of " + fileName + " is different from the last time it was run!");
		} else {
			logger.info("Running migration " + fileName);

			String command = parameters.getLocation() + " -k " + parameters.getKeyspace() + " -h " + parameters.getHost() + " -x \"" + fileContents + "\"";
			System.out.println(command);
			try {
				Process process = Runtime.getRuntime().exec(command);
				process.waitFor();
				if (process.exitValue() == 0) {
					connection.markMigration(fileName, md5);
				}

			} catch (IOException | InterruptedException e ) {
				logger.error("failed executing command: " + command, e);
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
