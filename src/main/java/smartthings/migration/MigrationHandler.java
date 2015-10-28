package smartthings.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.cassandra.CassandraConnection;
import smartthings.util.Util;

public class MigrationHandler implements Handler {

	private Logger logger = LoggerFactory.getLogger(MigrationHandler.class);

	private CassandraConnection connection;
	private boolean override;

	public MigrationHandler(CassandraConnection connection, boolean override) {
		this.connection = connection;
		this.override = override;
	}

	@Override
	public void handle(final String fileName, final String fileContents) {
		logger.info("Handling file: " + fileName);
		String md5 = Util.calculateMd5(fileContents);

		String existingMd5 = connection.getMigrationMd5(fileName);
		if (existingMd5 != null && md5.equals(existingMd5)) {
			logger.info(fileName + " was already run.");
		} else if (existingMd5 != null && !override) {
			throw new CassandraMigrationException("ERROR! md5 of " + fileName + " is different from the last time it was run!");
		} else {
			logger.info("Running migration " + fileName);
			connection.runMigration(fileName, fileContents, md5, override);
		}

	}
}
