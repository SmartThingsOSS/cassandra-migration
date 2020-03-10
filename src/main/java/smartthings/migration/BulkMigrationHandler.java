package smartthings.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.cassandra.CassandraConnection;
import smartthings.util.Util;

import java.io.File;
import java.util.Map;

public class BulkMigrationHandler implements Handler {

	private Logger logger = LoggerFactory.getLogger(BulkMigrationHandler.class);

	private CassandraConnection connection;
	private boolean override;
	private Map<String, String> tableMap;

	public BulkMigrationHandler(CassandraConnection connection, boolean override) {
		this.connection = connection;
		this.override = override;
		this.tableMap = connection.getMigrationTable();
	}

	@Override
	public void handle(final String fileName, final String fileContents) {
		logger.info("Handling file: " + fileName);
		String md5 = Util.calculateMd5(fileContents);

		File file = new File(fileName);

		String existingMd5 = tableMap.getOrDefault(file.getName(), "");
		if (existingMd5 != null && md5.equals(existingMd5)) {
			logger.warn(fileName + " was already run.");
		} else if (existingMd5 != null && !override) {
			throw new CassandraMigrationException("ERROR! md5 of " + fileName + " is different from the last time it was run!");
		} else {
			logger.info("Running migration " + fileName);
			connection.runMigration(fileName, fileContents, md5, override);
		}
	}
}
