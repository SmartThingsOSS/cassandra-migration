package smartthings.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.cassandra.CassandraConnection;
import smartthings.util.Util;

public class MarkCompleteHandler implements Handler {
	private Logger logger = LoggerFactory.getLogger(MarkCompleteHandler.class);

	public MarkCompleteHandler(CassandraConnection connection) {
		this.connection = connection;
	}

	@Override
	public void handle(final String fileName, final String fileContents) {
		String md5 = Util.calculateMd5(fileContents);
		String existingMd5 = connection.getMigrationMd5(fileName);
		if (existingMd5 == null) {
			logger.info("Marking migration " + fileName + " as run!");
			connection.markMigration(fileName, md5);
		}

	}

	public CassandraConnection getConnection() {
		return connection;
	}

	public void setConnection(CassandraConnection connection) {
		this.connection = connection;
	}

	private CassandraConnection connection;
}
