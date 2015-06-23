package st.migration;

import st.cassandra.CassandraConnection;
import st.util.Util;

import java.io.File;

public class MarkCompleteHandler implements Handler {
	public MarkCompleteHandler(CassandraConnection connection) {
		this.connection = connection;
	}

	@Override
	public void handle(final String fileName, final String fileContents) {
		String md5 = Util.calculateMd5(fileContents);
		String existingMd5 = connection.getMigrationMd5(fileName);
		if (existingMd5 == null) {
			System.out.println("Marking migration " + fileName + " as run!");
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
