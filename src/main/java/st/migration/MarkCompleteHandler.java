package st.migration;

import st.cassandra.CassandraConnection;
import st.util.Util;

import java.io.File;

public class MarkCompleteHandler implements Handler {
	public MarkCompleteHandler(CassandraConnection connection) {
		this.connection = connection;
	}

	@Override
	public void handle(final File file) {
		String md5 = Util.calculateMd5(file);
		String existingMd5 = connection.getMigrationMd5(file.getName());
		if (existingMd5 == null) {
			System.out.println("Marking migration " + file.getName() + " as run!");
			connection.markMigration(file, md5);
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
