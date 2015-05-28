package st.migration;

import st.util.Util;
import st.cassandra.CassandraConnection;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class MigrationHandler implements Handler {

	private CassandraConnection connection;
	private boolean override;

	public MigrationHandler(CassandraConnection connection, boolean override) {
		this.connection = connection;
		this.override = override;
	}

	public void handle(final File file) {
		String md5 = null;

		md5 = Util.calculateMd5(file);

		String existingMd5 = connection.getMigrationMd5(file.getName());
		if (existingMd5 != null && md5 != null && md5.equals(existingMd5)) {
			System.out.println(file.getName() + " was already run");
		} else if (existingMd5 != null && !override) {
			throw new RuntimeException("ERROR! md5 of " + file.getName() + " is different from the last time it was run!");
		} else {
			System.out.println("Running migration " + file.getName());
			connection.runMigration(file, md5, override);
		}

	}
}
