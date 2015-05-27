package st.migration;

import st.cassandra.CassandraConnection;

import java.io.File;

public class MigrationRunner {

	public void run(MigrationParameters migrationParameters) {

		try (CassandraConnection connection = new CassandraConnection(migrationParameters)) {
			MigrationParameters.HandlerClass handlerClass = migrationParameters.getHandlerClass(); //connection:connection, parameters:parameters
			Handler handler = null;
			switch (handlerClass) {
				case MarkRunHandler:
					handler = new MarkCompleteHandler(connection);
					break;
				case ExternalHandler:
					handler = new ExecuteExternallyHandler(connection, migrationParameters);
					break;
				case MigrationHandler:
				default:
					handler = new MigrationHandler(connection, migrationParameters.getOverride());
					break;
			}

			connection.connect();
			connection.setKeyspace(migrationParameters.getKeyspace());
			connection.setupMigration();
			if (migrationParameters.getMigrationFile() != null) {
				handler.handle(migrationParameters.getMigrationFile());
			} else {
				File migrationsDir = migrationParameters.getMigrationsDir();
				if (migrationsDir != null) {
					File[] files = migrationsDir.listFiles();
					if (files != null) {
						for (File file : files) {
							handler.handle(file);
						}
					}

				}
			}
		}
	}
}
