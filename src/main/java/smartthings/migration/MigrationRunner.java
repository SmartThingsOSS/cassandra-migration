package smartthings.migration;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.cassandra.CassandraConnection;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

public class MigrationRunner {
	private Logger logger = LoggerFactory.getLogger(MigrationRunner.class);

	public void backfillMigrations(MigrationParameters migrationParameters) {
		try (CassandraConnection connection = new CassandraConnection(migrationParameters)) {
			connection.connect();
			connection.backfillMigrations();
		} catch (CassandraMigrationException e) {
			throw e;
		} catch (Exception e) {
			logger.error("Failed while truncating migrations.", e);
			throw new CassandraMigrationException("Failed while truncating migrations.", e);
		}
	}

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

			try {
				connection.connect();
				connection.backfillMigrations(); //Cleans up old style migrations with full file path
				connection.setKeyspace(migrationParameters.getKeyspace());
				connection.setupMigration();
				if (migrationParameters.getMigrationsLogFile() != null) {

					logger.info("Using Migration Log File: " + migrationParameters.getMigrationsLogFile());

					String migrationLog = CharStreams.toString(new InputStreamReader(this.getClass().getResourceAsStream(migrationParameters.getMigrationsLogFile())));
					List<String> files = Arrays.asList(migrationLog.split("\n"));

					for (String file : files) {
						if (!file.equalsIgnoreCase("")) {
							InputStream stream = this.getClass().getResourceAsStream(file);
							if (stream != null) {
								String migrationFile = CharStreams.toString(new InputStreamReader(stream));
								handler.handle(file, migrationFile);
							} else {
								throw new CassandraMigrationException("File " + file + " was not found.");
							}
						}
					}
				} else if (migrationParameters.getMigrationFile() != null) {
					File f = migrationParameters.getMigrationFile();
					handler.handle(f.getName(), Files.toString(f, Charsets.UTF_8));
				} else {
					File migrationsDir = migrationParameters.getMigrationsDir();
					logger.info("Using migrations Directory " + migrationsDir);
					if (migrationsDir != null) {
						File[] files = migrationsDir.listFiles();
						if (files != null) {
							for (File file : files) {
								handler.handle(file.getName(), Files.toString(file, Charsets.UTF_8));
							}
						} else {
							logger.warn("No files found in migrations directory.");
						}
					}
				}
			} catch (CassandraMigrationException e) {
				throw e;
			} catch (Exception e) {
				logger.error("Failed while running migrations.", e);
				throw new CassandraMigrationException("Failed while running migrations.", e);
			}
		}
	}
}
