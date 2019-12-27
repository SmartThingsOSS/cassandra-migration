package smartthings.migration;

import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.driver.shaded.guava.common.io.CharSource;
import com.datastax.oss.driver.shaded.guava.common.io.Files;
import com.datastax.oss.driver.shaded.guava.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.cassandra.CassandraConnection;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

public class MigrationRunner {
	private Logger logger = LoggerFactory.getLogger(MigrationRunner.class);
	private boolean running;

	private String trimLeadingSlash(String s) {
		if (s.startsWith("/")) {
			s = s.substring(1);
		}
		return s;
	}

	private CharSource loadResource(String r) {
		r = trimLeadingSlash(r);
		return Resources.asCharSource(Resources.getResource(r), Charsets.UTF_8);
	}

	public void run(MigrationParameters migrationParameters) {
		running = false;

		try {
			final String myName = InetAddress.getLocalHost().getHostName().trim();

			try (CassandraConnection connection = new CassandraConnection(migrationParameters, myName)) {
				connection.connect();
				connection.acquireLock();
				running = true;

				doMigration(connection, migrationParameters);
				running = false;
			}
		} catch (Exception e) {
			running = false;
			logger.error("Failed while running migrations.", e);

			if (e instanceof CassandraMigrationException) {
				throw (CassandraMigrationException)e;
			}

			throw new CassandraMigrationException("Failed while running migrations.", e);
		}
	}

	public boolean isRunning() {
		return running;
	}

	private void doMigration(CassandraConnection connection, MigrationParameters migrationParameters) throws IOException {


		MigrationParameters.HandlerClass handlerClass = migrationParameters.getHandlerClass(); //connection:connection, parameters:parameters

		Handler handler;
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

		connection.setupMigration();

		logger.info("Starting Migration.... ");

		connection.backfillMigrations(); //Cleans up old style migrations with full file path
		if (migrationParameters.getMigrationsLogFile() != null) {
			logger.info("Using Migration Log File: " + migrationParameters.getMigrationsLogFile());
			List<String> migrations = loadResource(migrationParameters.getMigrationsLogFile()).readLines();
			for (String file : migrations) {
				if (!file.equalsIgnoreCase("")) {
					String cql;
					try {
						cql = loadResource(file).read();
					} catch (IOException e) {
						throw new CassandraMigrationException("Error loading cql file " + file, e);
					}
					handler.handle(file, cql);
				}
			}
		} else if (migrationParameters.getMigrationFile() != null) {
			File f = migrationParameters.getMigrationFile();
			handler.handle(f.getName(), Files.asCharSource(f, Charsets.UTF_8).read());
		} else {
			File migrationsDir = migrationParameters.getMigrationsDir();
			logger.info("Using migrations Directory " + migrationsDir);
			if (migrationsDir != null) {
				File[] files = migrationsDir.listFiles();
				if (files != null) {
					for (File file : files) {
						handler.handle(file.getName(), Files.asCharSource(file, Charsets.UTF_8).read());
					}
				} else {
					logger.warn("No files found in migrations directory.");
				}
			}
		}
	}
}
