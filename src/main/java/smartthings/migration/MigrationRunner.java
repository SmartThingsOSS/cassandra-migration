package smartthings.migration;

import com.google.common.base.Charsets;
import com.google.common.io.CharSource;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.cassandra.CassandraConnection;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class MigrationRunner {
	private Logger logger = LoggerFactory.getLogger(MigrationRunner.class);

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

		try (CassandraConnection connection = new CassandraConnection(migrationParameters)) {
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

			boolean isLeader = false;
			try {
				connection.connect();
				// too early! must be leader first //connection.backfillMigrations(); //Cleans up old style migrations with full file path
				connection.setKeyspace(migrationParameters.getKeyspace());
				connection.setupMigration();
				isLeader = connection.becomeLeader(migrationParameters.getLeaderId());
				if (!isLeader) {
					// TODO: per Arun, we should have the other nodes that didn't become leader wait and periodically check for the lock release and if the migrations were performed.
					return;
				}
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
			} finally {
				if (isLeader) {
					try {
						connection.execute("DELETE leader FROM migrations WHERE name = 'LEADER ELECTION'");
						logger.info("leader " + migrationParameters.getLeaderId() + " should not longer be leader");
					} catch (Exception cleanupException) {
						// this is a desperation cleanup task, so we just log-and-eat...
						// print to stderr/stdout too?
					}
				}
			}
		}
	}
}
