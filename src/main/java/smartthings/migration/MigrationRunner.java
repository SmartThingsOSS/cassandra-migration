package smartthings.migration;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Charsets;
import com.google.common.io.CharSource;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.cassandra.CassandraConnection;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;

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

			try {
				connection.connect();
				connection.setKeyspace(migrationParameters.getKeyspace());
				connection.setupMigration();

				// attempt to become leader
				String currentHost = InetAddress.getLocalHost().getHostName().trim();
				ResultSet resultSet = connection.execute("SELECT * from databasechangelock where id = 1");
				Row row  = resultSet.one();

				String migrationRunnerHost = null;
				boolean isLeader = false;

				if(row != null){
					migrationRunnerHost = row.getString("lockedby");

					if(migrationRunnerHost == null){
						isLeader = connection.upsertLockTableWithNull(true, currentHost).wasApplied();
					}else if(migrationRunnerHost.equals("NONE")){
						isLeader = connection.upsertLockTable(true, currentHost).wasApplied();
					} else if(migrationRunnerHost.trim().equals(currentHost)){
						isLeader = true;
					}
				}else{
					isLeader = connection.insertLock(true, currentHost).wasApplied();
				}

				if (isLeader){
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
					logger.info("Done with migration, releasing lock.. ");
					connection.releaseLockTable(false, "NONE");
				} else {
					CountDownLatch countDownLatch = new CountDownLatch(1);
					Thread thread = new Thread(new MigrationChecker(connection, countDownLatch));
					thread.start();
					countDownLatch.await();
					logger.info("Latch released... ");
				}
			} catch (CassandraMigrationException e) {
				throw e;
			} catch (Exception e) {
				logger.error("Failed while running migrations.", e);
				throw new CassandraMigrationException("Failed while running migrations.", e);
			}
		}
	}

	private class MigrationChecker implements Runnable {

		private CassandraConnection connection;
		private CountDownLatch countDownLatch;

		public MigrationChecker (CassandraConnection connection, CountDownLatch countDownLatch){
			this.connection = connection;
			this.countDownLatch = countDownLatch;
		}

		@Override
		public void run() {

			while (connection.isMigrationRunning()) {
				logger.info("Migration is running.. please wait..");

				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					logger.info("Migration checker interrupted", e);
				}
			}
			countDownLatch.countDown();
		}
	}
}
