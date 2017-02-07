package smartthings.cassandra;

import com.datastax.driver.core.*;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.migration.CassandraMigrationException;
import smartthings.migration.MigrationParameters;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static smartthings.util.Util.all;

public class CassandraConnection implements AutoCloseable {
	private Logger logger = LoggerFactory.getLogger(CassandraConnection.class);

	private static String[] cipherSuites = new String[2];
	private String truststorePath;
	private String truststorePassword;
	private String keystorePath;
	private String keystorePassword;
	private Cluster cluster;
	private Session session;
	private String keyspace;
	private String host;
	private int port;
	private String username;
	private String password;

	private String cassandraVersion;

	public CassandraConnection(MigrationParameters parameters) {
		cipherSuites[0] = "TLS_RSA_WITH_AES_128_CBC_SHA";
		cipherSuites[1] = "TLS_RSA_WITH_AES_256_CBC_SHA";

		session = parameters.getSession();
		if (session == null) {
			this.host = parameters.getHost();
			this.port = parameters.getPort();
			this.username = parameters.getUsername();
			this.password = parameters.getPassword();
			this.truststorePassword = parameters.getTruststorePassword();
			this.truststorePath = parameters.getTruststorePath();
			this.keystorePassword = parameters.getKeystorePassword();
			this.keystorePath = parameters.getKeystorePath();
		}
		this.keyspace = parameters.getKeyspace();

	}

	public void connect() throws Exception {
		if (session == null) {
			logger.debug("Connecting to Cassandra at " + host + ":" + port);
			Cluster.Builder builder = Cluster.builder().addContactPoint(host).withPort(port);

			if (all(truststorePath, truststorePassword, keystorePath, keystorePassword)) {
				logger.debug("Using SSL for the connection");
				SSLContext sslContext = getSSLContext(truststorePath, truststorePassword, keystorePath, keystorePassword);
				builder.withSSL(JdkSSLOptions.builder().withSSLContext(sslContext).withCipherSuites(cipherSuites).build());
			}

			if (username != null && password != null) {
				logger.debug("Using withCredentials for the connection");
				builder.withCredentials(username, password);
			}

			cluster = builder.build();
			session = cluster.connect();
		}

		if (keyspace != null) {
			setKeyspace(keyspace);
		}

		cassandraVersion = execute("select release_version from system.local where key = 'local'")
				.one().getString(0);
	}

	@Override
	public void close() {
		if (cluster != null) {
			//We don't close the connection if we were given a session
			cluster.close();
		}
	}

	private static SSLContext getSSLContext(String truststorePath, String truststorePassword, String keystorePath, String keystorePassword) throws Exception {
		FileInputStream tsf = new FileInputStream(truststorePath);
		FileInputStream ksf = new FileInputStream(keystorePath);
		SSLContext ctx = SSLContext.getInstance("SSL");

		KeyStore ts = KeyStore.getInstance("JKS");
		ts.load(tsf, truststorePassword.toCharArray());
		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(ts);

		KeyStore ks = KeyStore.getInstance("JKS");
		ks.load(ksf, keystorePassword.toCharArray());
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

		kmf.init(ks, keystorePassword.toCharArray());

		ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
		return ctx;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
		execute("use " + keyspace);
	}

	public ResultSet execute(String query) {
		return session.execute(query);
	}

	public void backfillMigrations() {
		if (migrationsTableExists()) {
			ResultSet result = execute("SELECT * FROM migrations");
			List<Row> results = result.all();
			int numMigrated = 0;
			logger.info("Checking for migrations that need to be backfilled");
			for (Row row : results) {
				String sha = row.getString("sha");
				String name = row.getString("name");

				if (name.contains("/")) {
					String truncatedName = name.substring(name.lastIndexOf("/") + 1);
					ResultSet rs = execute("INSERT INTO migrations (name, sha) VALUES (?, ?) IF NOT EXISTS", truncatedName, sha);
					if (rs.wasApplied()) {
						numMigrated++;
						logger.info("Backfilled migration. {} -> {}", name, truncatedName);
					}
				}
			}
			logger.info("{} migrations records backfilled", numMigrated);
		}
	}

	public ResultSet execute(String query, Object... params) {
		return session.execute(query, params);
	}

	public void setupMigration() {

		if (!migrationsTableExists()) {
			logger.info("migrations table not found creating.");
			execute("CREATE TABLE IF NOT EXISTS migrations " +
					"(name text, sha text, " +
					"PRIMARY KEY (name));");
		}

	}

	public boolean migrationsTableExists() {
		logger.debug("Checking for migrations table.");
		if (cassandraVersion.startsWith("3.")) {
			ResultSet existingMigration = execute("SELECT table_name " +
							"FROM system_schema.tables	" +
							"WHERE keyspace_name=? and table_name = 'migrations';",
					keyspace);
			return (existingMigration.one() != null);
		} else {
			ResultSet existingMigration = execute("SELECT columnfamily_name " +
							"FROM System.schema_columnfamilies	" +
							"WHERE keyspace_name=? and columnfamily_name = 'migrations';",
					keyspace);
			return (existingMigration.one() != null);
		}
	}

	public void runMigration(File file, String sha, boolean override) {
		try {
			runMigration(file.getName(), Files.toString(file, Charsets.UTF_8), sha, override);
		} catch (IOException e) {
			logger.error("Failed to run migration", e);
			e.printStackTrace();
		}
	}

	public void runMigration(String fileName, String fileContents, String sha, boolean override) {
		if (markMigration(fileName, sha, override)) {
			logger.info("Running migration " + fileName + " with sha " + sha);
			List<String> statements = Arrays.asList(fileContents.split(";"));
			List<String> runStatements = new ArrayList<>();

			try {
				for (String statement : statements) {
					String trimmedStatement = statement.trim();
					if (!trimmedStatement.equals("")) {
						ResultSet resultSet = execute(trimmedStatement + ";");
						if (!resultSet.getExecutionInfo().isSchemaInAgreement()) {
							logger.error("Schema is not in agreement");
							throw new CassandraMigrationException("Schema is not in agreement.");
						}
						runStatements.add(trimmedStatement);
					}
				}
			} catch (Exception e) {

				if (!runStatements.isEmpty()) {
					String msg = "Statements run prior to failure:\n";
					for (String statement : runStatements) {
						msg += statement + ";\n";
					}
					logger.error(msg);
				}

				logger.error("removing mark for migration " + fileName);
				removeMigration(fileName);
				throw e;
			}

		} else {
			logger.warn("Not running " + fileName + " as another process has already marked it.");
		}
	}

	private void removeMigration(String fileName) {
		File file = new File(fileName);
		execute("DELETE FROM migrations WHERE name = ?", file.getName());
	}

	private boolean markMigration(String fileName, String sha, boolean override) {

		File file = new File(fileName);

		//We use the light weight transaction to make sure another process hasn't started the work, but only if we aren't overriding
		String ifClause = override ? "" : "IF NOT EXISTS";
		ResultSet result = execute("INSERT INTO migrations (name, sha) VALUES (?, ?) " + ifClause + ";", file.getName(), sha);

		return override || result.wasApplied();
	}

	public boolean markMigration(String fileName, String sha) {
		return markMigration(fileName, sha, false);
	}

	public String getMigrationMd5(String fileName) {
		File file = new File(fileName);
		ResultSet result = execute("SELECT sha FROM migrations WHERE name=?", file.getName());
		if (result.isExhausted()) {
			return null;
		}

		return result.one().getString("sha");
	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getTruststorePath() {
		return truststorePath;
	}

	public void setTruststorePath(String truststorePath) {
		this.truststorePath = truststorePath;
	}

	public String getTruststorePassword() {
		return truststorePassword;
	}

	public void setTruststorePassword(String truststorePassword) {
		this.truststorePassword = truststorePassword;
	}

	public String getKeystorePath() {
		return keystorePath;
	}

	public void setKeystorePath(String keystorePath) {
		this.keystorePath = keystorePath;
	}

	public String getKeystorePassword() {
		return keystorePassword;
	}

	public void setKeystorePassword(String keystorePassword) {
		this.keystorePassword = keystorePassword;
	}
}
