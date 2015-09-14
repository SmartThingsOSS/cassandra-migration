package st.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import st.migration.MigrationParameters;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public CassandraConnection(MigrationParameters parameters) {
		cipherSuites[0] = "TLS_RSA_WITH_AES_128_CBC_SHA";
		cipherSuites[1] = "TLS_RSA_WITH_AES_256_CBC_SHA";

		this.host = parameters.getHost();
		this.port = parameters.getPort();
		this.username = parameters.getUsername();
		this.password = parameters.getPassword();
		this.truststorePassword = parameters.getTruststorePassword();
		this.truststorePath = parameters.getTruststorePath();
		this.keystorePassword = parameters.getKeystorePassword();
		this.keystorePath = parameters.getKeystorePath();

	}

	public void connect() throws Exception {
		logger.debug("Connecting to Cassandra at " + host + ":" + port);
		Cluster.Builder builder = Cluster.builder().addContactPoint(host).withPort(port);

		if (all(truststorePath, truststorePassword, keystorePath, keystorePassword)) {
			logger.debug("Using SSL for the connection");
			SSLContext sslContext = getSSLContext(truststorePath, truststorePassword, keystorePath, keystorePassword);
			builder.withSSL(new SSLOptions(sslContext, cipherSuites));
		}

		if (username != null && password != null) {
			logger.debug("Using withCredentials for the connection");
			builder.withCredentials(username, password);
		}

		cluster = builder.build();
		session = cluster.connect();
	}

	private boolean all(String... strings) {
		for (String string : strings) {
			if (string == null || string.trim() == "") {
				return false;
			}
		}
		return true;
	}

	@Override
	public void close() {
		cluster.close();
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

	public ResultSet execute(String query, Object... params) {
		return session.execute(query, params);
	}

	public void setupMigration() {
		logger.debug("Checking for migrations table.");
		ResultSet existingMigration = execute("SELECT columnfamily_name " +
				"FROM System.schema_columnfamilies	" +
				"WHERE keyspace_name=? and columnfamily_name = 'migrations';",
			keyspace);

		if (existingMigration.one() == null) {
			logger.info("migrations table not found creating.");
			execute("CREATE TABLE IF NOT EXISTS migrations " +
				"(name text, sha text, " +
				"PRIMARY KEY (name));");
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
			List<String> statements = Collections.emptyList();

			statements = Arrays.asList(fileContents.split(";"));

			for (String statement : statements) {
				String trimmedStatement = statement.trim();
				if (!trimmedStatement.equals("")) {
					execute(trimmedStatement + ";");
				}
			}
		} else {
			logger.warn("Not running " + fileName + " as another process has already marked it.");
		}
	}

	public void runMigration(File file, String sha) {
		runMigration(file, sha, false);
	}

	public boolean markMigration(String fileName, String sha, boolean override) {

		String ifClause = override ? "" : "IF NOT EXISTS";

		//We use the light weight transaction to make sure another process hasn't started the work, but only if we aren't overriding
		ResultSet result = execute("INSERT INTO migrations (name, sha) VALUES (?, ?) " + ifClause + ";", fileName, sha);

		//We know by having IF NOT EXISTS there will always be a row returned with a column of applied
		return ((boolean)(override ? true : result.one().getBool("[applied]")));
	}

	public boolean markMigration(String fileName, String sha) {
		return markMigration(fileName, sha, false);
	}

	public boolean markMigration(File file, String sha) {
		return markMigration(file.getName(), sha, false);
	}

	public String getMigrationMd5(String fileName) {
		ResultSet result = execute("SELECT sha FROM migrations WHERE name=?", fileName);
		if (result.isExhausted()) {
			return null;
		}

		return result.one().getString("sha");
	}

	public Cluster getCluster() {
		return cluster;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
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
