package st.migration;

import java.io.File;

public class MigrationParameters {

	private Boolean override;
	private HandlerClass handlerClass;
	private File migrationFile;
	private String host = "localhost";
	private String keyspace = "test";
	private String location;
	private String migrationsPath = "../migrations";
	private String password;
	private String username;
	private int port = 9042;

	public MigrationParameters() {
		host = System.getProperty("host", "localhost");
		port = Integer.parseInt(System.getProperty("port", "9042"));
		keyspace = System.getProperty("keyspace", "test");
		migrationsPath = System.getProperty("migrationPath", "../migrations");

		migrationFile = new File(System.getProperty("migrationFile"));

		username = System.getProperty("username");
		password = System.getProperty("password");
		String handlerClassString = System.getProperty("handlerClass");
		try {
			handlerClass = HandlerClass.valueOf(handlerClassString);
		} catch (IllegalArgumentException ex) {
			handlerClass = HandlerClass.MigrationHandler;
		}

		location = System.getProperty("location");
		override = new Boolean(System.getProperty("override"));
	}

	public MigrationParameters(Boolean override, HandlerClass handlerClass, File migrationFile, String host, String keyspace, String location, String migrationsPath, String password, String username, int port) {
		this.override = override;
		this.handlerClass = handlerClass;
		this.migrationFile = migrationFile;
		this.host = host;
		this.keyspace = keyspace;
		this.location = location;
		this.migrationsPath = migrationsPath;
		this.password = password;
		this.username = username;
		this.port = port;
	}

	public String toString() {
		final File file = migrationFile;
		return "\n		Host: " + host + "\n		Port: " + String.valueOf(port) + "\n		Keyspace: " + keyspace + "\n		Migrations Directory: " + migrationsPath + "\n		Migrations File: " + "\n		";
	}

	public File getMigrationsDir() {
		return new File(migrationsPath);
	}

	public Boolean getOverride() {
		return override;
	}

	public void setOverride(Boolean override) {
		this.override = override;
	}

	public HandlerClass getHandlerClass() {
		return handlerClass;
	}

	public void setHandlerClass(HandlerClass handlerClass) {
		this.handlerClass = handlerClass;
	}

	public File getMigrationFile() {
		return migrationFile;
	}

	public void setMigrationFile(File migrationFile) {
		this.migrationFile = migrationFile;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getMigrationsPath() {
		return migrationsPath;
	}

	public void setMigrationsPath(String migrationsPath) {
		this.migrationsPath = migrationsPath;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public class Builder {
		private File migrationFile;
		private String host = "localhost";
		private String keyspace = "test";
		private String migrationsPath = "../migrations";
		private String password;
		private String username;
		private int port = 9042;

		public Builder() {}

		public Builder setMigrationFile(File migrationFile) {
			this.migrationFile = migrationFile;
			return this;
		}

		public Builder setHost(String host) {
			this.host = host;
			return this;
		}

		public Builder setKeyspace(String keyspace) {
			this.keyspace = keyspace;
			return this;
		}

		public Builder setMigrationsPath(String migrationsPath) {
			this.migrationsPath = migrationsPath;
			return this;
		}

		public Builder setPassword(String password) {
			this.password = password;
			return this;
		}

		public Builder setUsername(String username) {
			this.username = username;
			return this;
		}

		public Builder setPort(int port) {
			this.port = port;
			return this;
		}

		public MigrationParameters build() {

			return new MigrationParameters(false, HandlerClass.MigrationHandler, migrationFile, host, keyspace, null, migrationsPath, password, username, port);
		}

	}

	public enum HandlerClass {
		MigrationHandler, MarkRunHandler, ExternalHandler
	}
}
