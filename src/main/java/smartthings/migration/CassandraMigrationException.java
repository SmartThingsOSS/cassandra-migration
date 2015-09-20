package smartthings.migration;
public class CassandraMigrationException extends RuntimeException {

	public CassandraMigrationException(String message) {
		super(message);
	}

	public CassandraMigrationException(String message, Exception e) {
		super(message, e);
	}
}
