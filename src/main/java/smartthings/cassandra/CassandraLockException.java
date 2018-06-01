package smartthings.cassandra;

public class CassandraLockException extends RuntimeException {

	public CassandraLockException(String message) {
		super(message);
	}
}
