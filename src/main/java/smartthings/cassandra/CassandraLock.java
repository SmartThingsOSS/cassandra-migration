package smartthings.cassandra;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.migration.CassandraMigrationException;

public class CassandraLock implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(CassandraLock.class);

	private static final int lockId = 1;

	private final int ttl;
	private final CassandraConnection cassandraConnection;
	private final String owner;
	private final Session session;
	private final PreparedStatement insertLock;
	private final PreparedStatement deleteLock;
	private final PreparedStatement selectLock;
	private final PreparedStatement updateLock;

	public CassandraLock(CassandraConnection cassandraConnection) {
		this(cassandraConnection, 60);
	}

	public CassandraLock(CassandraConnection cassandraConnection, int ttl) {
		this.ttl = ttl;
		this.cassandraConnection = cassandraConnection;
		this.session = cassandraConnection.getSession();
		this.owner = cassandraConnection.getOwnerName();

		setupTables();

		insertLock = session.prepare("INSERT INTO databasechangelock(id, lockedby) VALUES (:lockId, :owner) IF NOT EXISTS USING TTL :ttl");
		insertLock.setConsistencyLevel(ConsistencyLevel.QUORUM);

		deleteLock = session.prepare("DELETE FROM databasechangelock WHERE id = :lockId IF lockedby = :owner");
		deleteLock.setConsistencyLevel(ConsistencyLevel.QUORUM);

		selectLock = session.prepare("SELECT lockedby, TTL(lockedby) AS ttl FROM databasechangelock WHERE id = :lockId");
		selectLock.setConsistencyLevel(ConsistencyLevel.SERIAL);

		updateLock = session.prepare("UPDATE databasechangelock USING TTL :ttl SET lockedby = :owner WHERE id = :lockId IF lockedby = :owner");
		deleteLock.setConsistencyLevel(ConsistencyLevel.QUORUM);
	}

	public boolean tryLock() {
		ResultSet rs = session.execute(insertLock.bind().setInt("lockId", lockId)
				.setInt("ttl", ttl).setString("owner", owner));
		if (rs.wasApplied()) {
			return true;
		} else {
			Row row = rs.one();
			return owner.equals(row.getString("lockedby"));
		}
	}

	public void unlock() {
		// Only try to release lock if its mine
		if (isMine()) {
			ResultSet rs = session.execute(deleteLock.bind().setInt("lockId", lockId).setString("owner", owner));
			if (!rs.wasApplied()) {
				// if ownership was lost, should be fine, since we are relinquishing ownership
				if (isMine()) {
					throw new CassandraLockException("unable to release lock");
				}
			}
		}
	}

	public void keepAlive() {
		ResultSet rs = session.execute(updateLock.bind().setInt("lockId", lockId)
				.setInt("ttl", ttl).setString("owner", owner));
		if (!rs.wasApplied()) {
			throw new CassandraLockException("unable to keep alive lock");
		}
	}

	public String getOwner() {
		Row row = session.execute(selectLock.bind().setInt("lockId", lockId)).one();
		if (row != null) {
			return row.getString("lockedby");
		}

		return null;
	}

	public int getTtl() {
		Row row = session.execute(selectLock.bind().setInt("lockId", lockId)).one();
		if (row != null) {
			return row.getInt("ttl");
		}

		return 0;
	}

	public boolean isLocked() {
		return getOwner() != null;
	}

	public boolean isMine() {
		return owner.equals(getOwner());
	}

	@Override
	public void close() {
		if (isMine()) {
			unlock();
		}
	}

	private void setupTables() {
		if (!cassandraConnection.tableExists("databasechangelock")) {
			logger.info("lock table not found creating.");
			ResultSet rs = session.execute("CREATE TABLE IF NOT EXISTS databasechangelock " +
					"(id int, lockedby text, " +
					"PRIMARY KEY (id));");
			if (!rs.getExecutionInfo().isSchemaInAgreement()) {
				throw new CassandraMigrationException("databasechangelock table creation postcheck: schema not in agreement");
			}
		}
	}
}
