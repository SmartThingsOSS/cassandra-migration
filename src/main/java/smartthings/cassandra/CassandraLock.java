package smartthings.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.migration.CassandraMigrationException;

public class CassandraLock implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(CassandraLock.class);

	private static final int lockId = 1;

	private final int ttl;
	private final CassandraConnection cassandraConnection;
	private final String owner;
	private final CqlSession session;
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

		insertLock = session.prepare(
			SimpleStatement.newInstance("INSERT INTO databasechangelock(id, lockedby) VALUES (:lockId, :owner) IF NOT EXISTS USING TTL :ttl")
				.setConsistencyLevel(DefaultConsistencyLevel.QUORUM)
		);

		deleteLock = session.prepare(
			SimpleStatement.newInstance("DELETE FROM databasechangelock WHERE id = :lockId IF lockedby = :owner")
				.setConsistencyLevel(DefaultConsistencyLevel.QUORUM)
		);

		selectLock = session.prepare(
			SimpleStatement.newInstance("SELECT lockedby, TTL(lockedby) AS ttl FROM databasechangelock WHERE id = :lockId")
				.setConsistencyLevel(DefaultConsistencyLevel.SERIAL)
		);

		updateLock = session.prepare(
			SimpleStatement.newInstance("UPDATE databasechangelock USING TTL :ttl SET lockedby = :owner WHERE id = :lockId IF lockedby = :ifowner")
				.setConsistencyLevel(DefaultConsistencyLevel.QUORUM)
		);
	}

	public boolean tryLock() {
		ResultSet rs = session.execute(
			insertLock.boundStatementBuilder()
				.setInt("lockId", lockId)
				.setInt("ttl", ttl)
				.setString("owner", owner)
				.build()
		);
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
			ResultSet rs = session.execute(
				deleteLock.boundStatementBuilder()
					.setInt("lockId", lockId)
					.setString("owner", owner)
					.build()
			);
			if (!rs.wasApplied()) {
				// if ownership was lost, should be fine, since we are relinquishing ownership
				if (isMine()) {
					throw new CassandraLockException("unable to release lock");
				}
			}
		}
	}

	public void keepAlive() {
		ResultSet rs = session.execute(
		updateLock.boundStatementBuilder()
			.setInt("lockId", lockId)
			.setInt("ttl", ttl)
			.setString("owner", owner)
			.setString("ifowner", owner)
			.build()
		);
		if (!rs.wasApplied()) {
			throw new CassandraLockException("unable to keep alive lock");
		}
	}

	public String getOwner() {
		Row row = session.execute(
			selectLock.boundStatementBuilder()
				.setInt("lockId", lockId)
				.build()
		).one();
		if (row != null) {
			return row.getString("lockedby");
		}

		return null;
	}

	public int getTtl() {
		Row row = session.execute(
			selectLock.boundStatementBuilder()
				.setInt("lockId", lockId)
				.build()
		).one();
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
