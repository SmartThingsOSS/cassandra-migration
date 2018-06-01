package smartthings.cassandra

import org.cassandraunit.CassandraCQLUnit
import org.cassandraunit.dataset.CQLDataSet
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import smartthings.migration.MigrationParameters
import spock.lang.Specification

class CassandraLockSpec extends Specification {
	static final String owner = 'its-me'
	static final keySpace = 'test_lock'
	static final MigrationParameters params = new MigrationParameters.Builder()
			.setHost('localhost')
			.setPort(9142)
			.setKeyspace(keySpace)
			.setMigrationsLogFile('/cassandra/success.changelog')
			.build()

	CassandraCQLUnit cassandraCQLUnit

	def setup() {
		CQLDataSet dataSet = new ClassPathCQLDataSet('test-baseline.cql', keySpace)
		cassandraCQLUnit = new CassandraCQLUnit(dataSet, 'test-cassandra.yaml', 30000L, 3000)
		cassandraCQLUnit.before()
	}

	def cleanup() {
		cassandraCQLUnit.after()
	}

	def 'single threaded lock works as expected'() {
		given: 'connection'
		CassandraConnection connection = new CassandraConnection(params, owner)
		connection.connect()

		and: 'lock'
		CassandraLock lock = new CassandraLock(connection)

		expect: 'initially is unlocked'
		!lock.locked

		and: 'returns true when acquiring lock'
		lock.tryLock()

		and: 'lock is locked'
		lock.locked

		when:
		lock.keepAlive()

		then:
		noExceptionThrown()

		expect:
		lock.owner == owner

		and: 'the lock was acquired by me'
		lock.mine

		when:
		lock.unlock()

		then: 'lock is not locked'
		!lock.locked
		!lock.mine
		!lock.owner

		when:
		lock.unlock()

		then:
		noExceptionThrown()

		when:
		lock.keepAlive()

		then:
		thrown(CassandraLockException)
	}

	def 'lock is relinquished when TTL is expired'() {
		given:
		CassandraConnection connection = new CassandraConnection(params, owner)
		connection.connect()

		and: 'lock with ttl of 5 seconds'
		CassandraLock lock = new CassandraLock(connection, 5)

		expect:
		lock.tryLock()

		and:
		lock.mine

		when:
		Thread.sleep(6000)

		then:
		!lock.mine
		!lock.locked
	}

	def 'keep alive extends lock TTL'() {
		given:
		CassandraConnection connection = new CassandraConnection(params, owner)
		connection.connect()

		and: 'lock with ttl of 5 seconds'
		CassandraLock lock = new CassandraLock(connection, 10)

		when:
		lock.tryLock()

		then:
		lock.mine
		lock.ttl > 0

		when:
		Thread.sleep(5000)
		int lockTtl = lock.ttl

		then:
		lockTtl > 0

		when:
		lock.keepAlive()

		then:
		lock.ttl >= lockTtl
	}

	def 'only one owner can acquire a lock'() {
		given: 'owner named 1'
		String owner1 = '1'
		CassandraConnection connection1 = new CassandraConnection(params, owner1)
		connection1.connect()
		CassandraLock lock1 = new CassandraLock(connection1, 5)

		and: 'owner named 2'
		String owner2 = '2'
		CassandraConnection connection2 = new CassandraConnection(params, owner2)
		connection2.connect()
		CassandraLock lock2 = new CassandraLock(connection2, 5)

		expect: 'no one owns the lock'
		!lock1.mine
		!lock1.locked
		!lock2.locked
		!lock2.mine

		and: 'only 1 acquired lock'
		lock1.tryLock()
		!lock2.tryLock()

		and: '1 reports lock status and ownership'
		lock1.locked
		lock1.mine
		lock1.owner == owner1

		and: '2 reports lock status and ownership'
		lock2.locked
		!lock2.mine
		lock2.owner == owner1

		when: '1 relinquishes lock'
		lock1.unlock()

		then: '2 can acquire lock'
		lock2.tryLock()
		!lock1.tryLock()

		expect:
		lock1.locked
		!lock1.mine
		lock1.owner == owner2

		and:
		lock2.locked
		lock2.mine
		lock2.owner == owner2

		when: 'lock ttl expires'
		Thread.sleep(5000)

		then:
		!lock1.locked
		!lock2.locked
		lock1.tryLock()
	}
}
