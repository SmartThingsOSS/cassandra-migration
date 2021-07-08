package smartthings.migration

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSet
import org.cassandraunit.CassandraCQLUnit
import org.cassandraunit.dataset.CQLDataSet
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import smartthings.cassandra.CassandraLock
import spock.lang.Specification
import smartthings.cassandra.CassandraConnection
import spock.util.concurrent.PollingConditions

class MigrationRunnerSpec extends Specification {

	static final String owner = 'owner'
	static final String keyspace = 'test'

	CassandraCQLUnit cassandraCQLUnit

	MigrationRunner runner = new MigrationRunner()

	def setup() {
		CQLDataSet dataSet = new ClassPathCQLDataSet('test-baseline.cql', keyspace)
		cassandraCQLUnit = new CassandraCQLUnit(dataSet, 'test-cassandra.yaml', 30000L, 3000)
		cassandraCQLUnit.before()
	}

	def cleanup() {
		cassandraCQLUnit.after()
	}

	def 'run migrations'() {
		given:
		def params = new MigrationParameters.Builder()
			.setHost('localhost')
			.setPort(9142)
			.setKeyspace(keyspace)
			.setMigrationsLogFile('/cassandra/success.changelog')
			.build()

		CassandraConnection connection = new CassandraConnection(params, owner)
		connection.connect()

		when:
		runner.run(params)

		then: 'the migrations table exists and has an entry'

		def migrations = processRows(connection.execute('SELECT * from migrations'))

		migrations == [[name: 'change-1.cql', sha: '834bd37fb41f231e3df36dcd2c51afda']]

		and: 'the migration data is present'

		def data = processRows(connection.execute('SELECT * FROM a'))
		data == [[id: '1', value: 'success']]

		when: 'run a second migration'
		params.migrationsLogFile = '/cassandra/success2.changelog'
		runner.run(params)

		then: 'new migration is there too and data is added'
		def migrations2 = processRows(connection.execute('SELECT * from migrations'))

		migrations2 == [
			[name: 'change-1.cql', sha: '834bd37fb41f231e3df36dcd2c51afda'],
			[name: 'change-2.cql', sha: 'd00f97fc30b437bf887a2d7eb557f2c5'],
		]

		def data2 = processRows(connection.execute('SELECT * FROM a')).sort()
		data2 == [[id: '1', value: 'success'], [id: '2', value: 'success-2']]
	}

	def 'run migrations via session'() {
		given:

		Cluster.Builder builder = Cluster.builder().addContactPoint('localhost').withPort(9142);


		def params = new MigrationParameters.Builder()
			.setSession(builder.build().connect())
			.setKeyspace(keyspace)
			.setMigrationsLogFile('/cassandra/success.changelog')
			.build()

		CassandraConnection connection = new CassandraConnection(params, owner)
		connection.connect()

		when:
		runner.run(params)

		then: 'the migrations table exists and has an entry'

		def migrations = processRows(connection.execute('SELECT * from migrations'))

		migrations == [[name: 'change-1.cql', sha: '834bd37fb41f231e3df36dcd2c51afda']]

		and: 'the migration data is present'

		def data = processRows(connection.execute('SELECT * FROM a'))
		data == [[id: '1', value: 'success']]

		when: 'run a second migration'
		params.migrationsLogFile = '/cassandra/success2.changelog'
		runner.run(params)

		then: 'new migration is there too and data is added'
		def migrations2 = processRows(connection.execute('SELECT * from migrations'))

		migrations2 == [
			[name: 'change-1.cql', sha: '834bd37fb41f231e3df36dcd2c51afda'],
			[name: 'change-2.cql', sha: 'd00f97fc30b437bf887a2d7eb557f2c5'],
		]

		def data2 = processRows(connection.execute('SELECT * FROM a')).sort()
		data2 == [[id: '1', value: 'success'], [id: '2', value: 'success-2']]
	}

	def 'run failing migration'() {
		given:
		def params = new MigrationParameters.Builder()
			.setHost('localhost')
			.setPort(9142)
			.setKeyspace(keyspace)
			.setMigrationsLogFile('/cassandra/failure.changelog')
			.build()

		CassandraConnection connection = new CassandraConnection(params, owner)
		connection.connect()

		when:
		runner.run(params)

		then:
		thrown(CassandraMigrationException)

		and: 'the migrations table exists and has an entry from the successful migration only'
		def migrations = processRows(connection.execute('SELECT * from migrations'))

		migrations == [[name: 'change-1.cql', sha: '834bd37fb41f231e3df36dcd2c51afda']]

		and: 'the migration data is present from the successful migration and the failed!'
		//TODO: batch would allow rollback
		def data = processRows(connection.execute('SELECT * FROM a')).sort()
		data == [[id: '1', value: 'success'], [id: '3', value: 'fail-added']]
	}

	def 'migration does not run while lock is held'() {
		given:
		def conditions = new PollingConditions(delay: 0.1, factor: 0, timeout: 60)

		and:
		def params = new MigrationParameters.Builder()
				.setHost('localhost')
				.setPort(9142)
				.setKeyspace(keyspace)
				.setMigrationsLogFile('/cassandra/success.changelog')
				.build()

		and:
		CassandraConnection connection = new CassandraConnection(params, owner)
		connection.connect()
		CassandraLock lock = new CassandraLock(connection)

		expect: 'lock is acquired'
		lock.tryLock()
		!runner.running

		when: 'start migration runner'
		def t = Thread.start {
			runner.run(params)
		}

		then: 'thread is running but runner is not running migration'
		t.alive
		!runner.running

		when: 'wait 5 seconds'
		Thread.sleep(5000)

		then: 'thread is running but runner is not running migration'
		t.alive
		!runner.running

		when: 'lock is released'
		lock.unlock()

		then: 'thread is running and runner eventually starts'
		t.alive
		conditions.eventually {
			assert runner.running
		}

		and: 'lock is owned by runner'
		assert lock.locked
		assert !lock.mine

		when: 'wait for runner to finish'
		t.join()

		then: 'runner is stopped and lock is released'
		!runner.running
		!lock.locked
	}

	List<Map> processRows(ResultSet results) {
		results.all().collect { row ->
			row.columnDefinitions.collect { column ->
				assert column.keyspace == keyspace
				[column.name, row.getString(column.name)]
			}.collectEntries()
		}
	}
}
