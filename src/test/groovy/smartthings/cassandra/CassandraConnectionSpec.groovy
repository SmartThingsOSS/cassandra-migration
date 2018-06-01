package smartthings.cassandra

import com.datastax.driver.core.*
import smartthings.migration.CassandraMigrationException
import smartthings.migration.MigrationParameters
import spock.lang.Specification

class CassandraConnectionSpec extends Specification {

	CassandraConnection cassandraConnection

	Session session = Mock()
	CassandraLock lock = Mock()

	void setup() {
		MigrationParameters parameters = new MigrationParameters.Builder().setSession(session).build()
		cassandraConnection = new CassandraConnection(parameters, '')
		cassandraConnection.lock = lock
	}

	def "Migration is successful"() {
		setup:
		String migrationFileName = 'make-a-table.cql'
		ResultSet migrationsResultSet = Mock()
		ResultSet createResultSet = Mock()
		ExecutionInfo createExecutionInfo = Mock()


		when:
		cassandraConnection.runMigration(migrationFileName, 'CREATE TABLE;', 'SHA1', false)

		then:
		1 * session.execute('INSERT INTO migrations (name, sha) VALUES (?, ?) IF NOT EXISTS;', [migrationFileName, 'SHA1']) >> migrationsResultSet
		1 * migrationsResultSet.wasApplied() >> true
		1 * session.execute('CREATE TABLE;') >> createResultSet
		1 * createResultSet.getExecutionInfo() >> createExecutionInfo
		1 * createExecutionInfo.isSchemaInAgreement() >> true
		_ * lock.isMine() >> true
		_ * lock.keepAlive()
		0 * _
	}

	def "Migration entry already exists"() {
		setup:
		String migrationFileName = 'make-a-table.cql'
		ResultSet migrationsResultSet = Mock()

		when:
		cassandraConnection.runMigration(migrationFileName, 'CREATE TABLE;', 'SHA1', false)

		then:
		1 * session.execute('INSERT INTO migrations (name, sha) VALUES (?, ?) IF NOT EXISTS;', [migrationFileName, 'SHA1']) >> migrationsResultSet
		1 * migrationsResultSet.wasApplied() >> false
		_ * lock.isMine() >> true
		_ * lock.keepAlive()
		0 * _
	}

	def "Migration does not achieve schema agreement"() {
		setup:
		String migrationFileName = 'make-a-table.cql'
		ResultSet migrationsResultSet = Mock()
		ResultSet createResultSet = Mock()
		ResultSet removeResultSet = Mock()
		ExecutionInfo createExecutionInfo = Mock()

		when:
		cassandraConnection.runMigration(migrationFileName, 'CREATE TABLE;', 'SHA1', false)

		then:
		1 * session.execute('INSERT INTO migrations (name, sha) VALUES (?, ?) IF NOT EXISTS;', [migrationFileName, 'SHA1']) >> migrationsResultSet
		1 * migrationsResultSet.wasApplied() >> true
		1 * session.execute('CREATE TABLE;') >> createResultSet
		1 * createResultSet.getExecutionInfo() >> createExecutionInfo
		1 * createExecutionInfo.isSchemaInAgreement() >> false
		1 * session.execute('DELETE FROM migrations WHERE name = ? IF EXISTS', [migrationFileName]) >> removeResultSet
		1 * removeResultSet.wasApplied() >> true
		_ * lock.isMine() >> true
		_ * lock.keepAlive()
		0 * _
		thrown(CassandraMigrationException)
	}

	def "Gets migration MD5"() {
		setup:
		ResultSet resultSet = Mock()
		Row row = Mock()

		when:
		String result = cassandraConnection.getMigrationMd5('/tmp/add-column.cql')

		then:
		1 * session.execute('SELECT sha FROM migrations WHERE name=?', ['add-column.cql']) >> resultSet
		1 * resultSet.isExhausted() >> false
		1 * resultSet.one() >> row
		1 * row.getString('sha') >> '1234567890'
		_ * lock.isMine() >> true
		_ * lock.keepAlive()
		0 * _
		result == '1234567890'
	}

	def "Get migration MD5 that does not exist"() {
		setup:
		ResultSet resultSet = Mock()

		when:
		String result = cassandraConnection.getMigrationMd5('/tmp/add-column.cql')

		then:
		1 * session.execute('SELECT sha FROM migrations WHERE name=?', ['add-column.cql']) >> resultSet
		1 * resultSet.isExhausted() >> true
		_ * lock.isMine() >> true
		_ * lock.keepAlive()
		0 * _
		result == null
	}
}
