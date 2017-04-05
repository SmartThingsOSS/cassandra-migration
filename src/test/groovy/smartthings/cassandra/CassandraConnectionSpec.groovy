package smartthings.cassandra

import com.datastax.driver.core.ExecutionInfo
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import smartthings.migration.MigrationParameters
import spock.lang.Specification

class CassandraConnectionSpec extends Specification {

	CassandraConnection cassandraConnection

	Session session = Mock()

	void setup() {
		MigrationParameters parameters = new MigrationParameters.Builder().setSession(session).build()
		cassandraConnection = new CassandraConnection(parameters)
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
		0 * _
	}

	def "Migration does not achieve schema agreement"() {
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
		1 * createExecutionInfo.isSchemaInAgreement() >> false
		0 * _
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
		0 * _
		result == null
	}
}
