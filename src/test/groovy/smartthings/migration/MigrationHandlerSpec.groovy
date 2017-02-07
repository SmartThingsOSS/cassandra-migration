package smartthings.migration

import smartthings.cassandra.CassandraConnection
import spock.lang.Specification

class MigrationHandlerSpec extends Specification {

	MigrationHandler migrationHandler

	CassandraConnection cassandraConnection

	void setup() {
		cassandraConnection = Mock()
		migrationHandler = new MigrationHandler(cassandraConnection, false)
	}

	def "Runs migration"() {
		setup:
		String fileName = '/users/foo/create-table.cql'
		String query = 'CREATE TABLE;'

		when:
		migrationHandler.handle(fileName, query)

		then:
		1 * cassandraConnection.getMigrationMd5(fileName) >> null
		1 * cassandraConnection.runMigration(fileName, query, 'db1bda2a977f65e4135f4bca7827cf13', false)
		0 * _
	}

	def "Ignores migration that's already run"() {
		setup:
		String fileName = '/users/foo/create-table.cql'
		String query = 'CREATE TABLE;'

		when:
		migrationHandler.handle(fileName, query)

		then:
		1 * cassandraConnection.getMigrationMd5(fileName) >> 'db1bda2a977f65e4135f4bca7827cf13'
		0 * _
	}
}
