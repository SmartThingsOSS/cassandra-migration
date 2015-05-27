package physicalgraph.migration

import st.migration.MigrationParameters
import st.migration.MigrationRunner

class MigrationExecutor {
	static void main(String[] args) {

		MigrationParameters parameters = new MigrationParameters()

		println "Running Cassandra Task $parameters"

		MigrationRunner migrationRunner = new MigrationRunner()
		migrationRunner.run(parameters)
	}
}
