package smartthings.migration;

public class MigrationExecutor {
	public static void main(String[] args) {

		MigrationParameters parameters = new MigrationParameters();

		System.out.println("Running Cassandra Task " + String.valueOf(parameters));

		MigrationRunner migrationRunner = new MigrationRunner();
		migrationRunner.run(parameters);
	}

}
