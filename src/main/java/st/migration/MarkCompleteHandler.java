package st.migration;

import org.codehaus.groovy.runtime.DefaultGroovyMethods;
import org.codehaus.groovy.runtime.ResourceGroovyMethods;
import org.codehaus.groovy.runtime.StringGroovyMethods;
import st.cassandra.CassandraConnection;
import st.util.Util;

import java.io.File;
import java.io.IOException;

public class MarkCompleteHandler implements Handler {
	public MarkCompleteHandler(CassandraConnection connection) {
		this.connection = connection;
	}

	@Override
	public void handle(final File file) {
		String md5 = null;
		try {
			md5 = Util.calculateMd5(ResourceGroovyMethods.getText(file));
		} catch (IOException e) {
			e.printStackTrace();
		}
		String existingMd5 = connection.getMigrationMd5(file.getName());
		if (!StringGroovyMethods.asBoolean(existingMd5)) {
			DefaultGroovyMethods.println(this, "Marking migration " + file.getName() + " as run!");
			connection.markMigration(file, md5);
		}

	}

	public CassandraConnection getConnection() {
		return connection;
	}

	public void setConnection(CassandraConnection connection) {
		this.connection = connection;
	}

	private CassandraConnection connection;
}
