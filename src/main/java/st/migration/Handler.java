package st.migration;

import java.io.File;

public interface Handler {
	public void handle(String fileName, String fileContents);
}
