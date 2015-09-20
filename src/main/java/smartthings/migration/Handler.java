package smartthings.migration;

public interface Handler {
	public void handle(String fileName, String fileContents);
}
