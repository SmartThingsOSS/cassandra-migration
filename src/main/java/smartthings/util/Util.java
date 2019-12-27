package smartthings.util;

import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.driver.shaded.guava.common.hash.Hashing;
import com.datastax.oss.driver.shaded.guava.common.io.Files;

import java.io.File;
import java.io.IOException;

public class Util {

	public static String calculateMd5(File file) {
		try {
			return calculateMd5(Files.toString(file, Charsets.UTF_8));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String calculateMd5(String text) {
		return Hashing.md5().newHasher().putString(text, Charsets.UTF_8).hash().toString();
	}

	public static boolean all(String... strings) {
		for (String string : strings) {
			if (string == null || string.trim() == "") {
				return false;
			}
		}
		return true;
	}
}
