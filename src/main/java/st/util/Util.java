package st.util;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

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
