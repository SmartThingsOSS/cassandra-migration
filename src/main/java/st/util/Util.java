package st.util;

import groovy.lang.Closure;
import groovy.lang.Reference;
import org.codehaus.groovy.runtime.StringGroovyMethods;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Util {
	public static String calculateMd5(String text) {

		final Reference<String> strippedText = new Reference<String>("");
		try {
			StringGroovyMethods.eachLine(text, new Closure<String>(null, null) {
				public String doCall(String it) {
					return setGroovyRef(strippedText, strippedText.get() + it.trim());
				}

				public String doCall() {
					return doCall(null);
				}

			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return StringGroovyMethods.padLeft(new BigInteger(1, digest.digest(strippedText.get().getBytes())).toString(16), 32, "0");
	}

	private static <T> T setGroovyRef(Reference<T> ref, T newValue) {
		ref.set(newValue);
		return newValue;
	}
}
