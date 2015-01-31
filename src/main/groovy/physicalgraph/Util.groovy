package physicalgraph

import java.security.MessageDigest
class Util {
	static String calculateMd5(String text) {
 
		String strippedText = ''
		text.eachLine {
			strippedText+=it.trim()
		}
		def digest = MessageDigest.getInstance("MD5")
		return new BigInteger(1,digest.digest(strippedText.getBytes())).toString(16).padLeft(32,"0")
	}
}
