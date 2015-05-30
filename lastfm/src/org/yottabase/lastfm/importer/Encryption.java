package org.yottabase.lastfm.importer;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Encryption {

	public static void main(String[] args) {
		String str = "Hora De Acordar";
		String cry = toSHA1(str);

		System.out.println(cry);
	}

	private static final String MD5 = "MD5";
	private static final String SHA1 = "SHA-1";
	private static final String SHA256 = "SHA-256";

	public static String toMD5(String str) {
		return encrypt(str, MD5);
	}

	public static String toSHA1(String str) {
		return encrypt(str, SHA1);
	}

	public static String toSHA256(String str) {
		return encrypt(str, SHA256);
	}

	private static String encrypt(String str, String algorithm) {
		String strCrypt = "";
		try {
			MessageDigest crypt = MessageDigest.getInstance(algorithm);
			
			crypt.reset();
			crypt.update(str.getBytes(Charset.forName("UTF8")));
			strCrypt = byteArrayToHexString(crypt.digest());
			
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return strCrypt;
	}

	private static String byteArrayToHexString(final byte[] byteArray) {
		return new BigInteger(1, byteArray).toString(16);
	}

}
