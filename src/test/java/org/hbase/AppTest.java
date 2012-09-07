package org.hbase;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {

	@Test
	public void testDecode() {
		System.out.println("B: " + Base64.decodeBase64("hiQjHZXBIaxDpA").length);
	}

	@Test
	public void testBase64() throws Throwable {
		Base64 b64 = new Base64(0, "\n".getBytes());

		String s1 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaasaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

		byte[] b1 = s1.getBytes("UTF-8");

		byte[] result;
		result = b64.encodeBase64(b1);
		System.out.println("[" + new String(result, "UTF-8") + "]");
		result = b64.encodeBase64(b1);
		System.out.println("[" + new String(result, "UTF-8") + "]");

		System.out.println("Decode:[ "
				+ new String(Base64.decodeBase64(result)) + "]");

	}

}
