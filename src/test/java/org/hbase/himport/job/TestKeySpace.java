package org.hbase.himport.job;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.hbase.himport.keys.KeySpace;
import org.hbase.himport.keys.KeySpace.ByteKeyField;
import org.hbase.himport.keys.KeySpace.KeyField;
import org.hbase.himport.keys.KeySpace.LongKeyField;
import org.hbase.himport.keys.KeySpace.ShortKeyField;
import org.junit.Test;

public class TestKeySpace {

	@Test
	public void test2() {

		int regions = 90;

		byte[][] ranges = KeySpace.ranges(new KeyField[] { new ShortKeyField((short) 0, regions),
				new LongKeyField(0L, regions), new LongKeyField(0L, regions),
				new ByteKeyField((byte) 0, regions) }, regions);
		
		for(byte[] key : ranges){
			System.out.println(Arrays.toString(key));
		}
		
		assertEquals(ranges.length, regions );
	}

	@Test
	public void test() {

		int a = Integer.parseInt("04444444", 16);
		int b = Integer.parseInt("7bbbbbb4", 16);
		int c = Integer.parseInt("77777770", 16);

		System.out.println("a: " + a + " b: " + b + " c: " + c);

		/*
		 * Bytes.putShort(key, 0, ((short)index(uid, 31991)));
		 * Bytes.putLong(key, 2, uid); Bytes.putLong(key, 10, invts); key[18] =
		 * (byte) type;
		 */

		float regions = 90;

		float intervalShort = (Short.MAX_VALUE / regions);
		float intervalLong = (Long.MAX_VALUE / regions);
		float intervalByte = (float) ((Byte.MAX_VALUE / regions) + 0.5);

		System.out.println("INTERVAL BYTE " + intervalByte);
		short s = 0;
		long l = 0;
		byte m = (byte) 0;

		for (int i = 0; i < regions; i++) {
			System.out.println(s + "\t" + "\t" + l + "\t" + m);

			s = (short) Math.round(s + intervalShort);
			l = (long) Math.round((double) (l + intervalLong));
			m = (byte) Math.round(m + intervalByte);

		}

		System.out.println(s + "\t" + "\t" + l + "\t" + m);
		System.out.println();
		System.out.println(Short.MAX_VALUE + "\t" + "\t" + Long.MAX_VALUE
				+ "\t" + Byte.MAX_VALUE);
		System.out.println(intervalShort + " : "
				+ (int) (intervalShort * regions));
		System.out.println(intervalLong + " : "
				+ (long) (intervalLong * regions));
		System.out.println(intervalByte + " : "
				+ (byte) (intervalByte * regions));
	}

}
