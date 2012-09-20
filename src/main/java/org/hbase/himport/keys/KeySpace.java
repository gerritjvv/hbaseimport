package org.hbase.himport.keys;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * Calculates the key space and equal splits
 * 
 */
public class KeySpace {

	/**
	 * 
	 * @param keyFields
	 *            each field is a type
	 * @return byte[][] [N] == the key with length = keyFields.length
	 */
	public static byte[][] ranges(KeyField<?>[] keyFields, int regions) {

		int len = keyFields.length;
		byte[][] keys = new byte[regions][];

		// loop once to determine the key size
		int byteSize = 0;
		for (int i = 0; i < len; i++)
			byteSize += keyFields[i].getByteSize();

		for (int r = 0; r < regions; r++) {
			byte[] key = new byte[byteSize];
			int pos = 0;
//			String line = null;

			for (int i = 0; i < len; i++) {
//				line += "\t" + keyFields[i].getValue();
				pos += keyFields[i].write(key, pos).inc().getByteSize();
			}
//			System.out.println(line);
			keys[r] = key;
		}

		return keys;
	}

	public static abstract class KeyField<T extends Number> {

		final float interval;
		T val;
		final int byteSize;

		public KeyField(T val, float interval, int byteSize) {
			this.val = val;
			this.interval = ( ((int)interval) <= 1) ? 2F : interval;
			this.byteSize = byteSize;
		}

		public T getValue() {
			return val;
		}

		public abstract KeyField<T> inc();

		public int getByteSize() {
			return byteSize;
		}

		/**
		 * 
		 * @param arr
		 * @param pos
		 * @return the incremented offset
		 */
		public abstract KeyField<T> write(byte[] arr, int pos);

	}

	public static class ShortKeyField extends KeyField<Short> {

		public ShortKeyField(Short val, int regions) {
			super(val, Short.MAX_VALUE / regions, 2);
		}

		@Override
		public KeyField<Short> inc() {

			if ((Short.MAX_VALUE - val) < interval)
				val = 0;

			val = (short) Math.round(val + interval);

			return this;
		}

		@Override
		public KeyField<Short> write(byte[] arr, int pos) {
			Bytes.putShort(arr, pos, val);
			return this;
		}

	}

	public static class ByteKeyField extends KeyField<Byte> {

		public ByteKeyField(Byte val, int regions) {
			super(val, Byte.MAX_VALUE / regions, 1);
		}

		@Override
		public KeyField<Byte> inc() {

			if ((Byte.MAX_VALUE - val) < interval)
				val = (byte)0;

			val = (byte) Math.round((int)val + interval);

			return this;
		}

		@Override
		public KeyField<Byte> write(byte[] arr, int pos) {
			arr[pos] = val;
			return this;
		}

	}

	public static class IntKeyField extends KeyField<Integer> {

		public IntKeyField(Integer val, int regions) {
			super(val, Integer.MAX_VALUE / regions, 4);
		}

		@Override
		public KeyField<Integer> inc() {

			if ((Integer.MAX_VALUE - val) < interval)
				val = 0;

			val = Math.round(val + interval);

			return this;
		}

		@Override
		public KeyField<Integer> write(byte[] arr, int pos) {
			Bytes.putInt(arr, pos, val);
			return this;
		}

	}

	public static class LongKeyField extends KeyField<Long> {

		public LongKeyField(Long val, int regions) {
			super(val, Long.MAX_VALUE / regions, 8);
		}

		@Override
		public KeyField<Long> inc() {
			val = Math.round((double) (val + interval));
			return this;
		}

		@Override
		public KeyField<Long> write(byte[] arr, int pos) {
			Bytes.putLong(arr, pos, val);
			return this;
		}

	}

}
