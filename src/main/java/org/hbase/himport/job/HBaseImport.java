package org.hbase.himport.job;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.pig.data.DataByteArray;

import com.google.common.primitives.Longs;

/**
 * Data must be stored with Pig's BinStorage
 * 
 */
public class HBaseImport {

	public static final void main(String arg[]) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, arg)
				.getRemainingArgs();
		String colSpec = conf.get(HBaseImportMapper.COL_SPEC);

		if (otherArgs.length < 3) {
			printUsage("Wrong number of arguments" + otherArgs.length);
			System.exit(-1);
		} else if (colSpec == null || colSpec.trim().length() < 1) {
			printUsage("Please specify " + HBaseImportMapper.COL_SPEC);
			System.exit(-1);
		} else {

			System.exit(createSubmittableJob(conf, otherArgs)
					.waitForCompletion(true) ? 0 : 1);
		}

	}

	private static final Job createSubmittableJob(Configuration conf,
			String[] otherArgs) throws IOException {

		String tableName = otherArgs[0];
		Path input = new Path(otherArgs[1]);
		Path output = new Path(otherArgs[2]);

		String jobName = input.getName();
		if (jobName.length() > 20)
			jobName = jobName.substring(0, 20);

		conf.set(HBaseImportMapper.TABLE_NAME, tableName);

		Job job = new Job(conf, "Import " + jobName);

		// set job for mapper = HBaseImportMapper, MapOutput=ImmutableBytes,Put,
		// Reducer=PutSortReducer, File Input=TextInputFormat
		job.setJarByClass(HBaseImportMapper.class);
		FileInputFormat.setInputPaths(job, input);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(HBaseImportMapper.class);
		job.setReducerClass(PutSortReducer.class);
		FileOutputFormat.setOutputPath(job, output);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		HTable table = new HTable(conf, tableName);
		try {
			// call hfile output configuration
			HFileOutputFormat.configureIncrementalLoad(job, table);
		} finally {
			table.close();
		}

		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
				Longs.class);

		return job;
	}

	private static final void printUsage(String msg) {

		System.err.println(msg);

		System.err
				.println("USAGE\n"
						+ "-Dcolspec=<colFam:colName:Type,colFam:colName:Type...> table inputDir outputDir\n"
						+ "[-Dsep=<non regex seperator> -Dignore.badlines=<true/false> -Dignore.badvalues=<true/false>]\n"
						+ "Type: str, int, long, double, float, boolean, array (this is for byte array, string value must be base64 encoded)\n"
						+ "Key: the first column in the file must be the key column and must be base64 encoded\n"

				);

	}

	/**
	 * 
	 * Each column contains a type TYPE value with its own TypeParser.
	 * 
	 */
	public static class Column {
		final byte[] family;
		final byte[] qualifier;
		final TYPE type;

		enum TYPE {
			STR(new StrTypeParser()), INT(new IntTypeParser()), LONG(
					new LongTypeParser()), DOUBLE(new DoubleTypeParser()), FLOAT(
					new FloatTypeParser()), BYTE(new ByteTypeParser()), BOOLEAN(
					new BooleanTypeParser()), ARRAY(new ArrayTypeParser());

			TypeParser parser;

			TYPE(TypeParser parser) {
				this.parser = parser;
			}

		}

		public Column(byte[] family, byte[] qualifier, String type) {
			super();
			this.family = family;
			this.qualifier = qualifier;
			this.type = TYPE.valueOf(type.toUpperCase());
		}

		public TYPE getType() {
			return type;
		}

		public byte[] getFamily() {
			return family;
		}

		public byte[] getQualifier() {
			return qualifier;
		}

		public byte[] parseString(String val) {
			return type.parser.parseString(val);
		}

		public byte[] parseObject(Object obj) {
			return type.parser.parseObject(obj);
		}

	}

	public static interface TypeParser {
		byte[] parseString(String val);

		byte[] parseObject(Object obj);
	}

	public static class ByteTypeParser implements TypeParser {

		public byte[] parseString(String val) {
			return new byte[] { Byte.valueOf(val) };
		}

		public byte[] parseObject(Object obj) {
			return new byte[] { (Byte) obj };
		}
	}

	public static class IntTypeParser implements TypeParser {

		public byte[] parseString(String val) {
			return Bytes.toBytes(Integer.parseInt(val));
		}

		public byte[] parseObject(Object obj) {
			return Bytes.toBytes((Integer) obj);
		}
	}

	public static class ArrayTypeParser implements TypeParser {

		public byte[] parseString(String val) {
			return Base64.decodeBase64(val);
		}

		public byte[] parseObject(Object obj) {
			return ((DataByteArray) obj).get();
		}

	}

	public static class StrTypeParser implements TypeParser {

		public byte[] parseString(String val) {
			return Bytes.toBytes(val);
		}

		public byte[] parseObject(Object obj) {
			return Bytes.toBytes((String) obj);
		}

	}

	public static class DoubleTypeParser implements TypeParser {

		public byte[] parseString(String val) {
			return Bytes.toBytes(Double.parseDouble(val));
		}

		public byte[] parseObject(Object obj) {
			return Bytes.toBytes((Double) obj);
		}

	}

	public static class LongTypeParser implements TypeParser {

		public byte[] parseString(String val) {
			return Bytes.toBytes(Long.parseLong(val));
		}

		public byte[] parseObject(Object obj) {
			return Bytes.toBytes((Long) obj);
		}

	}

	public static class FloatTypeParser implements TypeParser {

		public byte[] parseString(String val) {
			return Bytes.toBytes(Float.parseFloat(val));
		}

		public byte[] parseObject(Object obj) {
			return Bytes.toBytes((Float) obj);
		}
	}

	public static class BooleanTypeParser implements TypeParser {

		public byte[] parseString(String val) {
			return Bytes.toBytes(Boolean.parseBoolean(val));
		}

		public byte[] parseObject(Object obj) {
			return Bytes.toBytes((Boolean) obj);
		}

	}

	public static class ShortTypeParser implements TypeParser {

		public byte[] parseString(String val) {
			return Bytes.toBytes(Short.parseShort(val));
		}

		public byte[] parseObject(Object obj) {
			return Bytes.toBytes((Short) obj);
		}

	}

}