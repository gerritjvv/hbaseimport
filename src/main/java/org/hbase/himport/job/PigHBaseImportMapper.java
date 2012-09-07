package org.hbase.himport.job;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.pig.data.Tuple;
import org.hbase.himport.job.HBaseImport.Column;

/**
 * 
 * Mappers used by HBaseImport. Maps typed columns from their string
 * representation, to their java primitives.
 * 
 */
public class PigHBaseImportMapper extends
		Mapper<Writable, Tuple, ImmutableBytesWritable, Put> {

	enum ERROR {
		BAD_LINE, BAD_VALUE, BAD_TYPE, EMPTY_KEY
	}

	public static String TABLE_NAME = "table";
	public static String COL_SPEC = "colspec";
	public static String COL_SEP = "sep";
	public static String IGNORE_BAD_LINES = "ignore.badlines";
	public static String IGNORE_BAD_VALUES = "ignore.badvalues";

	Column[] columns;
	final Column keyColumn = new Column(null, null, "array");

	byte[] tableName;
	char sep;
	boolean ignoreBadLines = false;
	boolean ignoreBadValues = false;

	int errorLogs = 0;
	int logLimit = 50;
	int columnsLength;

	int keyLogs = 0;

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void map(Writable key, Tuple value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

		final int size = value.size();

		// key is not part of the columns but in the line.
		if ((size - 1) != columnsLength) {
			// notify that the line is not correct
			context.getCounter(ERROR.BAD_LINE).increment(1);

		} else {
			// get key as bytes

			final byte[] kval = keyColumn.parseObject(value.get(0));

			final ImmutableBytesWritable rowKey = new ImmutableBytesWritable(
					kval, 0, kval.length);
			final Put put = new Put(rowKey.copyBytes());

			int datIndex = 1;

			for (int i = 0; i < columnsLength; i++) {
				final Column col = columns[i];
				try {
					Object v = value.get(datIndex);
					if (v != null) {
						put.add(col.getFamily(), col.getQualifier(),
								col.parseObject(v));
					}
				} catch (ClassCastException excp) {
					// bad value detected
					context.getCounter(ERROR.BAD_TYPE).increment(1);
					// we only write out the error message once
					if (errorLogs++ < logLimit) {
						excp.printStackTrace();
					}

				} catch (Exception excp) {
					// bad value detected
					context.getCounter(ERROR.BAD_VALUE).increment(1);
					// we only write out the error message once
					if (errorLogs++ < logLimit) {
						excp.printStackTrace();
					}

				}
				datIndex++;
			}

			context.write(rowKey, put);

		}

	}

	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		String table = context.getConfiguration().get(TABLE_NAME);
		String columnStr = context.getConfiguration().get(COL_SPEC);
		ignoreBadLines = context.getConfiguration().getBoolean(
				IGNORE_BAD_LINES, true);
		ignoreBadValues = context.getConfiguration().getBoolean(
				IGNORE_BAD_VALUES, true);

		String strsep = context.getConfiguration().get(COL_SEP, "\t");

		if (strsep.equals("byte1")) {
			sep = ((byte) 1);
		} else if (strsep.equals("byte2")) {
			sep = ((byte) 2);
		} else {

			sep = strsep.charAt(0);
		}

		System.out.println("SEP: " + sep);
		tableName = Bytes.toBytes(table);
		String[] splits = columnStr.split("[ ,]");
		Column[] cols = new Column[splits.length];

		Pattern colfamSplit = Pattern.compile(":");

		for (int i = 0; i < splits.length; i++) {
			String[] colfam = colfamSplit.split(splits[i]);
			String type = (colfam.length == 3) ? colfam[2] : "str";

			cols[i] = new Column(Bytes.toBytes(colfam[0]),
					Bytes.toBytes(colfam[1]), type);
		}

		System.out.println("Columns ====== ");
		for (int i = 0; i < cols.length; i++) {
			System.out.println("[ + " + i + "] => "
					+ Bytes.toString(cols[i].getFamily()) + ":"
					+ Bytes.toString(cols[i].getQualifier()) + ":"
					+ cols[i].getType());
		}
		System.out.println("======== Columns");
		columns = cols;
		columnsLength = cols.length;

	}

}
