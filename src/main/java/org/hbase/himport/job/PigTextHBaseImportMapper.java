package org.hbase.himport.job;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.hbase.himport.job.HBaseImport.Column;

/**
 * 
 * Mappers used by HBaseImport. Maps typed columns from their string
 * representation, to their java primitives.
 * 
 */
public class PigTextHBaseImportMapper extends
		Mapper<Writable, Text, ImmutableBytesWritable, Put> {

	enum ERROR {
		BAD_LINE, BAD_VALUE, BAD_TYPE, EMPTY_KEY
	}

	public static final String TABLE_NAME = "table";
	public static String COL_SPECS = "colspecs";
	public static final String IGNORE_BAD_LINES = "ignore.badlines";
	public static final String IGNORE_BAD_VALUES = "ignore.badvalues";

	Map<Long, ColsSpec> colsSpecMap = null;

	byte[] tableName;
	char sep;
	boolean ignoreBadLines = false;
	boolean ignoreBadValues = false;

	int errorLogs = 0;
	int logLimit = 50;

	int keyLogs = 0;

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void map(Writable key, Text txt,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

		final String[] value = StringUtils.split(txt.toString(), '\t');

		final int size = value.length;

		// key is not part of the columns but in the line.
		if (size < 1) {
			// notify that the line is not correct
			context.getCounter(ERROR.BAD_LINE).increment(1);

		} else {
			// get key as bytes

			final Long id = Long.valueOf(value[0]);

			final ColsSpec colSpec = colsSpecMap.get(id);

			if (colSpec == null) {
				// print out
				System.out.println("ColSpec is null for key: " + id);
				System.out.println("ColSpecMap is: ");
				for (Entry<Long, ColsSpec> entry : colsSpecMap.entrySet()) {
					System.out
							.println(entry.getKey() + ": " + entry.getValue());
				}
			}

			if ((size - 2) != colSpec.columns.length) {
				context.getCounter(ERROR.BAD_LINE).increment(1);
				return;
			}

			final byte[] kval = colSpec.keyColumn.parseString(value[1]);

			final ImmutableBytesWritable rowKey = new ImmutableBytesWritable(
					kval, 0, kval.length);
			final Put put = new Put(rowKey.copyBytes());

			int datIndex = 2;

			for (int i = 0; i < colSpec.columns.length; i++) {
				final Column col = colSpec.columns[i];
				try {
					final String v = value[datIndex];
					if (v != null) {
						put.add(col.getFamily(), col.getQualifier(),
								col.parseString(v));
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

		colsSpecMap = createColSpecMap(context.getConfiguration()
				.get(COL_SPECS));

		tableName = Bytes.toBytes(table);

		ignoreBadLines = context.getConfiguration().getBoolean(
				IGNORE_BAD_LINES, true);
		ignoreBadValues = context.getConfiguration().getBoolean(
				IGNORE_BAD_VALUES, true);

	}

	public static Map<Long, ColsSpec> createColSpecMap(String colstr) {

		final Map<Long, ColsSpec> map = new HashMap<Long, PigTextHBaseImportMapper.ColsSpec>();
		/*
		 * Format for col_spec should be id=val val val,id=val val val, id=val
		 * val val
		 */
		String specDefs[] = StringUtils.split(colstr, ',');

		for (String specDef : specDefs) {
			// specDef has format id=val val val
			final String specDefParts[] = StringUtils.split(specDef, '=');

			// check if the cols string has a value
			if (specDefParts.length != 2 || specDefParts[1] == null) {
				System.out
						.println("PigTextHBaseImportMapper.createColSpecMap: "
								+ specDef
								+ " no definition for current column family");
			} else {

				final Long id = Long.valueOf(specDefParts[0]);
				map.put(id, createColsSpec(id, specDefParts[1]));
			}

		}

		return map;
	}

	private static final ColsSpec createColsSpec(Long id, String str) {

		String[] splits = StringUtils.split(str, ' ');
		Column[] cols = new Column[splits.length];

		for (int i = 0; i < splits.length; i++) {
			final String[] colfam = StringUtils.split(splits[i], ':');
			final String type = (colfam.length == 3) ? colfam[2] : "str";

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

		return new ColsSpec(id, cols);

	}

	/**
	 * 
	 * Class that contains the ColsSpec
	 * 
	 */
	public static class ColsSpec {

		final Column keyColumn = new Column(null, null, "array");

		final Long id;
		final Column[] columns;

		public ColsSpec(Long id, Column[] columns) {
			super();
			this.id = id;
			this.columns = columns;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ColsSpec other = (ColsSpec) obj;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "ColsSpec [id=" + id + ", columns="
					+ Arrays.toString(columns) + "]";
		}

	}

}
