package org.hbase.himport.keys;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.hbase.himport.keys.KeySpace.ByteKeyField;
import org.hbase.himport.keys.KeySpace.IntKeyField;
import org.hbase.himport.keys.KeySpace.KeyField;
import org.hbase.himport.keys.KeySpace.LongKeyField;
import org.hbase.himport.keys.KeySpace.ShortKeyField;

public final class CreateTable {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		if (args.length != 4) {
			throw new RuntimeException(
					"Type <tableName> <regions> <colFam:colFam:colFam> <keyDef>\n keyDef= long:short:int:byte means [long short int byte] key");
		}

		String tblName = args[0];
		int regions = Integer.parseInt(args[1]);
		String[] colFams = args[2].split(":");
		String[] keyDefs = args[3].split(":");

		// create key fields
		KeyField<?>[] fields = new KeyField<?>[keyDefs.length];
		for (int i = 0; i < keyDefs.length; i++) {
			String keyDef = keyDefs[i];
			if (keyDef.equals("long"))
				fields[i] = new LongKeyField(0L, regions);
			else if (keyDef.equals("int"))
				fields[i] = new IntKeyField(0, regions);
			else if (keyDef.equals("byte"))
				fields[i] = new ByteKeyField((byte) 0, regions);
			else if (keyDef.equals("short"))
				fields[i] = new ShortKeyField((short) 0, regions);
			else
				throw new RuntimeException(keyDef
						+ " is not supported use long short int or byte");

		}

		Configuration conf = HBaseConfiguration.create();

		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(tblName);

		for (String colFam : colFams) {
			desc.addFamily(new HColumnDescriptor(colFam));
		}

		admin.createTable(desc, KeySpace.ranges(fields, regions));
		System.out.println("Created");

	}

}
