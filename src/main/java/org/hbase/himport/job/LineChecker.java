package org.hbase.himport.job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
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
import org.hbase.himport.job.HBaseImport.Column;

import com.google.common.primitives.Longs;

/**
 * Data must be stored with Pig's BinStorage
 * 
 */
public class LineChecker {

	public static final void main(String arg[]) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, arg)
				.getRemainingArgs();
		String colSpec = conf.get(HBaseImportMapper.COL_SPEC);
		String sep = conf.get(HBaseImportMapper.COL_SEP);
		char csep;
		
		if(sep.equals("byte1")){
			csep = ((byte)1);
		}else{
			csep = sep.charAt(0);
		}
		
		sep = conf.get(HBaseImportMapper.COL_SEP, "\t");

		String[] splits = colSpec.split("[ ,]");
		Column[] cols = new Column[splits.length];

		Pattern colfamSplit = Pattern.compile(":");

		for (int i = 0; i < splits.length; i++) {
			String[] colfam = colfamSplit.split(splits[i]);
			String type = (colfam.length == 3) ? colfam[2] : "str";

			cols[i] = new Column(Bytes.toBytes(colfam[0]),
					Bytes.toBytes(colfam[1]), type);
		}

		
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("SEP: " + sep);
		try{
			String line = null;
			while((line = reader.readLine()) != null ){
				String[] split = StringUtils.splitPreserveAllTokens(line, csep);
				try{
					for(int i = 1; i < split.length; i++){
						cols[i-1].parseString(split[i]);
					}
				}catch(Throwable t){
					System.out.println(t.toString());
					System.out.println(Arrays.toString(split));
				}
			}
			
		}finally{
			reader.close();
		}
		
		String str = ",a,b,,,d";
		System.out.println("list: " + Arrays.toString(StringUtils.splitPreserveAllTokens(str, ",")));
	}

	
}