package org.hbase.himport.job;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;
import org.ini4j.Profile.Section;

import com.google.common.primitives.Longs;

/**
 * Data must be stored with Pig's BinStorage
 * 
 */
public class PigHBaseImport {

	public static final void main(String arg[]) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, arg)
				.getRemainingArgs();
		
		String colSpec = conf.get(PigTextHBaseImportMapper.COL_SPECS);
		final String colFamily = conf.get("colfamily");
		
		if (otherArgs.length < 3) {
			printUsage("Wrong number of arguments" + otherArgs.length);
			System.exit(-1);
		} else if (colSpec == null || colSpec.trim().length() < 1 || colFamily == null || colFamily.trim().length() < 1) {
			printUsage("Please specify " + PigTextHBaseImportMapper.COL_SPECS + " and colfamily");
			System.exit(-1);
		} else {

			//change the colspecs which contain file names from
			//id=fileName,id=fileName,id=fileName
			//id=val val
			conf.set(PigTextHBaseImportMapper.COL_SPECS, formatColSpec(colSpec, colFamily));
			

			
			conf.set("job.priority", "LOW");
			conf.set("mapred.job.priority", "LOW");
			conf.set("mapreduce.job.priority", "LOW");

			Job job = createSubmittableJob(conf, otherArgs);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

	}

	/**
	 * 
	 * @param colSpec format id=fileName,id=fileName,id=fileName
	 * @param colFamily
	 * @return
	 * @throws IOException 
	 * @throws InvalidFileFormatException 
	 */
	public static String formatColSpec(String colSpec, String colFamily) throws InvalidFileFormatException, IOException {
		
		StringBuilder buff = new StringBuilder();
		String idFilePairs[] = StringUtils.split(colSpec, ',');
		
		int i = 0;
		for(String idFilePair: idFilePairs){
			String parts[] = StringUtils.split(idFilePair, '=');
			
			File file = new File(parts[1]);
			if(!file.exists()){
				throw new FileNotFoundException(parts[1]);
			}
						
			Ini ini = new Ini(file);
			Section section = ini.get("colspec");
			String colspec = section.get(colFamily);
			if(colspec == null){
				//ignore do not include
				System.out.println("WARN: " + file + " does not have " + colFamily + " definition");
			}else{
				if(i++ != 0)
					buff.append(',');
				
				buff.append(parts[0]).append('=').append(colspec);
			}
		}
		
		return buff.toString();
	}

	private static final Job createSubmittableJob(Configuration conf,
			String[] otherArgs) throws IOException {

		String tableName = otherArgs[0];
		Path input = new Path(otherArgs[1]);
		Path output = new Path(otherArgs[2]);

		String jobName = input.getName();
		if (jobName.length() > 20)
			jobName = jobName.substring(0, 20);

		conf.set(PigHBaseImportMapper.TABLE_NAME, tableName);

		Job job = new Job(conf, "Import " + jobName);

		// set job for mapper = HBaseImportMapper, MapOutput=ImmutableBytes,Put,
		// Reducer=PutSortReducer, File Input=TextInputFormat
		job.setJarByClass(PigTextHBaseImportMapper.class);
		FileInputFormat.setInputPaths(job, input);
		job.setInputFormatClass(PigTextInputFormat.class);
//		job.setInputFormatClass(BinStorageInputFormat.class);
		job.setMapperClass(PigTextHBaseImportMapper.class);
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

}