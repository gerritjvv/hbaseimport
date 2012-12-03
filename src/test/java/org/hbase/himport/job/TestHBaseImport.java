package org.hbase.himport.job;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.hbase.himport.job.PigTextHBaseImportMapper.ColsSpec;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestHBaseImport {

	
	static File testSpec1;
	static File testSpec2;
	
	@Test
	public void testFormatColSpec()throws Throwable {
		
		//format id=fileName,id=fileName,id=fileName
		
		String spec = PigHBaseImport.formatColSpec("1=" + testSpec1.getAbsolutePath() + ",2=" + testSpec2.getAbsolutePath(), "n");
		Map<Long, ColsSpec> map = PigTextHBaseImportMapper.createColSpecMap(spec);
		
		
		ColsSpec colSpec1 = map.get(1L);
		ColsSpec colSpec2 = map.get(2L);
		
		assertEquals(3, colSpec1.columns.length);
		assertEquals(3, colSpec2.columns.length);
	}

	@BeforeClass
	public static void setup() throws IOException{
		
		File dir = new File("target/test/testHBaseImpport");
		dir.mkdirs();
		
		
		testSpec1 = new File(dir, "myspec1.cfg");
		testSpec1.createNewFile();
		FileWriter writer = new FileWriter(testSpec1);
		writer.write("[colspec]\n");
		writer.write("n=n:a:int n:b:int n:c:int\n");
		writer.close();
		
		testSpec2 = new File(dir, "myspec2.cfg");
		testSpec2.createNewFile();
		writer = new FileWriter(testSpec2);
		writer.write("[colspec]\n");
		writer.write("n=n:d:int n:e:int n:f:int\n");
		writer.close();
		
	}
	
}
