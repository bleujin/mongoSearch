package net.ion.isearcher.directory;

import java.io.File;
import java.io.FileInputStream;

import javax.swing.plaf.ListUI;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.sun.net.ssl.internal.ssl.Debug;

import net.ion.framework.util.IOUtil;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.RandomUtil;
import net.ion.isearcher.common.MyDocument;
import net.ion.isearcher.common.MyField;
import net.ion.isearcher.impl.Central;
import net.ion.isearcher.impl.DaemonIndexer;
import net.ion.isearcher.indexer.storage.mongo.StorageFac;
import net.ion.isearcher.indexer.write.IWriter;
import net.ion.isearcher.searcher.MyKoreanAnalyzer;
import junit.framework.TestCase;

public class TestMongoBig extends TestCase{

	
	public void testCreateIndex() throws Exception {
		Directory dir = StorageFac.testRecreateToMongo("61.250.201.78", "search", "bigTest");
		Central c = Central.createOrGet(dir) ;
		
		IWriter di = c.newDaemonIndexer(new StandardAnalyzer(Version.LUCENE_CURRENT)) ;
		
		String buildString = IOUtil.toString(new FileInputStream(new File("build.xml"))) ;
		di.begin("test") ;
		for (int i : ListUtil.rangeNum(100000)) {
			MyDocument doc = MyDocument.testDocument(); 
			doc.add(MyField.number("index", i)) ;
			doc.add(MyField.keyword("rstring", RandomUtil.nextRandomString(10))) ;
			doc.add(MyField.text("text", buildString)) ;
			di.insertDocument(doc) ;
			if (i % 1000 == 0) System.out.print('.') ;
		}
		di.end() ;
	}
	
	
	
	
}
