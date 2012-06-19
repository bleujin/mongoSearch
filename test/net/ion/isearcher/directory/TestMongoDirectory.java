package net.ion.isearcher.directory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.framework.util.IOUtil;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.StringUtil;
import net.ion.isearcher.ISTestCase;
import net.ion.isearcher.common.MyDocument;
import net.ion.isearcher.common.MyField;
import net.ion.isearcher.impl.Central;
import net.ion.isearcher.impl.ISearcher;
import net.ion.isearcher.indexer.storage.mongo.StorageFac;
import net.ion.isearcher.indexer.write.IWriter;
import net.ion.isearcher.searcher.MyKoreanAnalyzer;
import net.ion.isearcher.searcher.SearchResponse;

import org.apache.ecs.rtf.PageBreak;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.SimpleFSDirectory;

public class TestMongoDirectory extends ISTestCase {

	private Directory dir ;
	private Analyzer analyzer = new MyKoreanAnalyzer();
	public void setUp() throws Exception {
		super.setUp();

		dir = StorageFac.testRecreateToMongo("61.250.201.78", "search", "storageTest");
	}
	
	public void testIndex() throws Exception {
		writeDocument(dir, analyzer) ;
		
		long start = System.currentTimeMillis() ;
		Central c = Central.createOrGet(dir) ;
		ISearcher searcher = c.newSearcher() ;
		
		SearchResponse response = searcher.searchTest("bleujin");
		Debug.line(response.getTotalCount(), System.currentTimeMillis() - start, response.getDocument() ) ;
	}
	
	public void testFileIndex() throws Exception {
		Central c = Central.createOrGet(writeDocument(analyzer)) ;
		
		ISearcher searcher = c.newSearcher() ;
		long start = System.currentTimeMillis() ;
		
		SearchResponse response = searcher.searchTest("bleujin");
		Debug.line(response.getTotalCount(), System.currentTimeMillis() - start, response.getDocument() ) ;
	}
	
	
	public void testApply() throws Exception {
		Central c = Central.createOrGet(writeDocument(dir, analyzer)) ;
		
		Central newC = Central.createOrGet(StorageFac.createToMongo("61.250.201.78", "search", "storageTest")) ;
		newC.newSearcher().searchTest("bleujin").debugPrint(Page.ALL) ;
		Debug.line('@', dir.getLockID(), newC.getDir().getLockID()) ;


		IWriter writer = c.newIndexer(analyzer) ;
		writer.begin("my") ;
		writer.insertDocument(MyDocument.testDocument().keyword("name", "bleujin")) ;
		writer.end() ;

		newC.newSearcher().searchTest("bleujin").debugPrint(Page.ALL) ;
		Debug.line() ;
	}
	
	
	public void testSpeed() throws Exception {
		Directory[] dirs = new Directory[]{dir, new MMapDirectory(new File("c:/temp/")  )} ;
		for (Directory dir : dirs) {
			long start = System.currentTimeMillis() ;
			indexSpeed(dir);
			Debug.line(dir.getClass(), System.currentTimeMillis() - start) ;
		}
	}

	private void indexSpeed(Directory dir) throws LockObtainFailedException, IOException {
		Central cram = Central.createOrGet(dir) ;
		IWriter indexer = cram.newIndexer(new MyKoreanAnalyzer());
		indexer.begin(dir.getLockID()) ;
		String readString = IOUtil.toString(new FileReader(new File("test/" + StringUtil.replace(this.getClass().getCanonicalName(), ".", "/")) + ".java")) ;
		String extString = IOUtil.toString(new FileReader(new File("../ICSS6/icss/common/ext/ext-all-debug.js"))) ;
		for (int i = 0; i < 10; i++) {
			MyDocument doc = MyDocument.testDocument();
			doc.keyword("name", "bleujin") ;
			doc.text("file", extString) ;
			indexer.insertDocument(doc) ;
		}
		indexer.end() ;
	}
	
	public void xtestArrayEqual() throws Exception {
		String[] a1 = new String[]{"jin", "hero"};
		String[] a2 = new String[]{"hero", "jin"};
		String[] a3 = new String[]{"bleu", "jin"};
		String[] a4 = new String[]{"hero", "jin", "bleu"};
		
		
		// assertEquals(true, Arrays.equals(a1, a2)) ;
		
		HashSet<String> s1 = new HashSet<String>(ListUtil.toList(a1)) ;
		HashSet<String> s2 = new HashSet<String>(ListUtil.toList(a2)) ;
		HashSet<String> s3 = new HashSet<String>(ListUtil.toList(a3)) ;
		HashSet<String> s4 = new HashSet<String>(ListUtil.toList(a4)) ;
		
		assertEquals(true, s1.equals(s2)) ;
		assertEquals(false, s1.equals(s4)) ;
		assertEquals(false, s4.equals(s1)) ;
	}
	
	
}
