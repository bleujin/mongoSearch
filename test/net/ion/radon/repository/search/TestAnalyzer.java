package net.ion.radon.repository.search;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.nsearcher.common.MyField;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.SearchRepositoryCentral;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.mongodb.MongoException;

public class TestAnalyzer extends TestBaseSearch{

	public void testCJKSearch() throws MongoException, IOException, ParseException, InterruptedException, ExecutionException {
		SearchRepositoryCentral rc = SearchRepositoryCentral.testCreate() ;
		session = rc.login("test", new CJKAnalyzer(Version.LUCENE_35));
		Node node = session.newNode();
		node.put("title", "플라워즈22");
		session.commit();
		session.waitForFlushed();
		
		Debug.line(session.createSearchQuery().find("플").totalCount());
	}
	
	public void testSearch() throws Exception {
		SearchRepositoryCentral rc = SearchRepositoryCentral.testCreate() ;
		session = rc.login("test");
		session.newNode().put("index", "1").put("age", 20).getSession().commit();
		
		session.waitForFlushed();
		session.createSearchQuery().term("index", "1").find().debugPrint() ;
	}
	
	public void testLucene() throws Exception {
		Directory dir = new RAMDirectory() ;
		IndexWriter indexWriter = new IndexWriter(dir, new CJKAnalyzer(Version.LUCENE_35), MaxFieldLength.LIMITED) ;
		Document doc = new Document(); 
		Fieldable field = MyField.text("name", "플라워즈22");
		doc.add(field) ;
		indexWriter.addDocument(doc) ;
		indexWriter.commit() ;
		

		IndexReader indexReader = IndexReader.open(dir) ;
		IndexSearcher indexSearcher = new IndexSearcher(indexReader) ;
		Query query = new QueryParser(Version.LUCENE_35, "name", new SimpleAnalyzer(Version.LUCENE_35)).parse("플라워");
		TopDocs docs = indexSearcher.search(query, 10) ;
		Debug.line(docs.totalHits) ;
	}

}
