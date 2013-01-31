package net.ion.nsearcher.directory;

import junit.framework.TestCase;
import net.ion.framework.util.Debug;
import net.ion.nsearcher.indexer.storage.mongo.DistributedDirectory;
import net.ion.nsearcher.indexer.storage.mongo.MongoDirectory;
import net.ion.nsearcher.search.analyzer.MyKoreanAnalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.mongodb.Mongo;

public class TestMongoDir extends TestCase {


	public void testMongoDir() throws Exception {
		Mongo mongo = new Mongo("61.250.201.78");

		String TEST_DATABASE_NAME = "search";
		mongo.getDB(TEST_DATABASE_NAME).dropDatabase();

		MongoDirectory mdir = new MongoDirectory(mongo, TEST_DATABASE_NAME, "storageTest", false, false);

		Debug.line(mongo.getAddress(), mdir.getFileNames());
		Directory dir = new DistributedDirectory(mdir);

		Analyzer analyzer = new MyKoreanAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_35, analyzer);
		IndexWriter writer = new IndexWriter(dir, config);
		writer.addDocument(createDoc("bleujin"));
		writer.commit();
		writer.close();

		IndexReader reader = IndexReader.open(dir);

		Debug.line(reader.maxDoc());

		Debug.line(reader.document(0));
		reader.close();
	}
	
	private Document createDoc(String value) {
		Document doc = new Document();
		doc.add(new Field("title", value, Field.Store.YES, Field.Index.ANALYZED));

		return doc;
	}

}
