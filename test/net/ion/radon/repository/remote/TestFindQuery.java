package net.ion.radon.repository.remote;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import net.ion.framework.util.Debug;
import net.ion.framework.util.MapUtil;
import net.ion.radon.client.AradonClient;
import net.ion.radon.client.AradonClientFactory;
import net.ion.radon.core.Aradon;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.NodeCursor;
import net.ion.radon.repository.NodeObject;
import net.ion.radon.repository.RemoteRepository;
import net.ion.radon.repository.RemoteRepositoryCentral;
import net.ion.radon.repository.RemoteSession;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.UpdateChain;
import net.ion.radon.repository.remote.FindQueryLet;
import net.ion.radon.repository.remote.body.MergeBody;
import net.ion.radon.util.AradonTester;
import junit.framework.TestCase;

public class TestFindQuery extends TestBaseRemote {

	public void testLogin() throws Exception {
		RemoteSession session = super.remoteLogin();
		session.createQuery().find().debugPrint(PageBean.ALL);
	}

	
	
	public void testNewNode() throws Exception {
		Session cs = super.confirmLogin(RemoteTestWorkspaceName);
		cs.createQuery().remove();

		RemoteSession session = super.remoteLogin();
		session.newNode().put("name", "bleujin").setAradonId("emp", "bleujin").getSession().commit();

		Node foundNode = cs.createQuery().findOne();
		assertEquals(true, foundNode != null);
		assertEquals("bleujin", foundNode.getString("name"));
		assertEquals("emp", foundNode.getAradonId().getGroup());
	}

	public void testFind() throws Exception {
		addTestNode();

		RemoteSession session = super.remoteLogin();
		NodeCursor nc = session.createQuery().find();
		assertEquals(3, nc.count());
	}

	public void testFindLimit() throws Exception {
		addTestNode();

		RemoteSession session = super.remoteLogin();
		NodeCursor nc = session.createQuery().find().limit(2);
		assertEquals(2, nc.count());
	}

	public void testSort() throws Exception {
		addTestNode();
		RemoteSession session = super.remoteLogin();
		List<Node> list = session.createQuery().descending("index").find().limit(2).toList(PageBean.ALL);
		assertEquals(2, list.get(0).get("index"));
		assertEquals(1, list.get(1).get("index"));
	}
	
	
	public void testExplain() throws Exception {

		addTestNode();
		RemoteSession session = super.remoteLogin();
		NodeCursor nc = session.createQuery().descending("index").find().limit(2);
		
		assertEquals(false, nc.explain().useIndex()) ;
	}
	

	private void addTestNode() {
		RemoteSession session = super.remoteLogin();
		session.dropWorkspace();

		session.newNode().put("name", "bleujin").put("index", 1).inlist("friend").push(MapUtil.chainKeyMap().put("name", "novision").put("age", 20)).push(MapUtil.chainKeyMap().put("name", "pm1200").put("age", 25));
		session.newNode().put("name", "hero").put("index", 0).inlist("friend").push(MapUtil.chainKeyMap().put("name", "iihi").put("age", 30)).push(MapUtil.chainKeyMap().put("name", "bleujin").put("age", 35));
		session.newNode().put("name", "novision").put("index", 2).inlist("friend").push(MapUtil.chainKeyMap().put("name", "iihi").put("age", 30)).push(MapUtil.chainKeyMap().put("name", "bleujin").put("age", 35));

		session.commit();
	}

	
	public void testPage() throws Exception {
		addTestNode();
		RemoteSession session = super.remoteLogin();
		List<Node> list = session.createQuery().ascending("index").find().skip(1).toList(PageBean.ALL);
		
		assertEquals(1, list.get(0).get("index")) ;
		assertEquals(2, list.get(1).get("index")) ;
		assertEquals(2, list.size()) ;
	}
	
	
	
	public void testNodeObjectSerial() throws Exception {
		DBObject dbo = new BasicDBObject();
		dbo.put("name", "bleujin");
		dbo.put("loc", new BasicDBObject("city", "seoul"));

		NodeObject no = NodeObject.load(dbo);

		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		ObjectOutputStream oout = new ObjectOutputStream(bout);
		oout.writeObject(no);

		ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
		ObjectInputStream oin = new ObjectInputStream(bin);

		NodeObject readno = (NodeObject) oin.readObject();
		Debug.line(readno, readno.toMap());
	}

	public void testMergeBodySerial() throws Exception {
		RemoteSession session = remoteLogin();
		Node node = session.newNode().put("name", "bleujin");

		MergeBody bo = MergeBody.create(new ArrayList(session.getModified()));

		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		ObjectOutputStream oout = new ObjectOutputStream(bout);
		oout.writeObject(session.getModified());
	}

}
