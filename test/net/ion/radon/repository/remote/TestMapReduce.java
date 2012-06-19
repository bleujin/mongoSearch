package net.ion.radon.repository.remote;

import net.ion.framework.util.Debug;
import net.ion.framework.util.MapUtil;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.ApplyHander;
import net.ion.radon.repository.CommandOption;
import net.ion.radon.repository.Explain;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.NodeConstants;
import net.ion.radon.repository.NodeCursor;
import net.ion.radon.repository.PropertyFamily;
import net.ion.radon.repository.RemoteSession;

public class TestMapReduce extends TestBaseRemote{

	public void testDefault() throws Exception {
		RemoteSession session = createSample();

		String map = "function(){ emit(this._id, {self:this});}";
		String finalFunction = makeSampleFinalFunction();

		NodeCursor nc = session.createQuery().eq("address", "seoul").mapreduce(map, "", finalFunction);
		assertEquals(2, nc.toList(PageBean.ALL).size());
		nc.debugPrint(PageBean.ALL);
	}

	public void testLimit() throws Exception {
		RemoteSession session = createSample();
		String map = "function(){ emit(this._id, {self:this, friend:this.friend, name:this.name, address:this.address});}";
		String finalFunction = makeSampleFinalFunction();

		NodeCursor nc = session.createQuery().eq("address", "seoul").mapreduce(map, "", finalFunction).limit(1);
		assertEquals(1, nc.toList(PageBean.ALL).size());
		nc.debugPrint(PageBean.ALL);
	}

	public void testOnlyMapFunction() throws Exception {
		RemoteSession session = createSample();
		
		session.setAttribute(Explain.class.getCanonicalName(), null) ;
		
		PropertyFamily initial = PropertyFamily.create().put("sortkey", "name").put("skip", 0).put("limit", 2).put("order", 1);
		String params = "var _params = " + initial.toJSONString() + "; _params.comfn = function(f1, f2){ if (f1[_params.sortkey] > f2[_params.sortkey]) return 1 * _params.order ; else if(f1[_params.sortkey] < f2[_params.sortkey]) return -1 * _params.order; else return 0 }; " ;
		String map = "function(){ " +
				" " + params +
				" var sorted = Array.prototype.slice.call(Array.prototype.sort.call(this.friend, _params.comfn), _params.skip, _params.limit);" +
				" emit(this._id, {friend:sorted, name:this.name, greeting:this.name + ' Hello', fcount:this.friend.length});}" ;

		NodeCursor nc = session.createQuery().eq("address", "seoul").mapreduce(map, "", "") ;
		
		assertEquals(true, nc.explain() != null) ;
		
		
		while(nc.hasNext()){
			Node found = nc.next() ;
			Debug.line(found.get("fcount"), found.getAsInt("fcount"), found.get("fcount").getClass()) ;
		}
		assertEquals(0, session.getModified().size()) ;
	}
	
	
	private String makeSampleFinalFunction() {
		PropertyFamily initial = PropertyFamily.create().put("sortkey", "age").put("skip", 0).put("limit", 2).put("order", 1);
		String params = "var _params = " + initial.toJSONString() + "; _params.comfn = function(f1, f2){ if (f1[_params.sortkey] > f2[_params.sortkey]) return 1 * _params.order ; else if(f1[_params.sortkey] < f2[_params.sortkey]) return -1 * _params.order; else return 0 }; " ;
		String finalFunction = "function(key, values){" + params + " var doc = values.self; doc.friend = Array.prototype.slice.call(Array.prototype.sort.call(doc.friend, _params.comfn), _params.skip, _params.limit); return doc ;}";
		return finalFunction;
	}
	
	public void xtestSpeed() throws Exception {
		createSample();
		

		String map = "function(){ emit(this._id, {friend:this.friend, name:this.name});}" ;
		
		String finalFunction = makeSampleFinalFunction();

		RemoteSession session = remoteLogin() ;
		for (int i = 0; i < 100; i++) {
			NodeCursor nc = session.createQuery().eq("address", "seoul").mapreduce(map, "", finalFunction) ;
		}
	}
	
	
	private RemoteSession createSample() {
		RemoteSession session = remoteLogin() ;
		session.createQuery().remove() ;
		
		session.newNode().put("name", "bleujin").put("address", "seoul").inlist("friend")
		.push(MapUtil.chainKeyMap().put("name", "novision").put("age", 20)) 
		.push(MapUtil.chainKeyMap().put("name", "pm1200").put("age", 30)) 
		.push(MapUtil.chainKeyMap().put("name", "iihi").put("age", 25)) ;
		
		session.newNode().put("name", "hero").put("address", "seoul").inlist("friend")
		.push(MapUtil.chainKeyMap().put("name", "baegi").put("age", 20)) 
		.push(MapUtil.chainKeyMap().put("name", "minato").put("age", 25))
		.push(MapUtil.chainKeyMap().put("name", "air").put("age", 30)) ;
		
		session.commit() ;
		
		assertEquals(2, session.createQuery().eq("address", "seoul").find().count()) ;
		return session ;
	}
	
	
	public void testScope() throws Exception {
		RemoteSession session = remoteLogin() ;
		session.newNode().put("address", "seoul").put("name", "1") ;
		session.newNode().put("address", "seoul").put("name", "1") ;
		session.newNode().put("address", "seoul").put("name", "2") ;
		
		session.newNode().put("address", "busan").put("name", "11") ;
		session.newNode().put("address", "busan").put("name", "11") ;
		session.newNode().put("address", "busan").put("name", "11") ;
		session.newNode().put("address", "busan").put("name", "11") ;
		session.newNode().put("address", "busan").put("name", "11") ;

		session.newNode().put("address", "inchon").put("name", "112") ;
		session.newNode().put("address", "inchon").put("name", "112") ;
		session.newNode().put("address", "inchon").put("name", "111") ;
		session.newNode().put("address", "inchon").put("name", "111") ;

		session.commit() ;
		
		String mapFunction =
			"function() { " +
			"	emit(" +
			"		this.address,{self: this, viewcount: 1, visitcount: 1, counter: {}}" +
			"	); " +
			"}";

		String reduceFunction =
			"function(key, values) { " +
			"	MyMap = function(){ this.map = new Object(); }; " +
			"	MyMap.prototype = { " +
			"		inc: function(key) { " +
			"			this.put(key, 1);" +
			"		}, " +
			"		put: function(key, inc) {" +
			"			if (this.map[key] == null) { this.map[key] = inc; } " +
			"			else this.map[key] = this.map[key] + inc; " +
			"		}, " +
			"		size: function() { " +
			"			var count = 0; " +
			"			for(var prop in this.map) count++; " +
			"			return count;" +
			"		}, " +
			"		total: function() { " +
			"			var count = 0; " +
			"			for(var prop in this.map) count += map[prop];" +
			"			return count;" +
			"		} " +
			"	}; " +
			"	var result = {viewcount: 0, visitcount: 0} ;" +
			"	var counter = new MyMap() ;" +
			"	values.forEach( function(row) {" +
			"		result.key = key; " +
			"		result.self = row.self; " +
			"		for (var prop in row.counter.map) {" +
			"			counter.put(prop, row.counter.map[prop]); " +
			"		} " +
			"		counter.inc(row.self.address + '/' + row.self.name); " +
			"		result.viewcount += row.viewcount; " +
			"	}); " +
			"	result.counter = counter ;" +
			"	result.visitcount = result.counter.size(); " +
			"	return result; " +
			"}";
	
		String finalFunction =
			"";

		NodeCursor nc = session.createQuery().mapreduce(mapFunction, reduceFunction, finalFunction).limit(500);
		System.out.println(nc.count());
		nc.debugPrint(PageBean.ALL);
		
	}

	

	public void testDebugHandler() throws Exception {
		ApplyHander handler = new ApplyHander() {
			public Object handle(NodeCursor ac) {
				int count = 0 ;
				while(ac.hasNext()){
					Debug.debug(ac.next()) ;
					count++ ;
				}
				return count;
			}
		};

		RemoteSession session = createApplySample() ;
		
		String mapFunction = "function(){ var parent = this ; this.friend.forEach(function(p) {  emit(p.name, {self:p, pname:parent.name}); } );}" ;
		String reduceFunction = "function(key, values){var doc={} ; var pnames = []; var index = 0 ; doc.key = key ; values.forEach(function(val){ doc['self'] = val.self ;  pnames[index++] = val.pname}) ; doc['pname'] = pnames; return doc ; }";
		String finalFunction = "function(key, value){var doc={}; doc['key'] = key; doc['self'] = value.self; doc['pname'] = value.pname;  return doc }";
		Object count = session.createQuery().apply(mapFunction, reduceFunction, finalFunction, CommandOption.BLANK, handler) ;
		assertEquals(4, count) ;
	}
	

	public void testInListSort() throws Exception {
		RemoteSession session = createApplySample();
		
		PropertyFamily initial = PropertyFamily.create().put("sortkey", "name").put("skip", 0).put("limit", 2);
		String compareFn = "var comfn = function(f1, f2){ var order = 1 ;  if (f1[out.sortkey] > f2[out.sortkey]) return 1 * order ; else if(f1[out.sortkey] < f2[out.sortkey]) return -1 * order; else return 0 }" ;
		String reduce = "function(doc, out){ " + compareFn + ";  out.friends = Array.prototype.slice.call(Array.prototype.sort.call(doc.friend, comfn), out.skip, out.limit); }";

		PropertyFamily keys = PropertyFamily.create().put(NodeConstants.ID, true) ;
		NodeCursor nc = session.createQuery().eq("address", "seoul").group(keys, initial, reduce) ;
		assertEquals(2, nc.count()) ;
		nc.debugPrint(PageBean.ALL) ;
		
	}
	
	public void testGroupCount() throws Exception {
		RemoteSession session = createApplySample() ;
		
		PropertyFamily initial = PropertyFamily.create().put("count", 0);
		String reduce = "function(doc, out){ out.count++; }";
		NodeCursor nc = session.createQuery().group(PropertyFamily.create("address", true), initial, reduce) ;
		nc.debugPrint(PageBean.ALL) ;
	}
	
	
	
	private RemoteSession createApplySample() {
		RemoteSession session = remoteLogin() ;
		session.createQuery().remove() ;
		session.newNode().put("name", "bleujin").put("address", "seoul").inlist("friend")
		.push(MapUtil.chainKeyMap().put("name", "novision").put("age", 20)) 
		.push(MapUtil.chainKeyMap().put("name", "pm1200").put("age", 30)) 
		.push(MapUtil.chainKeyMap().put("name", "iihi").put("age", 25)) ;
		
		session.newNode().put("name", "hero").put("address", "seoul").inlist("friend")
		.push(MapUtil.chainKeyMap().put("name", "iihi").put("age", 25)) 
		.push(MapUtil.chainKeyMap().put("name", "minato").put("age", 25))
		.push(MapUtil.chainKeyMap().put("name", "pm1200").put("age", 30)) ;
		
		session.commit() ;
		return session ;
	}
	
	
	
}
