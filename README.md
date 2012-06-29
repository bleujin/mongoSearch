mongoSearch
===========

remote mongo + search mongo


	Remote Mongo

	private Aradon aradon ;
	private RemoteRepositoryCentral rrc ;
	protected final static String RemoteTestWorkspaceName = "rwname";
	
	@Override protected void setUp() throws Exception {
		super.setUp() ;
		RepositoryCentral rc = RepositoryCentral.testCreate() ;
		this.aradon = new Aradon() ;
		RemoteClient.attachSection(aradon, rc) ;
		
		this.aradon.startServer(9000) ;
		this.rrc = RemoteRepositoryCentral.create("http://localhost:9000") ;
	}


	@Override
	protected void tearDown() throws Exception {
		this.aradon.stop() ;
		super.tearDown();
	}


	public void testFirst() throws Exception {
		RemoteSession session = rrc.login(RemoteTestWorkspaceName) ;
		session.dropWorkspace() ;
		session.newNode().put("name", "bleujin").put("age", 20) ;
		session.commit() ;
		
		Node found = session.createQuery().eq("name", "bleujin").findOne() ;
		assertEquals(20, found.get("age")) ;
	}