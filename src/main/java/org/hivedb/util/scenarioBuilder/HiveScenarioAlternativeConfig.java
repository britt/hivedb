package org.hivedb.util.scenarioBuilder;

import java.util.Collection;
import java.util.UUID;

import org.apache.commons.dbcp.BasicDataSource;
import org.hivedb.meta.GlobalSchema;
import org.hivedb.meta.Hive;
import org.hivedb.meta.Node;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;

public class HiveScenarioAlternativeConfig implements HiveScenarioConfig {
	
	private Hive hive;
	public HiveScenarioAlternativeConfig(String connectString) {
		try {
			try {
				new GlobalSchema(connectString).install();
				
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			BasicDataSource ds = new HiveBasicDataSource(connectString);
			new HiveSemaphoreDao(ds).create();
			hive = Hive.load(connectString);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	public Hive getHive()
	{
		return hive;
	}
	
	public Class[] getPrimaryClasses() { return new Class[] { Member.class, Admin.class };}
	public int getInstanceCountPerPrimaryIndex() { return 2; }
	public int getInstanceCountPerSecondaryIndex() { return 10; };
	// Classes to be used as resources and secondary indexes.
	// If the classes are also primary indexes, then the secondary index created will be
	// a property of class, such as name, which will reference the id of the class (an intra-class reference.)
	// If the classes are no also primary classes, then the secondary index created will be
	// the class's id which references the id of another class (an inter-class reference)
	public Class[] getResourceAndSecondaryIndexClasses() {
		return  new Class[] {
			Product.class, Token.class };
	}
	public Collection<String> getIndexUris(final Hive hive) {
		return Generate.create(new Generator<String>(){
			public String f() { return hive.getHiveUri(); }}, new NumberIterator(2));
	}
	// The nodes of representing the data storage databases. These may be nonunique as well.
	public Collection<Node> getNodes(final Hive hive) {
		return Generate.create(new Generator<Node>(){
			public Node f() { return new Node(  hive.getHiveUri(), 												
												false); }},
							  new NumberIterator(3));
	}
	
	public static class Member implements PrimaryAndSecondaryIndexIdentifiable
	{
		public Member() {
			this.id = ++memberId;
		}
		private static int memberId=0;
		private int id;
		private String memberName;
		public Integer getIdAsPrimaryIndexInstance() {
			
			return id;
		}

		public String getIdAsSecondaryIndexInstance() {
			return memberName;
		}

		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() {
			return id;
		}

		public Member getPrimaryIndexInstanceReference() {
			return this;
		}

		public String getSecondaryIdName() {
			return "name";
		}

		public Integer getId() {
			return this.id;
		}
		
		public String getPartitionDimensionName() {	return this.getClass().getSimpleName();	}
		public String getResourceName() { return this.getClass().getSimpleName(); }
	}
	public static class Product implements SecondaryIndexIdentifiable
	{
		private static int productId = 0;
		private int id;
		private Member member;
		public Product(Member member)
		{
			this.id = ++productId;
			this.member = member;
		}
		public Integer getIdAsSecondaryIndexInstance() {
			return this.id;
		}

		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() {
			return member.getId();
		}

		public Member getPrimaryIndexInstanceReference() {
			return member;
		}

		public String getResourceName()
		{
			return this.getClass().getSimpleName();
		}
		
		public String getSecondaryIdName() {
			return "member_id";
		}
	}
	public static class Admin implements PrimaryIndexIdentifiable
	{
		int id;
		private static int adminId = 0;
		public Admin()
		{
			id = ++adminId;
		}
		public Integer getIdAsPrimaryIndexInstance() {
			return id;
		}
		public int getId()
		{
			return id;
		}
		public String getPartitionDimensionName() {	return this.getClass().getSimpleName();	}
		public String getResourceName() { return this.getClass().getSimpleName(); }
	}
	public static class Token implements SecondaryIndexIdentifiable
	{
		String id;
		Admin admin;
		public Token(Admin admin)
		{
			this.id = UUID.randomUUID().toString();
			this.admin = admin;
		}
		public String getIdAsSecondaryIndexInstance() {
			return this.id;
		}

		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() {
			return admin.getId();
		}

		public Admin getPrimaryIndexInstanceReference() {
			return admin;
		}

		public String getSecondaryIdName() {
			return "admin_id";
		}
		public String getResourceName()
		{
			return this.getClass().getSimpleName();
		}
		
	}
}
