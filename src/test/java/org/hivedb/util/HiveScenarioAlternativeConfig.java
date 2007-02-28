package org.hivedb.util;

import java.util.UUID;

public class HiveScenarioAlternativeConfig extends HiveScenario.HiveScenarioConfig {
	protected Class[] getPrimaryClasses() { return new Class[] { Member.class, Admin.class };}
	protected int getInstanceCountPerPrimaryIndex() { return 2; }
	protected int getInstanceCountPerSecondaryIndex() { return 10; };
	// Classes to be used as resources and secondary indexes.
	// If the classes are also primary indexes, then the secondary index created will be
	// a property of class, such as name, which will reference the id of the class (an intra-class reference.)
	// If the classes are no also primary classes, then the secondary index created will be
	// the class's id which references the id of another class (an inter-class reference)
	protected Class[] getResourceAndSecondaryIndexClasses() {
		return  new Class[] {
			Product.class, Token.class };
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
		
	}
}
