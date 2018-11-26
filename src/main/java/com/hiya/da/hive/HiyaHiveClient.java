package com.hiya.da.hive;

public class HiyaHiveClient
{

	public static void main(String[] args) throws Exception
	{
		HiyaHiveUtils hhu = HiyaHiveUtils.SigleInstance.INSTANCE.getInstance();
		hhu.init();
		
		//hhu.createDatabase();

		hhu.showDatabases();
		
/*		hhu.createTable();
		
		hhu.showTables();
		
		hhu.descTable();
		
		hhu.loadData();
		
		hhu.selectData();
		
		hhu.countData();
		
		hhu.dropTable();
		
		hhu.dropDatabase();*/
	}

}
