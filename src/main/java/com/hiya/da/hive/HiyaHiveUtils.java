package com.hiya.da.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
public class HiyaHiveUtils
{
	
	
	/**
	 * /home/dev/ops/local/apache-hive-2.3.3-bin/bin 下面有个 hiveserver2
	 * 先要启动hiveserver2，默认端口号 10000
	 * hive --service hiveserver2 &
	 */
	private static String url = "jdbc:hive2://10.10.51.77:10000/hiya_hive";
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";// jdbc驱动路径
	private static String user = "root";// 用户名
	private static String password = "yunnex6j7";// 密码
	Connection conn = null;
	Statement stmt = null;
	private static ResultSet rs = null;

	// 加载驱动、创建连接
	public void init() throws Exception
	{
		Class.forName(driverName);
		conn = DriverManager.getConnection(url, user, password);
		stmt = conn.createStatement();
	}

	// 创建数据库
	public void createDatabase() throws Exception
	{
		String sql = "create database hiya_hive";
		System.out.println("Running: " + sql);
		stmt.execute(sql);
	}

	// 查询所有数据库
	public void showDatabases() throws Exception
	{
		String sql = "show databases";
		System.out.println("Running: " + sql);
		rs = stmt.executeQuery(sql);
		while (rs.next())
		{
			System.out.println(rs.getString(1));
		}
	}
	
	// 创建表
	public void createTable() throws Exception
	{
		String sql = "create table hiya_hive_menu(\n" + "empno int,\n" + "ename string,\n" + "job string,\n" + "mgr int,\n"
				+ "hiredate string,\n" + "sal double,\n" + "comm double,\n" + "deptno int\n" + ")\n"
				+ "row format delimited fields terminated by '\\t'";
		System.out.println("Running: " + sql);
		stmt.execute(sql);
	}
	
	// 查询所有表
	public void showTables() throws Exception
	{
		String sql = "show tables";
		System.out.println("Running: " + sql);
		rs = stmt.executeQuery(sql);
		while (rs.next())
		{
			System.out.println(rs.getString(1));
		}
	}
	
	// 查看表结构
	public void descTable() throws Exception
	{
		String sql = "desc emp";
		System.out.println("Running: " + sql);
		rs = stmt.executeQuery(sql);
		while (rs.next())
		{
			System.out.println(rs.getString(1) + "\t" + rs.getString(2));
		}
	}
	
	// 加载数据
	public void loadData() throws Exception
	{
		String filePath = "/home/hadoop/data/emp.txt";
		String sql = "load data local inpath '" + filePath + "' overwrite into table emp";
		System.out.println("Running: " + sql);
		stmt.execute(sql);
	}
	
	// 查询数据
	public void selectData() throws Exception
	{
		String sql = "select * from emp";
		System.out.println("Running: " + sql);
		rs = stmt.executeQuery(sql);
		System.out.println("员工编号" + "\t" + "员工姓名" + "\t" + "工作岗位");
		while (rs.next())
		{
			System.out.println(rs.getString("empno") + "\t\t" + rs.getString("ename") + "\t\t" + rs.getString("job"));
		}
	}
	
	// 统计查询（会运行mapreduce作业）
	public void countData() throws Exception
	{
		String sql = "select count(1) from emp";
		System.out.println("Running: " + sql);
		rs = stmt.executeQuery(sql);
		while (rs.next())
		{
			System.out.println(rs.getInt(1));
		}
	}
	
	// 删除数据库表
	public void dropTable() throws Exception
	{
		String sql = "drop table if exists emp";
		System.out.println("Running: " + sql);
		stmt.execute(sql);
	}
	
	// 删除数据库
	public void dropDatabase() throws Exception
	{
		String sql = "drop database if exists hive_jdbc_test";
		System.out.println("Running: " + sql);
		stmt.execute(sql);
	}

	// 释放资源
	public void destory() throws Exception
	{
		if (rs != null)
		{
			rs.close();
		}
		if (stmt != null)
		{
			stmt.close();
		}
		if (conn != null)
		{
			conn.close();
		}
	}
	
	public enum SigleInstance 
    {
        INSTANCE;
        public HiyaHiveUtils instance;

        SigleInstance() 
        {
            instance = new HiyaHiveUtils();
        }

        public HiyaHiveUtils getInstance() 
        {
            return instance;
        }
    }
}
