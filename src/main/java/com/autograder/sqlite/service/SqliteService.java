package com.autograder.sqlite.service;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.sqlite.SQLiteDataSource;

public class SqliteService {
	
	public List<String> getAllTableNames(String dbUrl) {
		List<String> tableNames = new ArrayList<>();
		SQLiteDataSource ds = new SQLiteDataSource();
		ds.setUrl(dbUrl);
		
		try {
			Connection conn = ds.getConnection();
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery("SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';");
			while(rs.next()) {
				tableNames.add(rs.getString("name"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tableNames;
	}
	
	private Map<String, Set<String>> getKeys(String tableName, String dbUrl) {
		Map<String, Set<String>> hm = new HashMap<>();
		Set<String> primaryKeys = new HashSet<>();
		Set<String> foreignKeys = new HashSet<>();
		SQLiteDataSource ds = new SQLiteDataSource();
		ds.setUrl(dbUrl);
		
		try {
			Connection conn = ds.getConnection();
			DatabaseMetaData meta = conn.getMetaData();
			ResultSet rsPk = meta.getPrimaryKeys(null, null, tableName);
			ResultSet rsEk = meta.getExportedKeys(null, null, tableName);
			while(rsPk.next()) {
				primaryKeys.add(rsPk.getString("COLUMN_NAME"));
			}
			while(rsEk.next()) {
				foreignKeys.add(rsEk.getString("COLUMN_NAME"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		hm.put("PrimaryKeys", primaryKeys);
		hm.put("ForeignKeys", foreignKeys);
		return hm;
	}
	
	private String compareKeys(Map<String, Set<String>> solnKeys, Map<String, Set<String>> subKeys) {
		StringBuilder sb = new StringBuilder();
		
		//compare primary keys
		Set<String> solnPrimaryKeys = solnKeys.get("PrimaryKeys");
		Set<String> subPrimaryKeys = subKeys.get("PrimaryKeys");
		
		for(String solnPk : solnPrimaryKeys) {
			if(subPrimaryKeys.contains(solnPk)) {
				sb.append("Primary key: "+solnPk+" found in the submission------------------Done");
			} else {
				sb.append("Primary key: "+solnPk+" not found in the submission--------------Inc");
			}
		}
		
		//compare foreign keys
		Set<String> solnForeignKeys = solnKeys.get("ForeignKeys");
		Set<String> subForeignKeys = subKeys.get("ForeignKeys");
		
		for(String solnFk : solnForeignKeys) {
			if(subForeignKeys.contains(solnFk)) {
				sb.append("Foreign key: "+solnFk+" found in the submission------------------Done");
			} else {
				sb.append("Foreign key: "+solnFk+" not found in the submission--------------Inc");
			}
		}
		sb.append("\n\n");
		
		return sb.toString();
	}
	
	public void doComparison(String solnDbUrl, String subDbUrl) throws Exception {
		String fileName = "/Output/"+subDbUrl+"_op.log";
		BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
		
		List<String> tbNames_solndb = getAllTableNames(solnDbUrl);
		List<String> tbNames_subDb = getAllTableNames(subDbUrl);
		
		Collections.sort(tbNames_solndb);
		Collections.sort(tbNames_subDb);
		
		for(int a = 0; a < tbNames_solndb.size(); a++) {
			String solnTableName = tbNames_solndb.get(a);
			String subTableName = tbNames_subDb.get(a);
			
			writer.write("***********Comparing "+solnTableName+" and "+subTableName+"************");
			
			//compare keys
			String keysComp = compareKeys(getKeys(solnTableName, solnDbUrl), getKeys(subTableName, subDbUrl));
			writer.write(keysComp);
			
			
		}
		writer.close();
	}
}
