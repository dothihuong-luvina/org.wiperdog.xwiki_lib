/**
 * 
 */
package test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.wiperdog.CustomMongoDbConnection.CMongoDBConn;

/**
 * @author leminhquan
 * 
 */
public class TestMongoDBConnection {

	String collection = "MySQL.Database_Area.InnoDBTablespace_Free";
	String istIid = "localhost-@MYSQL-information_schema";
	String param_file_path = "tmp\\conf.params";
	
	@Test
	public void testGetConnection1() {
		CMongoDBConn conn = new CMongoDBConn();
		try {
			conn.getConnection("localhost", 27017, "wiperdog", "", "");
		} catch (Exception e) {
			fail("Cannot create connection to Mongodb:\n" + e);
		}
		assertNotNull("Connection is null", conn.getDb());
		conn.closeConnection();
	}

	@Test
	public void testGetConnection2() {
		CMongoDBConn conn = new CMongoDBConn();
		try {
			File param_file = new File(param_file_path);
			conn.getConnection(param_file);
		} catch (Exception e) {
			fail("Cannot create connection to Mongodb:\n" + e);
		}
		assertNotNull("Connection is null", conn.getDb());
		conn.closeConnection();
	}

	@Test
	public void testGetConnection3() {
		CMongoDBConn conn = new CMongoDBConn();
		try {
			conn.getConnection(param_file_path);
		} catch (Exception e) {
			fail("Cannot create connection to Mongodb:\n" + e);
		}
		assertNotNull("Connection is null", conn.getDb());
		conn.closeConnection();
	}

	@Test
	public void testGetDataAllFields() {
		CMongoDBConn conn = createNewConnectionForTest();
		assertNotNull("Data from " + collection + "is null", conn.getDataAllFields(collection + "." + istIid));
		assertTrue("Data from " + collection + "is not a list", conn.getDataAllFields(collection + "." + istIid) instanceof List);
		assertTrue(((List)conn.getDataAllFields(collection + "." + istIid)).size() > 1);
		assertTrue(((List)conn.getDataAllFields(collection, 10, istIid)).size() <= 10);
		assertTrue(((List)conn.getDataAllFields(collection + "notInDB")).size() == 0);
		conn.closeConnection();
	}
	
	@Test(expected = AssertionError.class)
	public void testGetDataAllFieldsAssert1(){
		CMongoDBConn conn = createNewConnectionForTest();
		conn.getDataAllFields(null);
		conn.closeConnection();
	}
	
	@Test(expected = AssertionError.class)
	public void testGetDataAllFieldsAssert2(){
		CMongoDBConn conn = createNewConnectionForTest();
		conn.getDataAllFields("");
		conn.closeConnection();
	}
	
	@Test(expected = AssertionError.class)
	public void testGetDataAllFieldsAssert3(){
		CMongoDBConn conn = createNewConnectionForTest();
		conn.getDataAllFields(null, 10, null);
		conn.closeConnection();
	}
	
	@Test(expected = AssertionError.class)
	public void testGetDataAllFieldsAssert4(){
		CMongoDBConn conn = createNewConnectionForTest();
		conn.getDataAllFields("", 10, null);
		conn.closeConnection();
	}
	
	@Test(expected = AssertionError.class)
	public void testGetDataAllFieldsAssert5(){
		CMongoDBConn conn = createNewConnectionForTest();
		conn.getDataAllFields(null, 10, "");
		conn.closeConnection();
	}
	
	@Test(expected = AssertionError.class)
	public void testGetDataAllFieldsAssert6(){
		CMongoDBConn conn = createNewConnectionForTest();
		conn.getDataAllFields("", 10, "");
		conn.closeConnection();
	}
	
	@Test
	public void testGetDataLimitField(){
		CMongoDBConn conn = createNewConnectionForTest();
		String[] fields = new String[]{
				"fetchAt",
				"TablespaceName",
				"CurrentUsedSizeKB",
				"CurrentTotalSizeKB",
				"CurrentUsedPct"
		};
		
		Object ret = conn.getDataLimitFields(collection + "." + istIid, fields);
		assertNotNull(ret);
		assertTrue(ret instanceof List);
		assertTrue(((List)ret).size() > 1);
		
		Map element = (Map)((List)ret).get(0);
		Set<String> keys = element.keySet();
		String[] actualFields = new String[keys.size()];
		int i = 0;
		for (String key : keys) {
			actualFields[i] = key;
			i++;
		}
		
		assertEquals(fields, actualFields);
		
		assertTrue(((List)conn.getDataLimitFields(collection + "notInDB", fields)).size() > 1);
	}
	
	public CMongoDBConn createNewConnectionForTest(){
		CMongoDBConn conn = new CMongoDBConn();
		try {
			conn.getConnection(param_file_path);
		} catch (Exception e) {
			fail("Cannot create connection to Mongodb:\n" + e);
		}
		return conn;
	}
}
