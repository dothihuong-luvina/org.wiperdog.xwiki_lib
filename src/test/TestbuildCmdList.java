package test;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.wiperdog.ConsoleService.ConsoleServiceLib;

public class TestbuildCmdList {

	ConsoleServiceLib service;
	HashMap<String, Map<String, String>> params;
	List<String> listCmd;
	List<String> tmpListCmd;
	HashMap<String, String> tmpMap;
	String psExec;

	@Before
	public void setUp() throws Exception {
		service = new ConsoleServiceLib();
		// list command
		listCmd = new ArrayList<String>();
		// set params for test (value of params is taken from the conf.params
		// file)
		params = new HashMap<String, Map<String, String>>();
		// run command psExec
		psExec = "cmd /c echo %PSTOOLS% \\psExec.exe";
		tmpMap = new HashMap<String, String>();
		tmpMap.put("os", "win");
		tmpMap.put("host", "10.0.0.184");
		tmpMap.put("user", "thanhmx");
		tmpMap.put("pass", "insight");
		tmpMap.put("path", "D:\\testWiperdog\\1911Wiperdog");
		params.put("wiperdog_path", tmpMap);
		params.put("test", new HashMap<String, String>());
		// tmp output need to compare
		tmpListCmd = new ArrayList<String>();
		tmpListCmd.add(psExec);
		tmpListCmd.add("\\\\10.0.0.184");
		tmpListCmd.add("-i");
		tmpListCmd.add("-accepteula");
		tmpListCmd.add("-u");
		tmpListCmd.add("\"thanhmx\"");
		tmpListCmd.add("-p");
		tmpListCmd.add("\"insight\"");
	}

	@After
	public void tearDown() throws Exception {
		listCmd = new ArrayList<String>();
	}

	/**
	 * check output with isLocalhost: true and isInteractive: true
	 */
	@Test
	public void buildCmdListTest01() {
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(true, psExec, listCmd, true,
				params);
		assertTrue(listCmd.size() == 0);
	}

	/**
	 * check output with isLocalhost: false and isInteractive: true
	 */
	@Test
	public void buildCmdListTest02() {
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd,
				true, params);
		assertEquals(tmpListCmd, listCmd);
	}

	/**
	 * check output with isLocalhost: true and isInteractive: false
	 */
	@Test
	public void buildCmdListTest03() {
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(true, psExec, listCmd,
				false, params);
		assertTrue(listCmd.size() == 0);
	}

	/**
	 * check output with isLocalhost: false and isInteractive: false
	 */
	@Test
	public void buildCmdListTest04() {
		// remove -i because tmp output need to compare not contains -i
		tmpListCmd.remove("-i");
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd,
				false, params);
		assertEquals(tmpListCmd, listCmd);
	}

	/**
	 * check output with isLocalhost is empty
	 */
	@Test
	public void buildCmdListTest05() {
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList("", psExec, listCmd, false,
				params);
		assertTrue(listCmd.size() > 0);
	}

	/**
	 * check output with isLocalhost is null
	 */
	@Test
	public void buildCmdListTest06() {
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(null, psExec, listCmd,
				false, params);
		assertTrue(listCmd.size() > 0);
	}

	/**
	 * check output with psExec is empty
	 */
	@Test
	public void buildCmdListTest07() {
		// replace psExec because tmp output need to compare contains psExec is
		// empty
		tmpListCmd.set(0, "");
		// set value of psExec is empty
		psExec = "";
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd,
				true, params);
		assertEquals(tmpListCmd, listCmd);
	}

	/**
	 * check output with psExec is null
	 */
	@Test
	public void buildCmdListTest08() {
		// replace psExec because tmp output need to compare contains psExec is
		// null
		tmpListCmd.set(0, null);
		// set value of psExec is null
		psExec = null;
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd,
				true, params);
		assertEquals(tmpListCmd, listCmd);
	}

	/**
	 * check output with listCmd is not empty
	 */
	@Test
	public void buildCmdListTest09() {
		// set data for listCmd
		listCmd.add("abc");
		listCmd.add("def");
		// add data for tmp output
		List<String> addListCmd = new ArrayList<String>();
		addListCmd.add("abc");
		addListCmd.add("def");
		addListCmd.addAll(tmpListCmd);
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd,
				true, params);
		assertEquals(addListCmd, listCmd);
	}

	/**
	 * check output with isInteractive is empty
	 */
	@Test
	public void buildCmdListTest10() {
		// remove -i because tmp output need to compare not contains -i
		tmpListCmd.remove("-i");
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd, "",
				params);
		assertEquals(tmpListCmd, listCmd);
	}

	/**
	 * check output with isInteractive is null
	 */
	@Test
	public void buildCmdListTest11() {
		// remove -i because tmp output need to compare not contains -i
		tmpListCmd.remove("-i");
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd,
				null, params);
		assertEquals(tmpListCmd, listCmd);
	}

	/**
	 * check output with params is empty
	 */
	@Test
	public void buildCmdListTest12() {
		// create params
		params = new HashMap<String, Map<String, String>>();
		// replace host, user, pass because tmp output need to compare contains
		// host, user, pass is empty
		tmpListCmd.set(1, "");
		tmpListCmd.set(5, "");
		tmpListCmd.set(7, "");
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd,
				true, params);
		assertEquals(tmpListCmd, listCmd);
	}

	/**
	 * check output with params is null
	 */
	@Test
	public void buildCmdListTest13() {
		// set value of params is null
		params = null;
		// replace host, user, pass because tmp output need to compare contains
		// host, user, pass is empty
		tmpListCmd.set(1, "");
		tmpListCmd.set(5, "");
		tmpListCmd.set(7, "");
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd,
				true, params);
		assertEquals(tmpListCmd, listCmd);
	}

	/**
	 * check exception if the listCmd is null
	 */
	@Test
	public void buildCmdListTest14() {
		// set value of listCmd is null
		listCmd = null;
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd,
				true, params);
		assertNull(listCmd);
	}

	/**
	 * check exception if value of params does not contains wiperdog_path
	 */
	@Test
	public void buildCmdListTest15() {
		// create params
		params = new HashMap<String, Map<String, String>>();
		params.put("mongoDB", tmpMap);
		// replace host, user, pass because tmp output need to compare contains
		// host, user, pass is empty
		tmpListCmd.set(1, "");
		tmpListCmd.set(5, "");
		tmpListCmd.set(7, "");
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd,
				true, params);
		assertEquals(tmpListCmd, listCmd);
	}

	/**
	 * check exception if value of params does not contains host of
	 * wiperdog_path
	 */
	@Test
	public void buildCmdListTest16() {
		// create params
		params = new HashMap<String, Map<String, String>>();
		tmpMap = new HashMap<String, String>();
		tmpMap.put("os", "win");
		tmpMap.put("user", "thanhmx");
		tmpMap.put("pass", "insight");
		params.put("wiperdog_path", tmpMap);
		// replace host because tmp output need to compare contains host is
		// empty
		tmpListCmd.set(1, "");
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd,
				true, params);
		assertEquals(tmpListCmd, listCmd);
	}

	/**
	 * check exception if value of params does not contains user of
	 * wiperdog_path
	 */
	@Test
	public void buildCmdListTest17() {
		// create params
		params = new HashMap<String, Map<String, String>>();
		tmpMap = new HashMap<String, String>();
		tmpMap.put("os", "win");
		tmpMap.put("host", "10.0.0.184");
		tmpMap.put("pass", "insight");
		params.put("wiperdog_path", tmpMap);
		// replace user because tmp output need to compare contains user is
		// empty
		tmpListCmd.set(5, "");
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd,
				true, params);
		assertEquals(tmpListCmd, listCmd);
	}

	/**
	 * check exception if value of params does not contains pass of
	 * wiperdog_path
	 */
	@Test
	public void buildCmdListTest18() {
		// create params
		params = new HashMap<String, Map<String, String>>();
		tmpMap = new HashMap<String, String>();
		tmpMap.put("os", "win");
		tmpMap.put("host", "10.0.0.184");
		tmpMap.put("user", "thanhmx");
		params.put("wiperdog_path", tmpMap);
		// replace pass because tmp output need to compare contains pass is
		// empty
		tmpListCmd.set(7, "");
		// get data of function buildCmdList
		listCmd = (ArrayList) service.buildCmdList(false, psExec, listCmd,
				true, params);
		assertEquals(tmpListCmd, listCmd);
	}
}
