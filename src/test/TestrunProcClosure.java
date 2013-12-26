package test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.wiperdog.ConsoleService.ConsoleServiceLib;

public class TestrunProcClosure {
	ConsoleServiceLib service;
	List<String> listCmd;
	HashMap<String, String> tmpMap;
	File workDir;

	@Before
	public void setUp() throws Exception {
		service = new ConsoleServiceLib();
		// list command
		listCmd = new ArrayList<String>();
		// result of function
		tmpMap = new HashMap<String, String>();
		// path to folder on OS
		workDir = new File(System.getProperty("user.dir"));
		// set value of listCmd with each OS
		if (System.getProperty("os.name").contains("Win")) {
			listCmd.add("cmd");
			listCmd.add("/c");
			listCmd.add("cd");
		} else {
			listCmd.add("pwd");
		}
	}

	@After
	public void tearDown() throws Exception {
		listCmd = new ArrayList<String>();
		tmpMap = new HashMap<String, String>();
	}

	/**
	 * check output with value of waitFor is false on Linux
	 */
	@Test
	public void runProcClosureTest01() {
		// check on Linux
		if (System.getProperty("os.name").contains("Linux")) {
			// get data of function runProcClosure
			tmpMap = (HashMap) service.runProcClosure(listCmd, workDir, false);
			assertTrue(tmpMap.get("in").length() > 0);
			assertTrue(tmpMap.get("err").length() == 0);
		}
	}

	/**
	 * check output with value of waitFor is true on Linux
	 */
	@Test
	public void runProcClosureTest02() {
		// check on Linux
		if (System.getProperty("os.name").contains("Linux")) {
			// get data of function runProcClosure
			tmpMap = (HashMap) service.runProcClosure(listCmd, workDir, true);
			assertTrue(tmpMap.get("in").length() > 0);
			assertTrue(tmpMap.get("err").length() == 0);
			assertTrue(String.valueOf(tmpMap.get("exitVal")).length() > 0);
		}
	}

	/**
	 * check output with value of waitFor is false on Windows
	 */
	@Test
	public void runProcClosureTest03() {
		// check on Windows
		if (System.getProperty("os.name").contains("Win")) {
			// get data of function runProcClosure
			tmpMap = (HashMap) service.runProcClosure(listCmd, workDir, false);
			assertTrue(tmpMap.get("in").length() > 0);
			assertTrue(tmpMap.get("err").length() == 0);
		}
	}

	/**
	 * check output with value of waitFor is true on Windows
	 */
	@Test
	public void runProcClosureTest04() {
		// check on Windows
		if (System.getProperty("os.name").contains("Win")) {
			// get data of function runProcClosure
			tmpMap = (HashMap) service.runProcClosure(listCmd, workDir, true);
			assertTrue(tmpMap.get("in").length() > 0);
			assertTrue(tmpMap.get("err").length() == 0);
			assertTrue(String.valueOf(tmpMap.get("exitVal")).length() > 0);
		}
	}

	/**
	 * check output with listCmd is empty
	 */
	@Test
	public void runProcClosureTest05() {
		// create listCmd is empty
		listCmd = new ArrayList<String>();
		// get data of function runProcClosure
		tmpMap = (HashMap) service.runProcClosure(listCmd, workDir, false);
		assertTrue(tmpMap.get("in").length() == 0);
		assertTrue(tmpMap.get("err").length() > 0);
	}

	/**
	 * check output with listCmd is null
	 */
	@Test
	public void runProcClosureTest06() {
		// set value of listCmd is null
		listCmd = null;
		// get data of function runProcClosure
		tmpMap = (HashMap) service.runProcClosure(listCmd, workDir, false);
		assertTrue(tmpMap.get("in").length() == 0);
		assertTrue(tmpMap.get("err").length() > 0);
	}

	/**
	 * check output with workDir is empty
	 */
	@Test
	public void runProcClosureTest07() {
		// set value of workDir is empty
		workDir = new File("");
		// get data of function runProcClosure
		tmpMap = (HashMap) service.runProcClosure(listCmd, workDir, false);
		assertTrue(tmpMap.get("in").length() == 0);
		assertTrue(tmpMap.get("err").length() > 0);
	}

	/**
	 * check output with workDir is null
	 */
	@Test
	public void runProcClosureTest08() {
		// set value of workDir is null
		workDir = null;
		// get data of function runProcClosure
		tmpMap = (HashMap) service.runProcClosure(listCmd, workDir, false);
		assertTrue(tmpMap.get("in").length() > 0);
		assertTrue(tmpMap.get("err").length() == 0);
	}

	/**
	 * check output with waitFor is empty
	 */
	@Test
	public void runProcClosureTest09() {
		// get data of function runProcClosure
		tmpMap = (HashMap) service.runProcClosure(listCmd, workDir, "");
		assertTrue(tmpMap.get("in").length() > 0);
		assertTrue(tmpMap.get("err").length() == 0);
	}

	/**
	 * check output with waitFor is null
	 */
	@Test
	public void runProcClosureTest10() {
		// get data of function runProcClosure
		tmpMap = (HashMap) service.runProcClosure(listCmd, workDir, null);
		assertTrue(tmpMap.get("in").length() > 0);
		assertTrue(tmpMap.get("err").length() == 0);
	}

	/**
	 * check exception if the listCmd contains a syntax error
	 */
	@Test
	public void runProcClosureTest11() {
		// create list cmd
		listCmd = new ArrayList<String>();
		listCmd.add("cmderr");
		// get data of function runProcClosure
		tmpMap = (HashMap) service.runProcClosure(listCmd, workDir, false);
		assertTrue(tmpMap.get("in").length() == 0);
		assertTrue(tmpMap.get("err").length() > 0);
	}
}
