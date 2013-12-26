package org.wiperdog.ConsoleService

import org.wiperdog.WiperdogLogger.WiperdogLogger4Xwiki;

class ConsoleServiceLib {
	def static WiperdogLogger4Xwiki logger = new WiperdogLogger4Xwiki("ConsoleServiceLib")
	
	/**
	 * Closure to run a process
	 * @param listCmd
	 * @param dir
	 * @param waitFor
	 * @return
	 */
	def runProcClosure(listCmd,dir,waitFor){
		def output = [:]
		ProcessBuilder builder = new ProcessBuilder(listCmd);
		//builder.redirectErrorStream(true);
		builder.directory(dir);
		Process p = builder.start();
		if(waitFor){
			output['exitVal'] = p.waitFor()
		}
		InputStream procOut  = p.getInputStream();
		InputStream procErr  = p.getErrorStream();
	
		BufferedReader brIn = new BufferedReader(new InputStreamReader(procOut))
		BufferedReader brErr = new BufferedReader(new InputStreamReader(procErr))
		def line = null
		StringBuffer stdin = new StringBuffer()
		while((line = brIn.readLine()) != null){
			stdin.append(line + "<br/>")
		}
		StringBuffer  stderr = new StringBuffer()
		while((line = brErr.readLine()) != null){
			stderr.append(line + "<br/>")
		}
	
		output["in"] = stdin.toString()
		output["err"] = stderr.toString()
		return output
	}
	
	/**
	 * Closure to build list commands for process on Windows
	 * @param isLocalhost
	 * @param psExec
	 * @param listCmd
	 * @param isInteractive
	 * @return
	 */
	def buildCmdList(isLocalhost,psExec,listCmd,isInteractive,params){
		if(!isLocalhost){
			listCmd.add(psExec)
			listCmd.add("\\\\" + params['wiperdog_path']['host'])
			if(isInteractive){
				listCmd.add("-i")
			}
			listCmd.add("-accepteula")
			listCmd.add("-u")
			listCmd.add('"'+ params['wiperdog_path']['user']+'"')
			listCmd.add("-p")
			listCmd.add('"'+ params['wiperdog_path']['pass'] +'"')
		}
		return listCmd
	}
}
