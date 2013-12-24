package org.wiperdog.WiperdogLogger

class WiperdogLogger4Xwiki {
	def name = 'Wiperdog'
	
	def WiperdogLogger4Xwiki(name){
		this.name = name
	}
	
	/**
	 * Logging with WARN level
	 * @param message Message to be logged
	 * @return
	 */
	def warn(message){
		log(LogLevel.WARN, message)
	}
	
	/**
	 * Logging with INFO level
	 * @param message Message to be logged
	 * @return
	 */
	def info(message){
		log(LogLevel.INFO, message)
	}
	
	/**
	 * Logging with TRACE level
	 * @param message
	 * @return
	 */
	def trace(message){
		log(LogLevel.TRACE, message)
	}
	
	/**
	 * Logging with DEBUG level
	 * @param message
	 * @return
	 */
	def debug(message){
		log(LogLevel.DEBUG, message)
	}
	
	/**
	 * Logging message with level
	 * @param level Level log
	 * @param message Message to be logged
	 * @return
	 */
	def log(level, message){
		def logStr = ''
		def lvlStr = getLevel(level)
		Date now = new Date()
		def dateStr = now.format("yyyy-MM-dd HH:mm:ss.S")
		
		logStr = "$dateStr $lvlStr [$name] $message"
		println logStr
		return logStr
	}
	
	/**
	 * Get level from enum to String
	 * @param level Enum level
	 * @return
	 */
	def getLevel(level){
		if(level == LogLevel.TRACE){
			return 'TRACE'
		}
		if(level == LogLevel.INFO){
			return 'INFO'
		}
		if(level == LogLevel.DEBUG){
			return 'DEBUG'
		}
		if(level == LogLevel.WARN){
			return 'WARN'
		}
	}
}
