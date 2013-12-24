package org.wiperdog.Policy

import java.util.regex.Matcher
import java.util.regex.Pattern

import org.wiperdog.WiperdogLogger.WiperdogLogger4Xwiki;

class PolicyStringLib {
	def static WiperdogLogger4Xwiki logger = new WiperdogLogger4Xwiki("PolicyStringLib")
	/**
	 * Get level of the policy from String to int
	 * @param condition Condition's key
	 * @param mapConditionLevel Map contains all condition-level
	 * @return
	 */
	def getLevel(condition, mapConditionLevel, group = null){
		def ret = ''
		def intRet
		if(mapConditionLevel != null && mapConditionLevel.size() > 0){
			if(group == null){
				// Store
				mapConditionLevel.each{key, value->
					if(condition == key){
						ret = value
					}
				}
			}else{
				// Subtyped
				mapConditionLevel[group].each{key, value->
					if(condition == key){
						ret = value
					}
				}
			}
		}
		if(ret == "Low"){
			intRet = 1
		}
		if(ret == "Medium"){
			intRet = 2
		}
		if(ret == "High"){
			intRet = 3
		}
		return intRet
	}

	/**
	 * GENERATE DATA TO POLICY STRING
	 * @param data Data for generating policy's String
	 * @param listKey Keys of jobs
	 * @param type Job's type(Store/Subtyped)
	 * @param mapConditionLevel Map contains all condition-level
	 * @return policy's String
	 */
	def generatePolicyString(data, listKey, type, mapConditionLevel){
		logger.trace("Begin generate policy's string")
		def policyStr = ""
		//return data
		try {
			if(type == "store") {
				if(data.mappolicy != null && data.mappolicy.size() > 0){
					policyStr += "POLICY = {resultData->\n"
					policyStr += "\tdef listMess = []\n"
					policyStr += "\tdef ret = ['jobName' : '" + data.jobName + "', 'istIid' : '" + data.instanceName + "']\n"
					def mapPolicy = data.mappolicy
					policyStr += "\tresultData.each{data->\n"
					mapPolicy.each {key,value ->
						key = key.trim()
						if(key[0] != "("){
							key = "(" + key
						}
						if(key[key.size() - 1] != ")"){
							key = key + ")"
						}

						// if statement
						policyStr += "\t\tif(" + getDataConditionsAfterEdit(key, listKey) + "){\n"
						// message print statement
						policyStr += "\t\t\tlistMess.add([level: " + getLevel(key, mapConditionLevel) + ", message: \"\"\""+ value +"\"\"\"])\n\t\t}\n"
					}
					policyStr += "\t}\n"
					policyStr += "\tret['message'] = listMess\n"
					policyStr += "\treturn ret\n}"
				}
			} else if(type == "subtyped") {
				policyStr += "POLICY = {resultData->\n"
				policyStr += "\tdef listMess = []\n"
				policyStr += "\tdef ret = ['jobName' : '" + data.jobName + "', 'istIid' : '" + data.instanceName + "']\n"
				policyStr += "\tresultData.each {key,value ->\n"
				data.mappolicy.each {keyData,valueData ->
					if(valueData != [:]) {
						policyStr += "\t\tif(key == \"" + keyData + "\") {\n"
						policyStr += "\t\t\tvalue.each {data ->\n"
						valueData.each {key,value ->
							// If
							policyStr += "\t\t\t\tif(" + getDataConditionsAfterEdit(key, listKey) + "){\n"
							// Message
							policyStr += "\t\t\t\t\tlistMess.add([level: " + getLevel(key, mapConditionLevel, keyData) + ", message: \"\"\""+ value +"\"\"\"])\n"
							policyStr += "\t\t\t\t}\n"
						}
						policyStr += "\t\t\t}\n"
						policyStr += "\t\t}\n"
					}
				}
				policyStr += "\t}\n"
				policyStr += "\tret['message'] = listMess\n"
				policyStr += "\treturn ret\n"
				policyStr += "}"
			}
			logger.trace("Finish generate policy's string")
			logger.trace(policyStr)
			return policyStr
		} catch(Exception ex) {
			return "ex:" + ex
		}
	}

	/**
	 * Read and isolate the condition
	 * @param stringOfPolicy Policy's String
	 * @param dataKey key
	 * @return condition's string
	 */
	def getDataConditionsAfterEdit(String stringOfPolicy, dataKey){
		List OperatorList = [
			" ",
			"\\(",
			"\\)",
			"=",
			"\\+|\\-|\\*|\\/|%",
			">|<|=|!",
			"\\|\\||&&|\\?\\:",
			"\\~|<<|>>|>>>|&|\\^|\\|"
		]

		//Replace all unnecessary space
		String macherPattern = "([ ]{2,})"
		Pattern pattern = Pattern.compile(macherPattern, Pattern.DOTALL);
		stringOfPolicy = "(" + stringOfPolicy.replaceAll(pattern, " ").trim() + ")"

		String strKeyPattern = convertListToString(dataKey, "|")
		String strOperator = convertListToString(OperatorList, "|")

		//Create macher
		macherPattern = "(" + strOperator + ")(" + strKeyPattern + ")(" + strOperator + "|\\.)"
		pattern = Pattern.compile(macherPattern, Pattern.DOTALL);
		Matcher matcher = pattern.matcher(stringOfPolicy);
		def oldData
		def newData
		while(matcher.find()){
			oldData = matcher.group()
			newData = matcher.group(1) + "data." + matcher.group(2) + matcher.group(3)
			stringOfPolicy = stringOfPolicy.replace(oldData, newData)
		}
		stringOfPolicy = stringOfPolicy.substring(1, stringOfPolicy.length() -1)
		return stringOfPolicy
	}

	/**
	 * Read and isolate the message
	 * @param stringOfMessage Policy's String
	 * @param dataKey key
	 * @return message's string
	 */
	def getDataMessageAfterEdit(String stringOfMessage, dataKey){
		//Replace all unnecessary space
		String macherPattern = "([ ]{2,})"
		Pattern pattern = Pattern.compile(macherPattern, Pattern.DOTALL);
		stringOfMessage = stringOfMessage.replaceAll(pattern, " ").trim()
		stringOfMessage = " " + stringOfMessage + " "
		stringOfMessage = stringOfMessage.replaceAll('"""', '\'\'\'')
		String strKeyPattern = convertListToString(dataKey, "|")

		//Remove unneed data
		macherPattern = "(\\\$\\{data\\.)(" + strKeyPattern + ")(\\})"
		pattern = Pattern.compile(macherPattern, Pattern.DOTALL);
		Matcher matcher = pattern.matcher(stringOfMessage);
		while(matcher.find()){
			stringOfMessage = stringOfMessage.replace(matcher.group(), matcher.group(2))
		}

		//Create macher
		macherPattern = "((?:(?!(\\d|[a-zA-Z]|\'|\")).)+)(" + strKeyPattern + ")((?:(?!(\\d|[a-zA-Z]|\'|\")).)+)"
		pattern = Pattern.compile(macherPattern, Pattern.DOTALL);
		matcher = pattern.matcher(stringOfMessage);
		def oldData
		def newData
		while(matcher.find()){
			oldData = matcher.group()
			newData = matcher.group(1) + '${data.' + matcher.group(3) + '}' + matcher.group(4)
			stringOfMessage = stringOfMessage.replace(oldData, newData)
		}
		stringOfMessage = stringOfMessage.substring(1, stringOfMessage.length() -1)
		stringOfMessage = stringOfMessage.replace('"', '')
		stringOfMessage = stringOfMessage.replace('\'\'\'', '')
		return stringOfMessage
	}
	
	/**
	 * Convert list data to string
	 * @param listData
	 * @return
	 */
	def convertListToString (List listData, String concatStr = "|"){
		def strRet = ""
		listData.each {key->
			strRet += key + concatStr
		}
		if (strRet != "") {
			strRet = strRet.subSequence(0, strRet.length() - concatStr.length())
		}
	}
}
