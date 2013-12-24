package org.wiperdog.CustomMongoDbConnection
import com.gmongo.GMongo;
import java.text.SimpleDateFormat

import org.wiperdog.WiperdogLogger.WiperdogLogger4Xwiki;

class CMongoDBConn {
	def db
	GMongo gmongo
	def paramDir
	def static WiperdogLogger4Xwiki logger = new WiperdogLogger4Xwiki("CMongoDBConn")

	/**
	 * Get connection with param file's path
	 * @param filePath File's path
	 * @return Connection
	 */
	def getConnection(String filePath){
		logger.info("Begin getting connection ")
		def ret
		try{
			File paramFile = new File(filePath)
			ret = getConnection(paramFile)
		}catch(Exception ex){
			throw ex
		}
		logger.info("Finish getting connection:" + ret)
		return ret
	}

	/**
	 * Get Connection with param's file
	 * @param param_file Param's file
	 * @return Connection
	 * @throws Exception
	 */
	def getConnection(File param_file) throws Exception{
		logger.info("Begin getting connection with " + param_file)
		def decidedHost
		def decidedPort
		def decidedDbName
		def decidedUser
		def decidedPass
		if(param_file.exists()){
			GroovyShell shell = new GroovyShell()
			def params = shell.evaluate(param_file)
			def declaredMongoDB = params['mongoDB']
			if(declaredMongoDB != null){
				decidedDbName = declaredMongoDB['dbName']
				decidedHost = declaredMongoDB['host']
				decidedPort = declaredMongoDB['port']
				decidedUser = declaredMongoDB['user']
				decidedPass = declaredMongoDB['pass']
				if(decidedDbName == null || decidedHost == null || decidedPort == null || decidedUser == null || decidedPass == null){
					throw new Exception('host, port, user name, password must be full filled!')
				}else{
					return getConnection(decidedHost, decidedPort, decidedDbName, decidedUser, decidedPass)
				}
			}else{
				throw new Exception('Params file doesn\'t contain mongoDB')
			}
		}else{
			throw new Exception('File params doesn\'t existed!')
		}
	}

	/**
	 * Get Connection with host, port, database's name, user's name, password
	 * @param host Host
	 * @param port Port
	 * @param dbname Database's name
	 * @param userName User's name
	 * @param password Password
	 * @return Connection
	 * @throws Exception
	 */
	def getConnection(host, port, dbname, userName, password) throws Exception{
		logger.info("Begin getting connection with host : $host , port : $port , user : $userName)")
		try{
			this.gmongo = new GMongo(host, port)
			this.db = gmongo.getDB(dbname)
			if(userName != ''){
				db.authenticate(userName, password.toCharArray())
			}
		}catch(Exception ex){
			throw ex
		}
		logger.info("Current connection is : " + gmongo)
		logger.info("Current database is : " + db)
		return gmongo
	}

	/**
	 * Close the connection
	 * @return
	 */
	def closeConnection(){
		logger.info('Closed connection:' + gmongo)
		gmongo.close()
	}

	/**
	 * Get db variable to interact with database
	 * @return
	 */
	def getMongoDB(){
		return db
	}

	/**
	 * Get all data of the collection
	 * @param collection Collection's name
	 * @return data
	 */
	def getDataAllFields(collection){
		logger.trace("Begin getting all data")
		def list_data = []
		assert collection != null && collection != "" : "Can not get data ! Collection is null or empty string"
		def result = db[collection].find();
		while(result.hasNext()){
			list_data.add(result.next())
		}
		logger.trace("Got " + list_data.size() + " record(s).")
		return list_data
	}

	/**
	 * Get all data of the collection with a limit
	 * @param collection Collection's name
	 * @param limit Limit
	 * @param istIid IstIid of collection
	 * @return data
	 */
	def getDataAllFields(collection,limit,istIid){
		def list_data = []
		assert collection != null && collection != "" : "Can not get data ! Collection is null or empty string"
		assert limit != null : "Can not get data ! Limit is null"
		def realCollectionName = collection + "." + istIid
		logger.trace("Begin getting all data of $realCollectionName with limit of $limit")
		def result = db[realCollectionName].find().sort( fetchAt : -1).limit(limit)
		while(result.hasNext()){
			list_data.add(result.next())
		}
		logger.trace("Got " + list_data.size() + " record(s).")
		return (ArrayList)list_data.reverse()
	}

	/**
	 * Get data of the collection with a filter is field's name
	 * @param collection Collection's name
	 * @param fields List fields
	 * @return data
	 */
	def getDataLimitFields(collection,fields){
		logger.trace("Begin getting $collection data of these fields : $fields")
		def list_data = []
		assert collection != null && collection != "" : "Can not get data ! Collection is null or empty string"
		assert (fields !=null && fields !=[]) : "Can not get data ! List fields is null or empty"
		def result = db[collection].find().sort(fetchAt: -1)
		def tmp = [:]
		while(result.hasNext()){
			def doc = result.next()
			doc.data.each{
				if( tmp["fetchAt"] == null){
					tmp["fetchAt"] = doc.fetchAt
				}
				fields.each{item->
					tmp[item] = it[item]
				}
				list_data.add(tmp)
				tmp=[:]
			}
		}
		logger.trace("Got " + list_data.size() + " record(s).")
		return (ArrayList)list_data.reverse()
	}

	/**
	 * Get data of the collection with a filter is field's name with a limit
	 * @param collection Collection's name
	 * @param fields List fields
	 * @param limit Limit
	 * @return Data
	 */
	def getDataLimitFields(collection,fields,limit){
		logger.trace("Begin getting $collection data of these fields : $fields with limit of $limit")
		def list_data = []
		assert collection != null && collection != "" : "Can not get data ! Collection is null or empty string"
		assert limit != null : "Can not get data ! Limit is null"
		assert (fields !=null && fields !=[]) : "Can not get data ! List fields is null or empty"
		def result = db[collection].find().sort(fetchAt: -1).limit(limit)
		def tmp = [:]
		while(result.hasNext()){
			def doc = result.next()
			doc.data.each{
				if( tmp["fetchAt"] == null){
					tmp["fetchAt"] = doc.fetchAt
				}
				fields.each{item->
					tmp[item] = it[item]
				}
				list_data.add(tmp)
				tmp=[:]
			}
		}
		logger.trace("Got " + list_data.size() + " record(s).")
		return (ArrayList)list_data.reverse()
	}

	/**
	 * Get data type of this collection: Store/Subtyped
	 * @param collection Collection's name
	 * @return type of job
	 */
	def getDataType(collection){
		assert collection != null && collection != "" : "Can not get data ! Collection is null or empty string"
		def result = db[collection].find().sort(fetchAt: -1).limit(1)
		def doc
		while(result.hasNext()){
			doc = result.next()
		}
		if (doc != null) {
			return doc.type
		} else {
			return null
		}
	}

	/**
	 * Get data from date to date with limitation
	 * @param collection Collection's Name (In wiperdog, it's job's name)
	 * @param fromDate Date to begin filtered
	 * @param toDate Date to end filtered
	 * @param limit Records' limit
	 * @param istIid IstIid of the job
	 * @return Filtered list data
	 */
	def getDataInPeriod(collection,fromDate,toDate,limit,istIid){
		def realCollectionName = collection + "." + istIid
		logger.trace("Begin getting data of $realCollectionName from : $fromDate to : $toDate with limit of $limit")
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
		def list_data = []
		assert collection != null || collection != "" || istIid != null || istIid !="" : "Can not get data ! Collection is null or empty string"
		assert limit != null : "Can not get data ! Limit is null"
		assert (fromDate != null && toDate != null) : "Can not get data ! 'From Date' or 'To date' is null !"
		def result = null
		if( toDate!= "" ) {
			toDate = (Long)(format.parse(toDate).getTime()/1000.intValue())
			if(fromDate!= "" ){
				fromDate = (Long)(format.parse(fromDate).getTime()/1000.intValue())
				result = db[realCollectionName].find( fetchedAt_bin: [ $gt: fromDate ,$lt: toDate ]).sort(fetchAt: -1).limit(limit)
			} else {
				result = db[realCollectionName].find( fetchedAt_bin: [ $lt: toDate ]).sort(fetchAt: -1).limit(limit)
			}
		} else {
			if(fromDate!= "" ){
				fromDate = (Long)(format.parse(fromDate).getTime()/1000.intValue())
				result = db[realCollectionName].find( fetchedAt_bin: [ $gt: fromDate ]).sort(fetchAt: -1).limit(limit)
			} else {
				return this.getDataAllFields(collection,limit,istIid)
			}
		}
		if(result!=null){
			while(result.hasNext()){
				list_data.add(result.next())
			}
			logger.trace("Got " + list_data.size() + " record(s).")
			return (ArrayList)list_data.reverse()
		}
	}
}
