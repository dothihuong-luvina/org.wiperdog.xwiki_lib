package org.wiperdog.RealtimeLib

import com.google.gson.Gson;
import com.mongodb.util.JSON
import java.text.SimpleDateFormat
import org.wiperdog.DrawChartDataLib.DataToDrawChart
import org.wiperdog.WiperdogLogger.WiperdogLogger4Xwiki;

class RealtimeDataLib {
	def static WiperdogLogger4Xwiki logger = new WiperdogLogger4Xwiki("RealtimeDataLib")
	
	/**
	 * draw chart with data respective
	 * @param key key of data subtyped
	 * @param value first data draw chart
	 * @param typeChart type of data draw chart
	 */
	def drawChart(key,value,typeChart) {
		logger.trace("Begin getting Pie data for realtime monitoring")
		def dataPie = getDataJson(value,"pie")
		def lstName

		//get list name of data pie
		if(dataPie.size() > 0 ){
			lstName = dataPie['name']
		}
		//convert name of list name of data pie to format number_name
		def tmpListNameContainer = []
		if(lstName != null) {
			def chartNumPie = 0
			def tmpLst
			//for each data of list name
			for(def nameContainer in lstName){
				tmpLst = []
				for(def nameContainerItem in nameContainer ){
					nameContainerItem = chartNumPie + "_" + nameContainerItem.replaceAll("\\.", "_")
					if(typeChart == "Store") {
						//add to list name container of data Store
						tmpListNameContainer.add(nameContainerItem)
					} else if(typeChart == "Subtyped") {
						tmpLst.add(nameContainerItem )
					}
				}
				//add to list name container of data Subtyped
				if(typeChart == "Subtyped") {
					tmpListNameContainer.add(tmpLst)
					chartNumPie++
				}
			}
		}
		//get data json of pie
		def jsonPie = parseJson(dataPie)

		//get data json of container pie
		def jsonContainerPie = parseJson(tmpListNameContainer)

		//Prepare data, list container for draw chart and tags of hidden chart
		def mapDataToDraw = [:]
		mapDataToDraw['value'] = value
		mapDataToDraw['typeChart'] = typeChart
		mapDataToDraw['jsonPie'] = jsonPie
		mapDataToDraw['jsonContainerPie'] = jsonContainerPie
		mapDataToDraw['key'] = key
		logger.trace("Finish getting Pie data for realtime monitoring")
		return mapDataToDraw
	}
	/**
	 * getDataJson Get data to draw chart
	 * @param value first data draw chart
	 * @param type type of chart
	 * @return data data after convert to draw chart
	*/
	def getDataJson(value,type) {
		def data = []
		if(type == "pie" && DataToDrawChart.getDataToDrawPie(value) != []) {
			//get data to draw pie
			data = DataToDrawChart.getDataToDrawPie(value)
		}
		return data
	}
	
	/**
	 * parseJson: Convert data to json
	 * @param data data need to convert
	 * @return json data after convert to json
	*/
	def parseJson(data) {
		def json
		if(data != []) {
			//convert data to json
			json = JSON.serialize(data)
		}
		return json
	}
	
	/**
	 * getAdditionData: get informations of points to be added from previous monitoring to current monitoring to draw chart
	 * @param collection table data in mongo db
	 * @param key key of data (store, subtyped)
	 * @param typeOfChart line, bar, area
	 * @return finalListData
	*/
	def getAdditionData(collection, key) {
		def finalListData = []
		def chartAreaNum = 0
		def chartBarNum = 0
		if(collection != null && collection[0] != null && collection[0].KEYEXPR != null){
			def key_chart = collection[0].KEYEXPR._chart
			def key_root = collection[0].KEYEXPR._root
			def key_unit = collection[0].KEYEXPR._unit
			if (key_unit == null) {
			key_unit = [:]
			}
			if (key_root == null) {
			key_root = [:]
			}
			//Convert format
			def lstDataForKey = []
			key_chart.each{chart->
				
			   //Get declaration chart info
			   def chartCollumns = chart.chart_columns
		   def hintColumns = chart.chart_columns.clone()
		   if(chart.hint_columns != null){
			   chart.hint_columns.each{hintCol->
				   if(!hintColumns.contains(hintCol)){
					   hintColumns.add(hintCol)
				   }
			   }
				}
				if (chart.type == "line") {
					lstDataForKey = getAdditionDataLine(collection, key, chartCollumns, hintColumns, key_root, key_unit)
				} else if (chart.type == "bar") {
					lstDataForKey = getAdditionDataBar(collection, chartCollumns, hintColumns, key_root, key_unit, chartBarNum, key)
					chartBarNum ++
				} else if (chart.type == "area") {
					lstDataForKey = getAdditionDataArea(collection, chartCollumns, hintColumns, key_root, key_unit, chartAreaNum, chart.name, key)
					chartAreaNum ++
				}
				lstDataForKey.each {
					finalListData.add(it)
				}
				lstDataForKey = []
			}
			return finalListData
		}
	}
	
	/**
	 * getAdditionDataLine: get informations of points to be added from previous monitoring to current monitoring to draw line chart
	 * @param collection table data in mongo db
	 * @param key key of data (store, subtyped)
	 * @param chartCollumns
	 * @param hintColumns
	 * @param key_root
	 * @param key_unit
	 * @return returnResult
	*/
	def getAdditionDataLine(collection, key = '', chartCollumns, hintColumns, key_root, key_unit) {
		logger.trace("Begin getting addition data of Line chart for realtime monitoring $collection")
		def returnResult = []
		def resultRecord = [:]
		//Chart name
		def chartName
		chartCollumns.each{col->
			if(chartName != null){
				chartName += "_" + col
		   }else {
			   chartName = col
		   }
		}
		collection.each{record->
			//Type
			resultRecord["type"] = "line_"
			//Chart name
			resultRecord["chartName"] = chartName
			if (key != '') {
				resultRecord["type"] = "line"
				resultRecord["chartName"] = key + "_" + chartName
			}
			//Categories
			resultRecord["categories"] = convertFetchAtBin(record.fetchedAt_bin)
	
			// Chart data
			def chartDataList = []
			def chartRecord
				  
			//Hint data
			def hintDataList = []
			def hintRecord
	
			//Loop each data in one monitor data
			def dataMonitor = record['data']
	
						 if (key_root == [:] && record['data'].size() >= 1) {
							 def newCollection = []
							 newCollection.add(record['data'][record['data'].size() - 1])
							 dataMonitor = newCollection
						 }
			dataMonitor.each{ dataRecord ->
				//Get name key
			def chartKeyName
			key_root.each{keyRoot->
				if(chartKeyName == null){
					chartKeyName = dataRecord[keyRoot]
			}else{
				chartKeyName+= "." + dataRecord[keyRoot]
			}
				}
				//Loop to get chart data
				chartCollumns.each{ chartColumnItem ->
					chartRecord = [:]
					hintRecord = [:]
							 
					//Get series name
					if (chartKeyName != null){
						chartRecord["seriesName"] = chartKeyName + "($chartColumnItem)"
						hintRecord["seriesName"] = chartKeyName + "($chartColumnItem)"
					} else {
						chartRecord["seriesName"] = chartColumnItem
						hintRecord["seriesName"] = chartColumnItem
					}
					//Get value of chart data
					chartRecord["value"] = dataRecord[chartColumnItem]
							 
					//Add to list data
					chartDataList.add(chartRecord)
							 
					//Get hint data
					def hintRecordDetail = [:]
					def keyUnit
					hintColumns.each{hintColumnRecord->
						hintRecordDetail[hintColumnRecord] = dataRecord[hintColumnRecord]
						if (key_unit != null && key_unit[hintColumnRecord] != null && key_unit[hintColumnRecord] != "") {
							keyUnit = key_unit[hintColumnRecord]
							hintRecordDetail[hintColumnRecord] += "( $keyUnit )"
						}
					}
					hintRecordDetail["fetchAt"] = convertFetchAtBin(record.fetchedAt_bin)
					hintRecord["detail"] = hintRecordDetail
							 
					//Add to list hint data
					hintDataList.add(hintRecord)
				}
			}
			//Chart data
			resultRecord["data"] = chartDataList
	
			//Hint data
			resultRecord["detailData"] = hintDataList
			returnResult.add(resultRecord)
			resultRecord = [:]
			chartDataList = []
			hintDataList = []
		}
		logger.trace("Finish getting addition data of Line chart for realtime monitoring $collection : " + returnResult.size() + "record(s).")
		//Return
		return returnResult
	}
	
	/**
	 * getAdditionDataBar: get informations of points to be added from previous monitoring to current monitoring to draw bar chart
	 * @param collection table data in mongo db
	 * @param key key of data (store, subtyped)
	 * @param chartCollumns
	 * @param hintColumns
	 * @param key_root
	 * @param key_unit
	 * @return returnResult
	*/
	def getAdditionDataBar(collection, chartCollumns, hintColumns, key_root, key_unit, chart_Num, key = '') {
		logger.trace("Begin getting addition data of Bar chart for realtime monitoring $collection")
		def returnResult = []
		def resultRecord = [:]
		chartCollumns.each{chartCol->
						// Chart data
						def chartDataList = []
						def chartRecord
						
						//Hint data
						def hintDataList = []
						def hintRecord
	  
						//chart name
						def chartName = chart_Num + "_" + chartCol
	
						collection.each{record->
							resultRecord["type"] = "bar"
							resultRecord["chartName"] = chartName
							if (key != '') {
								resultRecord["type"] = "bar"
								resultRecord["chartName"] = key + "_" + chartName
							}
	
							//Categories
							resultRecord["categories"] = convertFetchAtBin(record.fetchedAt_bin)
	
							//Loop each data in one monitor data
							def dataMonitor = record['data']
							dataMonitor.each{ dataRecord ->
								chartRecord = [:]
								hintRecord = [:]
	
							   //Get name key
					   def chartKeyName
					   key_root.each{keyRoot->
						   if(chartKeyName == null){
								   chartKeyName = dataRecord[keyRoot]
						   }else{
						   chartKeyName+= "." + dataRecord[keyRoot]
						   }
					   }
	
							   //Get series name
							   if (chartKeyName != null){
								   chartRecord["seriesName"] = chartKeyName
								   hintRecord["seriesName"] = chartKeyName
							   } else {
								   chartRecord["seriesName"] = chartCol
								   hintRecord["seriesName"] = chartCol
							   }
						   
							   //Get value of chart data
							   chartRecord["value"] = dataRecord[chartCol]
	
							   //Add to list data
							   chartDataList.add(chartRecord)
	
							   //Get hint data
							   def hintRecordDetail = [:]
							   def keyUnit
							   hintColumns.each{hintColumnRecord->
								   hintRecordDetail[hintColumnRecord] = dataRecord[hintColumnRecord]
								   if (key_unit != null && key_unit[hintColumnRecord] != null && key_unit[hintColumnRecord] != "") {
									   keyUnit = key_unit[hintColumnRecord]
									   hintRecordDetail[hintColumnRecord] += "( $keyUnit )"
								   }
							   }
							   hintRecordDetail["fetchAt"] = convertFetchAtBin(record.fetchedAt_bin)
							   hintRecord["detail"] = hintRecordDetail
							
							   //Add to list hint data
							   hintDataList.add(hintRecord)
							}
	
							//Chart data
							resultRecord["data"] = chartDataList
	
							//Hint data
							resultRecord["detailData"] = hintDataList
							returnResult.add(resultRecord)
							resultRecord = [:]
							chartDataList = []
							hintDataList = []
						}
		}
		logger.trace("Finish getting addition data of Bar chart for realtime monitoring $collection : " + returnResult.size() + "record(s).")
		//Return
		return returnResult
	}
	
	/**
	 * getAdditionDataArea: get informations of points to be added from previous monitoring to current monitoring to draw area chart
	 * @param collection table data in mongo db
	 * @param key key of data (store, subtyped)
	 * @param chartCollumns
	 * @param hintColumns
	 * @param key_root
	 * @param key_unit
	 * @return returnResult
	*/
	def getAdditionDataArea(collection, chartCollumns, hintColumns, key_root, key_unit, chart_Num, nameOfChart, key = '') {
		logger.trace("Begin getting addition data of Area chart for realtime monitoring $collection")
		def returnResult = []
		def resultRecord = [:]
		collection.each{record->
						 // Chart data
						 def chartDataList = []
						 def chartRecord = [:]
					 
						 //Hint data
						 def hintDataList = []
						 def hintRecord = [:]
	
						 //Loop each data in one monitor data
						 def dataMonitor = record['data']
	
						 if (key_root == [:] && record['data'].size() >=1) {
							 def newCollection = []
							 newCollection.add(record['data'][0])
							 dataMonitor = newCollection
						 }
			   
						 dataMonitor.each{ dataRecord ->
							 // type
							 resultRecord["type"] = "area"
	
							 // Categories
							 resultRecord["categories"] = convertFetchAtBin(record.fetchedAt_bin)
	 
							 //Chart name
							 def chartContainerName = chart_Num
							 def chartName = nameOfChart
							 def nameOfChartAddition = ""
							 
							 key_root.each{col->
							 chartContainerName += "_" + dataRecord[col]
								 nameOfChartAddition += "." + dataRecord[col]
							 }
							 if (nameOfChartAddition != '') {
								 nameOfChartAddition = nameOfChartAddition
								 nameOfChartAddition = nameOfChartAddition.substring(1, nameOfChartAddition.length())
								 chartName += "( " + nameOfChartAddition  + " )"
							 } else {
								 chartName += "( " + chart_Num + "_root" + " )"
							 }
								  
					 if (chartContainerName == chart_Num) {
						 chartContainerName += "_root"
							 }
							 chartContainerName = chartContainerName.replaceAll("\\.", "_")
							 resultRecord["chartName"] = chartContainerName
							 if (key != '') {
								resultRecord["type"] = "area"
								resultRecord["chartName"] = key + "_" + chartContainerName
							 }
							 resultRecord["nameOfChart"] = chartName
	
							 //Loop to get chart data
							 chartCollumns.each{ chartColumnItem ->
								 //Get series name
								 chartRecord["seriesName"] = chartColumnItem
								 hintRecord["seriesName"] = chartColumnItem
							 
								 //Get value of chart data
								 chartRecord["value"] = dataRecord[chartColumnItem]
							 
								 //Add to list data
								 chartDataList.add(chartRecord)
							 
								 //Get hint data
								 def hintRecordDetail = [:]
								 def keyUnit
								 hintColumns.each{hintColumnRecord->
									 hintRecordDetail[hintColumnRecord] = dataRecord[hintColumnRecord]
									 if (key_unit != null && key_unit[hintColumnRecord] != null && key_unit[hintColumnRecord] != "") {
										 keyUnit = key_unit[hintColumnRecord]
										 hintRecordDetail[hintColumnRecord] += "( $keyUnit )"
									 }
								 }
								 hintRecordDetail["fetchAt"] = convertFetchAtBin(record.fetchedAt_bin)
								 hintRecord["detail"] = hintRecordDetail
							 
								 //Add to list hint data
								 hintDataList.add(hintRecord)
	
								 chartRecord = [:]
								 hintRecord = [:]
							 }
							 //Chart data
							 resultRecord["data"] = chartDataList
	
							 //Hint data
							 resultRecord["detailData"] = hintDataList
							 returnResult.add(resultRecord)
							 
							 //Reset data
							 resultRecord = [:]
							 chartDataList = []
							 hintDataList = []
							 nameOfChartAddition = ""
						 }
		}
		logger.trace("Finish getting addition data of Area chart for realtime monitoring $collection : " + returnResult.size() + "record(s).")
		//Return
		return returnResult
	}
	
	/**
	 * convertFetchAtBin: convert fetchAt_bin to format "yyyy/MM/dd HH:mm:ss z"
	 * @param fetchedAt_bin
	*/
	def convertFetchAtBin(fetchedAt_bin) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss z")
		def milis = ((Long)fetchedAt_bin) * 1000
		String dateFormatted = sdf.format(new Date(milis))
		return dateFormatted
	}
}
