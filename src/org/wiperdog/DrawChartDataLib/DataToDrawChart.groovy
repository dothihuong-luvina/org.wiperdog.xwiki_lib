package org.wiperdog.DrawChartDataLib

import org.wiperdog.WiperdogLogger.WiperdogLogger4Xwiki;

class DataToDrawChart {
	def static WiperdogLogger4Xwiki logger = new WiperdogLogger4Xwiki("DataToDrawChart")
	
	/**
	 * Get data for BAR chart
	 * @param collection Collection's name
	 * @return data
	 */
	def static getDataToDrawBar(collection){
		logger.info("Begin getting data to draw bar of " + collection)
		def result
		def dataSize = collection.size
		if (dataSize > 2) {
			result = [
				collection[dataSize - 2],
				collection[dataSize - 1]
			]
		} else {
			result = collection
		}
		def dataChart = result.KEYEXPR._chart[0]
		def type
		def resultData
		def finalResultData = []
		def  unit = result[0].KEYEXPR._unit
		if (unit == null) {
			unit = [:]
		}
		dataChart.each {
			type = it.type
			if(type == "bar") {
				resultData = []
				// get data to draw bar chart
				def chart_columns = it.chart_columns
				def hint_columns = it.hint_columns
				if (hint_columns == null ){
					hint_columns = []
					result[0].data[0].each{
						hint_columns.add(it.key)
					}
				}
				def finalData = [:]
				chart_columns.each {chartColumn ->
					def KEYEXPR
					def dataToDraw = [:]
					def dataToToolTip = [:]

					// xAxis
					def mapCategories = [:]
					mapCategories['categories'] = result['fetchAt']
					finalData['xAxis'] = mapCategories

					// chart_name
					finalData['chart_name'] = it.name

					// Chart Column and Hint Column
					finalData['chart_columns'] = chart_columns
					finalData['hint_columns'] = hint_columns

					if(result.KEYEXPR != null) {
						KEYEXPR = result.KEYEXPR._root[0]
					}
					if(KEYEXPR != null) {
						def lstKey = []
						result.each{record->
							record.data.each {dat->
								def tmp = []
								KEYEXPR.each{
									tmp.add(dat[it])
								}
								if(!lstKey.contains(tmp)){
									lstKey.add(tmp)
								}
							}
						}
						result['data'].each {data->
							lstKey.each {keySet->
								def dataChartColumn
								def key = ""
								keySet.each{ key += it + "." }
								key = key.substring(0, key.length()-1)

								if(dataToDraw[key] == null){
									dataToDraw[key] = []
								}
								data.each {dat->
									def isData = true
									for(int i = 0; i< KEYEXPR.size(); i++){
										isData = isData && (dat[KEYEXPR[i]] == keySet[i])
									}
									if(isData){
										dataChartColumn = dat[chartColumn]
									}
								}
								dataToDraw[key].add(dataChartColumn)
							}
						}

						def series = []
						dataToDraw.each{
							def tmp = [:]
							tmp['name'] = it.key
							tmp['data'] = it.value
							series.add(tmp)
						}
						finalData['series'] = series

						// get detail data to draw tooltip
						def hintData = [:]
						result.each {elementResult ->
							elementResult['data'].each {elementData ->
								def mapHintData = [:]
								mapHintData['fetchAt'] = elementResult.fetchAt
								hint_columns.each {elementHint ->
									mapHintData[elementHint] = elementData[elementHint]
									unit.each{key, value ->
										if(elementHint == key) {
											mapHintData[elementHint] = elementData[elementHint] + " ( " + value + " )"
										}
									}
								}
								def key = ""
								KEYEXPR.each {eKeyexpr ->
									key += elementData[eKeyexpr] + "."
								}
								key = key.substring(0, key.length()-1)
								if(hintData[key] == null) {
									hintData[key] = []
								}
								hintData[key].add(mapHintData)
								mapHintData = [:]
							}
						}
						def detail_data = []
						def mapFinalData = [:]
						hintData.each {key,value ->
							mapFinalData['name'] = key
							mapFinalData['data'] = value
							detail_data.add(mapFinalData)
							mapFinalData = [:]
						}
						finalData['detail_data'] = detail_data
					} else {
						// series
						def series = []
						def tmp = [:]
						tmp['name'] = chartColumn
						tmp['data'] = []
						result.each {
							tmp['data'].add(it.data[0][chartColumn])
						}
						series.add(tmp)
						finalData['series'] = series

						// detail_data
						def hintData = []
						result.each {elementResult ->
							def mapHintData = [:]
							mapHintData['fetchAt'] = elementResult.fetchAt
							hint_columns.each {elementHint->
								mapHintData[elementHint] = elementResult.data[0][elementHint]
								unit.each{key, value ->
									if(elementHint == key) {
										mapHintData[elementHint] = elementResult.data[0][elementHint] + " ( " + value + " )"
									}
								}
							}
							hintData.add(mapHintData)
							mapHintData = [:]
						}
						def mapFinalData = [:]
						def listFinalData = []
						mapFinalData['data'] = hintData
						listFinalData.add(mapFinalData)
						finalData['detail_data'] = listFinalData
					}
					resultData.add(finalData)
					finalData = [:]
				}
				finalResultData.add(resultData)
			}
		}
		logger.info("Finish getting data to draw bar")
		return finalResultData
	}

	/**
	 * Get data for LINE chart
	 * @param collection Collection's name
	 * @return data
	 */
	def static getDataToDrawLine(collection){
		logger.info("Begin getting data to draw line of " + collection)
		def finalResult = []
		def returnResult = []
		def key_chart = collection[0].KEYEXPR._chart
		def key_root = collection[0].KEYEXPR._root
		def key_unit = collection[0].KEYEXPR._unit
		if (key_unit == null) {
			key_unit = [:]
		}
		if (key_root == null) {
			key_root = [:]
		}
		def categories = []
		def listNameKey = []

		collection.each{record->
			categories.add(record.fetchAt)
		}

		collection.each {record->
			def data = record['data']
			data.each {dat->
				def datName
				key_root.each{keyRoot->
					if(datName == null){
						datName = dat[keyRoot]
					}else{
						datName += "." + dat[keyRoot]
					}
				}
				if(!listNameKey.contains(datName)){
					listNameKey.add(datName)
				}
			}
		}

		key_chart.each{chart->
			if(chart.type == "line"){
				def result = [:]
				def collumns = chart.chart_columns
				def hintColumns = chart.chart_columns.clone()
				if(chart.hint_columns != null){
					chart.hint_columns.each{hintCol->
						if(!hintColumns.contains(hintCol)){
							hintColumns.add(hintCol)
						}
					}
				}
				result.xAxis = [:]
				result.xAxis.categories = categories
				def chartName
				def title = chart.name
				collumns.each{col->
					if(chartName != null){
						chartName += "_" + col
					}else {
						chartName = col
					}
				}
				result.chart_name = chartName
				result.series = []
				result.chart_columns = collumns
				result.hint_columns = hintColumns
				result.detail_data = []
				result.title = title
				listNameKey.each{nameKey->
					collumns.each{col->
						def seriesRecord = [:]
						def detailDataRecord = [:]
						if(nameKey != null){
							seriesRecord['name'] = nameKey + "($col)"
							detailDataRecord['name'] = nameKey + "($col)"
						}else{
							seriesRecord['name'] = col
							detailDataRecord['name'] = col
						}
						seriesRecord['data'] = []
						detailDataRecord['data'] = []
						collection.each {record->
							boolean hasData = false
							def data = record['data']
							def eachData
							data.each{dat->
								def datName
								key_root.each{keyRoot->
									if(datName == null){
										datName = dat[keyRoot]
									}else{
										datName += "." + dat[keyRoot]
									}
								}
								if(datName == nameKey){
									hasData = true
									eachData = dat
								}
							}
							if(hasData){
								seriesRecord['data'].add(eachData[col])
								def fullDetailData = [:]
								fullDetailData.fetchAt = record.fetchAt
								if(hintColumns != null){
									hintColumns.each{hintCol->
										fullDetailData[hintCol] = eachData[hintCol]  + (key_unit[hintCol] != null ? ("( " + key_unit[hintCol] + " )") : "")
									}
								}else{
									fullDetailData = eachData
									fullDetailData.fetchAt = record.fetchAt
								}
								detailDataRecord['data'].add(fullDetailData)
							}else{
								seriesRecord['data'].add(null)
								detailDataRecord['data'].add(null)
							}
						}
						result.series.add(seriesRecord)
						result.detail_data.add(detailDataRecord)
					}
				}
				finalResult.add(result)
			}
		}
		if(finalResult != []) {
			returnResult.add(finalResult)
		}
		logger.info("Finish getting data to draw line")
		return returnResult
	}

	/**
	 * Get data for PIE chart
	 * @param collection Collection's name
	 * @return data
	 */
	def static getDataToDrawPie(collection){
		logger.info("Begin getting data to draw pie of " + collection)
		def result = collection[collection.size() - 1]
		def dataChart = result.KEYEXPR._chart
		def type
		def resultData
		def finalResultData = []
		def numOfChart
		dataChart.each {elementChart ->
			type = elementChart.type
			if(type == "pie") {
				numOfChart = 0
				resultData = []
				result.data.each {elementData ->
					def dataPie = [:]
					dataPie['type'] = "pie"
					dataPie['chart_name'] = elementChart.name
					if (result.KEYEXPR._unit != null) {
						dataPie['unit'] = '( ' + result.KEYEXPR._unit[elementChart.chart_columns[0]] + ' )'
					} else {
						dataPie['unit'] = ""
					}

					dataPie['data'] = []
					elementChart.chart_columns.each {
						def tmp = []
						tmp.add(it)
						tmp.add(elementData[it])
						dataPie['data'].add(tmp)
					}

					def KEYEXPR
					if(result.KEYEXPR != null) {
						KEYEXPR = result.KEYEXPR._root
					}
					dataPie['name'] = ""
					if(KEYEXPR != null) {
						KEYEXPR.each {
							dataPie['name'] += elementData[it] + "."
						}
						dataPie['name'] = dataPie['name'].substring(0, dataPie['name'].length()-1)
					} else {
						dataPie['name'] = elementChart.name.replace(" ", "") + numOfChart
						numOfChart++
					}
					resultData.add(dataPie)
				}
				finalResultData.add(resultData)
			}
		}
		logger.info("Finish getting data to draw pie")
		return finalResultData
	}

	/**
	 * Get data for AREA chart
	 * @param collection Collection's name
	 * @return data
	 */
	def static getDataToDrawArea(collection){
		logger.info("Begin getting data to draw area of " + collection)
		def result = collection
		def dataChart = result.KEYEXPR._chart[0]
		def type
		def resultData
		def finalResultData = []
		def lstKey
		def  unit = result[0].KEYEXPR._unit
		if (unit == null) {
			unit = [:]
		}

		dataChart.each {itemDataChart ->
			type = itemDataChart.type
			if(type == "area") {
				resultData = []
				def chart_columns = itemDataChart.chart_columns
				def keyExprRoot = null
				if(result.KEYEXPR != null) {
					keyExprRoot = result.KEYEXPR._root[0]
				}
				def finalData = [:]
				if(keyExprRoot == null) {// KEYEXPR hasn't _root
					// xAxis
					def mapCategories = [:]
					mapCategories['categories'] = result['fetchAt']
					finalData['xAxis'] = mapCategories
					// chart_name
					finalData['chart_name'] = itemDataChart.name

					def series = []
					def listFinalData = []
					def detail_data = []
					chart_columns.each {chartColumn ->

						// series
						def tmp = [:]
						tmp['name'] = chartColumn
						tmp['data'] = []
						result.each {itemResult ->
							if (itemResult.data[0] != null) {
								tmp['data'].add(itemResult.data[0][chartColumn])
							} else {
								tmp['data'].add(null)
							}
						}
						series.add(tmp)
						finalData['unit'] = unit[chartColumn]
					}
					finalData['series'] = series
					finalData['chartItemName'] = ["root"]
					resultData.add(finalData)
					finalData = [:]
				} else { // KEYEXPR has _root
					lstKey = []
					result.each{record->
						record.data.each {dat->
							def tmp = []
							keyExprRoot.each{
								tmp.add(dat[it])
							}
							if(!lstKey.contains(tmp)){
								lstKey.add(tmp)
							}
						}
					}
					def itemList = []
					lstKey.each {keySet ->
						// xAxis
						def mapCategories = [:]
						mapCategories['categories'] = result['fetchAt']
						finalData['xAxis'] = mapCategories

						def valueOfKey = ""
						keySet.each{ valueOfKey += it + "." }
						valueOfKey = valueOfKey.substring(0, valueOfKey.length()-1)
						finalData['chart_name'] = itemDataChart.name + " (" + valueOfKey + ")"
						itemList.add(valueOfKey.replaceAll("\\.","_"))
						finalData['series'] = []
						def mapSeries
						chart_columns.each {itemChart ->
							mapSeries = [:]
							mapSeries['name'] = itemChart
							mapSeries['data'] = []
							def isHasData
							result['data'].each {oneRunData ->
								isHasData = false
								oneRunData.each {itemData ->
									def dataKey = ""
									keyExprRoot.each {itemKeyExpr ->
										dataKey += itemData[itemKeyExpr] + "."
									}
									dataKey = dataKey.substring(0, dataKey.length()-1)
									if (valueOfKey.equals(dataKey)) {
										isHasData = true
										mapSeries['data'].add(itemData[itemChart])
									}
								}
								if (!isHasData) {
									mapSeries['data'].add(null)
								}
							}
							finalData['series'].add(mapSeries)
							finalData['unit'] = unit[itemChart]
						}
						finalData['chartItemName'] = itemList
						resultData.add(finalData)
						finalData = [:]
					}
				} // End else
				finalResultData.add(resultData)
			}
		}
		logger.info("Finish getting data to draw area")
		return finalResultData
	}

	/**
	 * Get data to draw subtyped
	 * @param dataSubtype Subtyped's data
	 * @return data
	 */
	def static getDataToDrawSubtype(dataSubtype){
		logger.info("Begin getting subtyped data")
		def listDataStore = []
		def mapForKey = [:]

		def listKey = []
		dataSubtype[0]['data'].each {
			listKey.add(it.key)
		}
		listKey.each {keySubtype ->
			mapForKey[keySubtype] = []
			dataSubtype.each {eachRunJob ->
				def dataStoreKey = [:]
				eachRunJob.each {eRunJob ->
					if(eRunJob.key == "data") {
						// process data job
						dataStoreKey['data'] = eachRunJob['data'][keySubtype]
					} else if(eRunJob.key == "KEYEXPR"){
						// process data keyexpr
						def mapKeyexpr = [:]
						// root of keyexpr
						if(eachRunJob['KEYEXPR'][keySubtype] != null) {
							mapKeyexpr['_root'] = eachRunJob['KEYEXPR'][keySubtype]
						}
						// chart of keyexpr
						if(eachRunJob.KEYEXPR._chart instanceof Map) {
							// chart is map data
							eachRunJob.KEYEXPR._chart.keySet().each {
								if(keySubtype == it) {
									mapKeyexpr['_chart'] = eachRunJob['KEYEXPR']['_chart'][it]
								}
							}
						} else {
							mapKeyexpr['_chart'] = eachRunJob.KEYEXPR._chart
						}
						// unit of keyexpr
						//Check if unit by key of subtype
						if (eachRunJob.KEYEXPR._unit != null){
							eachRunJob.KEYEXPR._unit.keySet().each {
								if(keySubtype == it) {
									mapKeyexpr['_unit'] = eachRunJob['KEYEXPR']['_unit'][it]
								}
							}
							if (mapKeyexpr['_unit'] == null) {
								mapKeyexpr['_unit'] = eachRunJob.KEYEXPR._unit
							}
						}

						// description of keyexpr
						mapKeyexpr['_description'] = eachRunJob.KEYEXPR._description

						dataStoreKey['KEYEXPR'] = mapKeyexpr
						mapKeyexpr = [:]
					} else {
						dataStoreKey[eRunJob.key] = eRunJob.value
					}
				}
				mapForKey[keySubtype].add(dataStoreKey)
				dataStoreKey = [:]
			}
		}
		logger.info("Finish getting subtyped data")
		return mapForKey
	}
}
