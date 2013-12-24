package org.wiperdog.MenuGenerator

import org.wiperdog.WiperdogLogger.WiperdogLogger4Xwiki;

class MenuGeneratorLib {
	def static WiperdogLogger4Xwiki logger = new WiperdogLogger4Xwiki("MenuGeneratorLib")
	
	/**
	 * Recursively function, used for gen tree menu data 
	 * Input treeItem: root tree map of menu (not leaf)
	 *       mapCollection: Map of collections, Item of map has key is a job group and value is list job which is applied for that group
	 *       parentList: used for recursively to canculate key if data if leaf
	 * Output: If data is leaf, create key of group data, get list of job which is applied for group from mapCollection and create menu Item
	 *         If data isn't leaf, create node and call recursively function with sub data
	 **/
	def getMenuItemsStr(treeItem, mapCollection, parentList = []) {
		logger.info("Begin getting meunu item string")
		def ul_open = false
		def result = ""
		def parentStr = ""
		def parentLstforChild = []

		//If data isn't leaf, create node and call recursively function with sub data
		if (treeItem instanceof Map) {
			result += "<ul id='treemenu2' class='treeview'>"
			treeItem.each{itemKey, itemVal ->
				parentList.each{parentListItem->
					parentLstforChild.add(parentListItem)
				}
				parentLstforChild.add(itemKey)
				result += "<li>"+ itemKey
				result += getMenuItemsStr(itemVal, mapCollection, parentLstforChild)
				result +="</li>"
				parentLstforChild = []
			}
			result += "</ul>"
		}

		//If data is leaf, create key of group data, get list of job which is applied for group from mapCollection and create menu Item
		if (treeItem instanceof List) {
			result += "<ul>"
			parentList.each{parentItem ->
				if (parentStr != ""){
					parentStr += "."
				}
				parentStr += parentItem
			}
			if (mapCollection[parentStr] != null) {
				mapCollection[parentStr].each {item->
					result += "<li><a>" + item +"</a></li>"
				}
			}
			result += "</ul>"
		}
		logger.info("Finish getting meunu item string")
		return result
	}

	/**
	 * Create list job group from declared treeMap
	 * Input treeItem: root tree map of menu (not leaf)
	 *       parentList: used for recursively to canculate parent key if data is the last node before leaf
	 * Output: if data is the last node before leaf, add as item of final list
	 **/
	def getListJobGroup(groupItems, parentList = []) {
		logger.info("Begin getting list job group")
		def returnList = []
		def parentLstforChild = []
		def parentStr = ""
		def chilData

		if (groupItems instanceof Map) {
			groupItems.each{itemKey, itemVal ->
				parentList.each{parentListItem->
					parentLstforChild.add(parentListItem)
				}
				parentLstforChild.add(itemKey)
				chilData = getListJobGroup(itemVal, parentLstforChild)
				chilData.each{ returnList.add(it) }
				parentLstforChild = []
			}
		}

		if (groupItems instanceof List) {
			parentList.each{parentItem ->
				if (parentStr != ""){
					parentStr += "."
				}
				parentStr += parentItem
			}
			returnList.add(parentStr)
		}
		logger.info("Finish getting list job group")
		return returnList
	}
}
