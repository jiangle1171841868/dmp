package com.itheima.dmp.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间工具类
  */
object DateUtils {

	/**
	  * 获取当前的日期，格式为:20190710
	  */
	def getTodayDate(): String = {
		// a. 获取当前日期
		val nowDate = new Date()
		// b. 转换日期格式
		FastDateFormat.getInstance("yyyyMMdd").format(nowDate)
	}


}
