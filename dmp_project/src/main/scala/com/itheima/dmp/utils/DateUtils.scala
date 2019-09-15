package com.itheima.dmp.utils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间工具类
  */
object DateUtils {

  /**
    * 获取当前的日期，格式为: 20190910
    */
  def getTodayDate(): String = {
    // a. 获取当前日期
    val nowDate = new Date()
    // b. 转换日期格式
    FastDateFormat.getInstance("yyyyMMdd").format(nowDate)
  }

  /**
    * 获取昨天时间,格式为: 20190915
    */
  def getYesterdayDate(): String = {

    // 1. 获取Calendar对象
    val calendar: Calendar = Calendar.getInstance()

    // 2. 获取昨天时间
    calendar.add(Calendar.DATE, -1)

    // 3. 转换日期格式
    FastDateFormat.getInstance("yyyyMMdd").format(calendar)

  }

}
