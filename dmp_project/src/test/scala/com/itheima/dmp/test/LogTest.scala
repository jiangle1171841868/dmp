package com.itheima.dmp.test

import org.slf4j.{Logger, LoggerFactory}

object LogTest {

  def main(args: Array[String]): Unit = {

    //参数打印日志的主类的名字
    val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName.stripSuffix("$"))

    logger.info("----------------")
    logger.warn("----------------")
    logger.error("---------------")

  }

}
