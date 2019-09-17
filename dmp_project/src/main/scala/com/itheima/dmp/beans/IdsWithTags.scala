package com.itheima.dmp.beans

/**
 * 用户标签信息
 *
 * @param mainId  用户主ID
 * @param ids   用户ID的Map集合
 * @param tags   用户的标签集合
 */
case class IdsWithTags(
                        mainId: String, idsMap: Map[String, String], tagsMap: Map[String, Double]
                      )