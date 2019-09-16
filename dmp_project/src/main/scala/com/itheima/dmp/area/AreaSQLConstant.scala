package com.itheima.dmp.area

object AreaSQLConstant {

  def areaSQL(odsViewName: String,areaViewName: String): String = {

       s"""
          |WITH temp AS (
          |	SELECT
          |		o.geoHash,
          |		round(avg(o.longitude), 6) AS avg_longitude,
          |		round(avg(o.latitude), 6) AS avg_latitude
          |	FROM
          |		$areaViewName AS d
          |	LEFT JOIN $odsViewName AS o
          | ON
          |     longitude >= 73.67
          |	AND longitude <= 135.06
          |	AND latitude >= 3.87
          |	AND latitude <= 53.56
          |	AND d.geo_hash = o.geoHash
          |	AND d.area IS NOT NULL
          |	GROUP BY
          |		o.geoHash
          |)
          |
          |SELECT
          |	geoHash AS geo_hash,
          | get_area(avg_longitude,avg_latitude) as area
          |FROM
          |	temp
        """.stripMargin


  }

}
