package com.itheima.dmp.beans

//封装ip解析后的省市、经纬度信息
case class IPRegion(ip: String, longitude: Double, latitude: Double,
                    province: String, city: String, geoHash: String)