package com.alex.clickhouse.hive2clickhouse.test

import java.util.regex.Pattern

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import ru.yandex.clickhouse.BalancedClickhouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties

object Demo1 {
  def main(args: Array[String]): Unit = {
    println("test!scala")

    var columns = ""
    val hql="select id,user,home from db.test where id=1"

    val matcher = Pattern.compile("select\\s(.+)from\\s(.+)where\\s(.*)").matcher(hql)
    if (matcher.find()) {
      columns = matcher.group(1).trim
    }

    println(columns)
    println(matcher)
    println(matcher.find())

    val columSize = columns.split(",").length
    var values = ""
    for (i <- 0 until columSize) {
      values = values + "?,"
    }
    println(values)
    values = values.substring(0, values.length - 1)
    println(values)

    var ptIndex = 0
    var flag = true
    val columnsArray = columns.split(",")
    for (i <- 0 until columnsArray.length if flag) {
      if ("pt".equals(columnsArray(i))) {
        ptIndex = i
        flag = false
      }
    }
    println("ptIndex is " + ptIndex)

    val clickHouseProperties = new ClickHouseProperties()
    val dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://192.168.1.106:8123", clickHouseProperties)

    val conf = new SparkConf().setAppName("----------Hive2ClickHouseJob-----------").setMaster("local[*]" )
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val context = spark.sparkContext

    val sqlDF = spark.sql("select move,type,day  from test.hive2clickhouse_test1 where day='2019-01-01'")



  }


}
