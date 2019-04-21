package com.alex.clickhouse.hive2clickhouse

import java.sql.{Connection, PreparedStatement, SQLException}
import java.util.regex.Pattern

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import ru.yandex.clickhouse.settings.ClickHouseProperties
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnection, ClickHouseDataSource}

/**
  * @author alex
  *         2019-04-21
  *         hive表数据导入clickhouse
  */
object Hive2ClickHouseDemo1 {

  def main(args: Array[String]): Unit = {

    if (args.length < 7) {
      println("param is error")
      println("param is ： hql day  clickhouseurl clickhouseDB clickhouseTable user password")
      sys.exit(1)
    }


    /**
      * 参数
      **/

    val hql = args(0)
    val clickHouseDay = args(1)
    //val clickHouseClusterName = args(2)
    val clickHouseHostList = args(2)
    val clickHouseDB = args(3)
    val clickHouseTable = args(4)
//    val isRename = args(6)
//    val rename = isRename.toBoolean
    var user = args(5)
    var pwd = args(6)
    if (user == "" || user.isEmpty || user == "null" || user == "NULL") {
      user = null
    }
    if (pwd == "" || pwd.isEmpty || pwd == "null" || pwd == "NULL") {
      pwd = null
    }

    var columns = ""
    //匹配 select  from  where 这种模式并把字段取出来
    val matcher = Pattern.compile("select\\s(.+)from\\s(.+)where\\s(.*)").matcher(hql)
    if (matcher.find()) {
      columns = matcher.group(1).trim
    }

    val columSize = columns.split(",").length
    var values = ""
    for (i <- 0 until columSize) {
      values = values + "?,"
    }
    //用问号拼接并去掉最后一个逗号
    values = values.substring(0, values.length - 1)

    //判断ptindex的位置
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

    val clickHouseUrl = "jdbc:clickhouse://" + clickHouseHostList
    println(clickHouseUrl)


    //deleteClickHouseOldData(user, pwd, clickHouseHostList, clickHouseDay, clickHouseDB, clickHouseTable, rename)

    val conf = new SparkConf().setAppName("----------Hive2ClickHouseJob-----------").setMaster("local[*]" )
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val context = sparkSession.sparkContext
    import sparkSession.implicits._

    val sqlDF = sparkSession.sql(hql)
    val colum_size_broadcast = context.broadcast(Integer.valueOf(columSize))
    val databases_broadcast = context.broadcast(clickHouseDB)
    val table_broadcastnit = context.broadcast(clickHouseTable)
    val colums_broadcast = context.broadcast(columns)
    val values_broadcast = context.broadcast(values)
    val pt_index_broadcast = context.broadcast(Integer.valueOf(ptIndex))
    //val is_rename = context.broadcast(rename)

    sqlDF.rdd.foreachPartition { rows =>
      val clickHouseProperties = new ClickHouseProperties()
      val dataSource = new BalancedClickhouseDataSource(clickHouseUrl, clickHouseProperties)

      var sql = "INSERT INTO " + databases_broadcast.value + "." +
         table_broadcastnit.value + " (" + colums_broadcast.value + ") values (" + values_broadcast.value + ")"


      println("hive2clickhouse!sql is " + sql)


      var count = 0
      val batchSize = 50000
      var connection: Connection = null
      var ps: PreparedStatement = null
      val line = new StringBuilder("line is : ")

      try {

        connection = dataSource.getConnection(user, pwd)
        ps = connection.prepareStatement(sql)

        while (rows.hasNext) {
          try {

            val row = rows.next()

            for (i <- 0 until colum_size_broadcast.value.intValue()) {

                ps.setObject(i + 1, row(i))
                line.append(row(i))
                line.append(",")

            }

            ps.addBatch()
            count = count + 1
            if (count % batchSize == 0) {
              ps.executeBatch()
              line.clear()
              line.append("line is : ")
              if (ps != null) {
                try {
                  ps.close()
                } catch {
                  case e: SQLException =>
                    println("close clickhouse prepare statement error!", e.getMessage)
                }
              }

              if (connection != null) {
                try {
                  connection.close()
                } catch {
                  case e: SQLException =>
                    println("close clickhouse connection error!", e.getMessage)
                }
              }

              connection = dataSource.getConnection(user, pwd)
              ps = connection.prepareStatement(sql)

            }
          } catch {
            case e: Exception =>
              println("insert clickhouse error!" + line.toString(), e)
              throw new RuntimeException("insert clickhouse error!" + line.toString(), e)
          }
        }

        ps.executeBatch()
        println("hive etl finished!day is " + clickHouseDay + " count:" + count)

      } catch {
        case e: Exception =>
          println("insert clickhouse error!" + line.toString(), e)
          throw new RuntimeException("insert clickhouse error!" + line.toString, e)
      } finally {
        if (ps != null) {
          try {
            ps.close()
          } catch {
            case e: SQLException =>
              println("close clickhouse prepare statement error!", e.getMessage)
          }
        }

        if (connection != null) {
          try {

          } catch {
            case e: SQLException =>
              println("close clickhouse connection error!", e.getMessage)
          }
        }
      }
    }


    context.stop()

  }

  /**
    *
    * @param user
    * @param password
    * @param clickHouseUrl
    * @param clickHouseDB
    * @param oldName
    * @param newName
    * @param clcikhouseClusterName
    */
  def reNameTable(user: String, password: String, clickHouseUrl: String, clickHouseDB: String, oldName: String, newName: String, clcikhouseClusterName: String) = {
    val clickHouseProperties = new ClickHouseProperties()
    val dataSource = new BalancedClickhouseDataSource(clickHouseUrl, clickHouseProperties)
    val sb = new StringBuilder("RENAME TABLE ")
    sb.append(clickHouseDB).append(".").append(oldName).append(" TO ").append(clickHouseDB).append(".").append(newName).append(" ON CLUSTER " + clcikhouseClusterName)
    var connection: ClickHouseConnection = null
    try {
      connection = dataSource.getConnection(user, password)
      val preparedStatement = connection.prepareStatement(sb.toString())
      preparedStatement.execute()
      println("rename table success!sql is " + sb.toString())

    } catch {
      case e: SQLException =>
        println("rename table faild!sql is " + sb.toString(), e)
    }

  }


  /**
    * 删除旧数据
    *
    * @param user
    * @param password
    * @param clickHouseHostList
    * @param day
    * @param databases
    * @param table
    * @param isRename
    */
  def deleteClickHouseOldData(user: String, password: String, clickHouseHostList: String, day: String, databases: String, table: String, isRename: Boolean) = {
    val delDays = List(day)

    val hostList = clickHouseHostList.split(",")
    for (j <- 0 until hostList.length) {
      val host = hostList(j)
      val dataSource = new ClickHouseDataSource("jdbc:clickhouse://" + host + "/" + databases)
      var connection: Connection = null

      try {
        for (i <- 0 until 10) {
          try {
            connection = dataSource.getConnection(user, password)
          } catch {
            case e: Exception =>
              println("connection error!host url is " + dataSource.getUrl(), e)
              Thread.sleep(2000L)
          }
        }

        if (isRename) {
          val deleteDataSql = "alter table " + databases + "." + table + "_bk delete where 1=1"
          for (i <- 0 until 10) {
            try {
              connection.createStatement().executeUpdate(deleteDataSql)
            } catch {
              case e: Exception =>
                println("drop data error!retry is" + i, e)
                Thread.sleep(2000L)
            }
          }
          println("drop data success!sql is " + deleteDataSql)
        } else {
          println("begin delete partition!host url is " + dataSource.getUrl())

          for (dt <- delDays) {
            val detachPartitionSql = "alter table " + databases + "." + table + " detach partition " + dt
            for (i <- 0 until 10) {
              try {
                connection.createStatement().executeUpdate(detachPartitionSql)
              } catch {
                case e: Exception =>
                  println("drop partition error!retry is" + i, e)
                  Thread.sleep(2000L)
              }
            }
            println("drop partition success!sql is " + detachPartitionSql)
          }
        }

        if (connection != null) {
          try {

          } catch {
            case e: SQLException =>
              println("close clickhouse connection error!", e.getMessage())
          }
        }
      } catch {
        case e: Exception =>
          println("drop partition error!", e)
      } finally {
        if (connection != null) {
          try
            connection.close()
          catch {
            case e: SQLException =>
              println("close clickhouse connection error!", e.getMessage)
          }
        }
      }
    }
  }

}
