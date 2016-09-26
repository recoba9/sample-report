package totals

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.io.StdIn.readLine
import org.joda.time.format._
import org.joda.time.DateTime
import oracle.jdbc.pool.OracleDataSource
/**
  * Created by Rostislav on 24.09.2016.
  */
object Main {
  val pattern = "dd.MM.yyyy"
  val monthPattern = "MM.yyyy"
  val fmt = DateTimeFormat forPattern pattern
  val monthFmt = DateTimeFormat forPattern monthPattern
  val defaultQty = 200

  def parseDate(input: String) = try {
    Some(fmt parseDateTime input)
  } catch {
    case _: Throwable => None
  }

  def connect(args: Array[String]): Connection = {
    val ods = new OracleDataSource()
    ods.setUser(args(0))
    ods.setPassword(args(1))
    val host = args(2)
    val serviceName = args(3)
    val port = args(4)

    ods.setURL("jdbc:oracle:thin:@" + host +":" + port + "/" + serviceName)
    val con = ods.getConnection
    println("Connected\n")
    con
  }

  def close(con: Connection, st: PreparedStatement) = {
    st.close()
    con.close()
    println("\nDisconnected")

  }

  def totalProfit(args: Array[String]) = {
    System.out.println("Input reportStart date (format: " + pattern + ")")
    val reportStart = readLine()

    if (parseDate(reportStart) == None) {
      println("incorrect date format")
      System.exit(1)
    }

    System.out.println("Input reportEnd date (format: " + pattern + ")")
    val reportEnd = readLine()

    if (parseDate(reportEnd) == None) {
      println("incorrect date format")
      System.exit(1)
    }

    val con = connect(args)

    val st = con.prepareStatement("select nvl(sum(s.price * s.quantity *\n" +
      "                                         (\n" +
      "                                          select max(rate) -- in case there are many rates for one date for the product\n" +
      "                                          from   profit_percentage pp\n" +
      "                                          where  pp.productid = s.productid\n" +
      "                                                 and pp.\"date\" = (\n" +
      "                                                                    select max(pp2.\"date\")\n" +
      "                                                                    from   profit_percentage pp2\n" +
      "                                                                    where  pp2.\"date\" <= s.\"date\"\n" +
      "                                                                           and pp2.productid = s.productid\n" +
      "                                                                   )\n" +
      "                                         )\n" +
      "                                      ), 0) as total_profit\n" +
      "                            from   sales s\n" +
      "                            where  s.\"date\" >= to_date(?, ?)\n" +
      "                                   and s.\"date\" <= to_date(?, ?)")
    st.setString(1, reportStart)
    st.setString(2, pattern)
    st.setString(3, reportEnd)
    st.setString(4, pattern)

    val rs = st.executeQuery()

    while (rs.next()) {
      val total = rs.getString(1)
      println("total profit for " + reportStart + " - " + reportEnd + ": " + total)
    }
    close(con, st)
  }

  def monthlyProfit(args: Array[String]) = {
    System.out.println("Input the quantity of sold items (default - " + defaultQty + ")")
    val quantity = readLine()
    var quantityNum: Int = defaultQty

    if (quantity != "") {
      try {
        quantityNum = quantity.toInt
      }
      catch {
        case _: Throwable => {
          println("incorrect integer format")
          System.exit(1)
        }
      }
    }

    val con = connect(args)

    val st = con.prepareStatement("select trunc(s2.\"date\", 'month'),\n" +
      "                                   sum(s2.price * s2.quantity * \n" +
      "                                       (\n" +
      "                                        select max(rate) -- in case there are many rates for one date for the product\n" +
      "                                        from   profit_percentage pp\n" +
      "                                        where  pp.productid = s2.productid\n" +
      "                                               and pp.\"date\" = (\n" +
      "                                                                  select max(pp2.\"date\")\n" +
      "                                                                  from   profit_percentage pp2\n" +
      "                                                                  where  pp2.\"date\" <= s2.\"date\"\n" +
      "                                                                         and pp2.productid = s2.productid\n" +
      "                                                                 )\n" +
      "                                       )\n" +
      "                                      ) as monthly_profit\n" +
      "                            from   sales s2\n" +
      "                            where  (trunc(s2.\"date\", 'month'), s2.productid) in (\n" +
      "                                                                                   select trunc(s.\"date\", 'month'), s.productid\n" +
      "                                                                                   from   sales s\n" +
      "                                                                                   group by trunc(s.\"date\", 'month'), s.productid\n" +
      "                                                                                   having sum(s.quantity) = ?\n" +
      "                                                                                  )\n" +
      "                            group by trunc(s2.\"date\", 'month')\n" +
      "                            order by trunc(s2.\"date\", 'month')")
    st.setInt(1, quantityNum)
    st.setFetchSize(100)

    val rs = st.executeQuery()

    println("monthly profit for " + quantityNum + " sold items\n")
    println("month\t\tprofit")
    while (rs.next()) {
      val month = monthFmt.print(new DateTime(rs.getDate(1)))
      val total = rs.getString(2)
      println(month + "\t\t" + total)
    }
    close(con, st)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      println("Usage (oracle credentials): user password host serviceName port")
      System.exit(1)
    }
    System.out.println("Input report number \n1-total profit for the period\n2-monthly profit for the fixed sold quantity")
    val report = readLine()

    report match {
      case "1" => totalProfit(args)
      case "2" => monthlyProfit(args)
      case _   => {
                   println("incorrect input")
                   System.exit(1)
                  }
    }
  }
}
