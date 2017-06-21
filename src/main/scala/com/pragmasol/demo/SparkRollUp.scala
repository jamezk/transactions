package com.pragmasol.demo

import java.util.{Date, UUID}

import com.datastax.spark.connector._
import com.pragmasol.demo.util.ArgHelper
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by jamezk on 09/06/2017.
  */
object SparkRollUp extends App with ArgHelper {


  //Create Spark config with sensible defaults
  val conf = new SparkConf()
    .setMaster(getArgOrDefault("spark.master", "local[2]"))
    .setAppName("spark-cass-csv-importer")
    .set("spark.executor.memory", getArgOrDefault("executor.memory", "512m"))
    .set("spark.default.parallelism", getArgOrDefault("processing.cores", "2"))
    .set("spark.cassandra.connection.host", getArgOrDefault("cassandra.host", "192.168.56.101"))

  val dtFrom = getBoundaryDateTime(2016,1)
  val dtTo = getBoundaryDateTime(2016,2)

  val sc = new SparkContext(conf)

  val txns = sc.cassandraTable("txns_demo","transactions")

  val txns_rdd = txns.select("acct_id","transaction_date","transaction_ts","transaction_id","transaction_type","transaction_amount","ttl_date").where("transaction_date >= ? and transaction_date < ?",dtFrom,dtTo)

  val txns_by_acct = txns_rdd.spanBy(r => r.getString("acct_id"))

  val txns_before_balance = txns_by_acct.map { t =>
    val (acct_id, txns) = t
    (acct_id, txns.takeWhile(r => r.getString("transaction_type") != "BALANCE_CALC"))
  }
  val txns_balance_record = txns_by_acct.map { t =>
    val (acct_id, txns) = t
    (acct_id, txns.dropWhile(r => r.getString("transaction_type") != "BALANCE_CALC").headOption)
  }

  val summed_txns = txns_before_balance.map { r =>

    val (acct_id, txns) = r

    val running_txns = txns.filter{ t =>
      t.getString("transaction_type") match {
        case "CC" |
             "DD" |
             "CHQ_DEPOSIT_PENDING" |
             "CHQ_DEPOSIT_CLEARED" =>
          true
        case _ =>
          false
      }
    }

    val pending_txns = txns.filter { t =>
      t.getString("transaction_type") match {
        case "CHQ_DEPOSIT_PENDING" |
             "PENDING_AUTH" =>
          true
        case _ =>
          false
      }
    }

    (acct_id,
      running_txns.map { t => t.getDecimal("transaction_amount") }.reduce((a,b) => a+b),
      pending_txns.map { r => Pending(r.getUUID("transaction_id"),r.getDecimal("transaction_amount"),r.getDate("ttl_date"))}.toSet
    )

  }.map { r =>

    val (acct_id, running_balance_amt, available_balances) = r
    val tot_available_balance = available_balances.reduce{
      (a,b)=> Pending(UUID.randomUUID(),a.amount+b.amount,new Date())
    }

    (acct_id, dtTo.toDate, dtTo.toDate, UUIDAcctAndDate(acct_id,dtTo,"BAL"), "BalanceCalculator Calc Record", running_balance_amt, available_balances, "BAL")

  }

  summed_txns.saveToCassandra("txns_demo","transactions",SomeColumns("acct_id","transaction_date","transaction_ts","transaction_id","description","transaction_amount","balance_pending","transaction_type"))


  private def getBoundaryDateTime(year: Int, month: Int) : DateTime = {
    new DateTime().withYear(year)
                  .withMonthOfYear(month)
                  .withDayOfMonth(1)
                  .withHourOfDay(0)
                  .withMinuteOfHour(0)
                  .withSecondOfMinute(0)
                  .withMillisOfSecond(0)
                  .withZone(DateTimeZone.forID("Europe/London"))
  }
  /*
    Create a UUID based on a bunch of unique values for the acct. For a given input the UUID will be
    the same every time
   */
  private def UUIDAcctAndDate(acct_id: String, dateTime: DateTime, misc : String) : UUID = {
    val stringToBuild = acct_id+dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ")+misc
    UUID.nameUUIDFromBytes(stringToBuild.getBytes)
  }


}
