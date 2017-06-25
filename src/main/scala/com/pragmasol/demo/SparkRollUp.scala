package com.pragmasol.demo

import java.util.UUID

import com.datastax.spark.connector._
import com.pragmasol.demo.util.ArgHelper
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.immutable.Set

/**
 * Created by jamezk on 09/06/2017.
 */
object SparkRollUp extends App with ArgHelper {


  //Create Spark config with sensible defaults
  val conf = new SparkConf()
    .setMaster(getArgOrDefault("spark.master", "local[2]"))
    .setAppName("spark-cass-balance-calculator")
    .set("spark.executor.memory", getArgOrDefault("executor.memory", "512m"))
    .set("spark.default.parallelism", getArgOrDefault("processing.cores", "2"))
    .set("spark.cassandra.connection.host", getArgOrDefault("cassandra.host", "192.168.56.101"))
  //Date now
  val dtTo = DateTime.now(DateTimeZone.forID("Europe/London")).plusDays(2)
  //Date two months previous, assumption here is that a balance calc has already been performed in that period
  //or the start of the account history is within that time
  val dtFrom = dtTo.minusMonths(2)

  val sc = new SparkContext(conf)

  val txns_rdd = sc.cassandraTable[Transaction]("txns_demo","transactions")
                    .select("acct_id","transaction_ts",
                            "transaction_id","transaction_type",
                            "transaction_amount","pending_txns","ttl_date")
                    .where("transaction_ts >= ? and transaction_ts < ?",dtFrom,dtTo)
  /*
   * Now use spanBy(..) on account id so our partitions are specific to individual token ranges - efficient when running
   * multiple executors on different C* nodes as subsequent filtering will occur locally where the data lives,
   * without shuffling
   */
  val txns_by_acct = txns_rdd.spanBy(t => (t.acct_id))
  /*
   * Filter the transactions so we only grab those prior to the most recent balance calculation
   */
  val txns_before_balance = txns_by_acct.map { t =>
    val (acct_id, txns) = t
    (acct_id, txns.takeWhile( _.transaction_type != "BALANCE_CALC"))
  }
  /*
   * Next obtain the last balance calculation if it exists and map it so it can be joined later
   */
  val previous_balance = txns_by_acct.map { t =>
    val (acct_id, txns) = t
    val txn = txns.dropWhile(_.transaction_type != "BALANCE_CALC").headOption
    txn match {
      case Some(t) =>
        (acct_id,(t.transaction_amount,t.pending_txns.filter(_.valid_until.after(dtTo.toDate))))
      case None =>
        (acct_id,(BigDecimal.valueOf(0l),Set.empty[Pending]))
    }
  }

  /*
   * Separate out the transaction types as we have ones that will be part of the running balance
   * and ones that are pending and _potentially_ may be part of the running balance
   */
  val summed_txns = txns_before_balance.map { r =>

    val (acct_id, txns) = r
    //Verified Txns
    val running_txns = txns.filter{ t =>
      t.transaction_type match {
        case "CC" |
             "DD" |
             "CHQ_DEPOSIT_CLEARED" =>
          true
        case _ =>
          false
      }
    }
    //Pending Auth Txns - Filter out any whose ttl_date has already passed as they're no longer valid
    val pending_txns = txns.filter { t =>( t.transaction_type == "PENDING_AUTH") && t.ttl_date.get.after(dtTo.toDate) }

    //Sum the total transaction amount and create a set of any pending transactions
    (acct_id,
      running_txns.map { t => t.transaction_amount }.reduceOption((a,b) => a+b).getOrElse(BigDecimal.valueOf(0l)),
      pending_txns.map { p => Pending(p.transaction_id,p.transaction_amount,p.ttl_date.get)}.toSet
    )

  }.map { r =>
    //Create a balance record
    val (acct_id, running_balance_amt, pending_txns) = r
    //Format the output so it aligns with the columns we're going to save
    (acct_id, (dtTo.toDate, UUIDAcctAndDate(acct_id,dtTo,"BALANCE_CALC"),
      "BalanceCalculator Calc Record", running_balance_amt, pending_txns, "BALANCE_CALC"))

  }
  /*
   * Join the summed transactions with the previous balance record and combine them then save
   * back to Cassandra
   */
  summed_txns.join(previous_balance).map { x =>
    val (acct_id, (summed, previous)) = x
    (acct_id,summed._1,summed._2,summed._3,summed._4+previous._1,previous._2++summed._5,summed._6)
  }.saveToCassandra("txns_demo","transactions",
    SomeColumns("acct_id","transaction_ts","transaction_id","description","transaction_amount","pending_txns","transaction_type"))



  /**
   * Create a UUID based on unique values for the acct. When called more than once for a given input the UUID will be
   * the same every time
   */
  private def UUIDAcctAndDate(acct_id: String, dateTime: DateTime, misc : String) : UUID = {
    val stringToBuild = acct_id+dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ")+misc
    UUID.nameUUIDFromBytes(stringToBuild.getBytes)
  }


}
