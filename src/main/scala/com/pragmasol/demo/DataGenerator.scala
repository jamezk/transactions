package com.pragmasol.demo

import java.io.{File, PrintWriter, Writer}
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.joda.time.DateTime
import com.github.javafaker.Faker
import collection.JavaConverters._

/**
  * Created by jamezk on 21/06/2017.
  */
object DataGenerator extends App {

  val faker = new Faker()

  val writer = new PrintWriter(new File("import.csv"))
  writeHeader(writer)
  def writeLine2 = writeLine(writer) _
    //val acct_id = faker.bothify("########|######")
    val acct_id = "38976193|911241"
    (1 to 100).foreach { x =>
      val txn_date = new DateTime(faker.date().past(60, TimeUnit.DAYS))
      val txn_id = UUID.randomUUID()
      val idx = faker.random().nextInt(100)

      val txn_type = idx match {
        case x if x > 95 =>
          faker.options().option("PENDING_AUTH", "CHQ_DEPOSIT_PENDING")
        case _ =>
          faker.options().option("CC", "DD", "CHQ_DEPOSIT_CLEARED")
      }

      val txn_amount = txn_type match {
        case "CC" | "DD" | "PENDING_AUTH" => -getAmount()
        case _ => getAmount()
      }

      val ttl_date = txn_type match {
        case "PENDING_AUTH" | "CHQ_DEPOSIT_PENDING" =>
          txn_date.plusDays(3).toString("yyyy-MM-dd")
        case _ =>
          ""
      }

      val description = faker.lorem().words(5).asScala.mkString(" ")
      writeLine2(
        Seq(acct_id, txn_date.toString("yyyy-MM-dd HH:mm:ss.SSSZ"),
          txn_id, txn_amount,
          txn_type, description, ttl_date))
    }

  writer.flush

  private def getAmount() : Double = { faker.number().randomDouble(2, 1, 500) }

  private def writeLine(writer : PrintWriter)(things : Any*): Unit = {
    writer.println(things.mkString("",",",""))
  }

  private def writeHeader(writer : PrintWriter) = {
    writer.println("acct_id,transaction_time,transaction_id,transaction_amount,transaction_type,description,ttl_date")
  }


}
