package com.pragmasol

import java.util.{Date, UUID}

import com.datastax.spark.connector.types._
import scala.reflect.runtime.universe._


/**
  * Created by jamezk on 20/06/2017.
  */
package object demo {

  case class Pending(transaction_id : UUID, amount : BigDecimal, valid_until : Date)

  case class Transaction(acct_id : String,
                         transaction_ts : Date,
                         transaction_id : UUID,
                         transaction_type : String,
                         transaction_amount : BigDecimal,
                         pending_txns : Set[Pending],
                         ttl_date : Option[Date])

}
