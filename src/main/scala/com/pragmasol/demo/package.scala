package com.pragmasol

import java.util.{Date, UUID}


/**
  * Created by jamezk on 20/06/2017.
  */
package object demo {

  case class Pending(transaction_id : UUID, amount : BigDecimal, valid_until : Date)


}
