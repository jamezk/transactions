package com.pragmasol.demo;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.joda.time.DateTime;

/**
 * Created by jamezk on 19/06/2017.
 */
public class BalanceRetriever {

    private final Session session;
    private PreparedStatement stmt = null;

    private String stmt_cql = "select * from transactions where " +
                                "acct_id = ? :acct_id and " +
                                "transaction_date >= :ts_date_from and transaction_date <= :ts_date_to";

    public BalanceRetriever(Session session) {
        this.session = session;
    }


    public BalanceCalculator getCurrentBalance(DateTime date, String accountNo, String sortCode) {

        if(stmt == null) {
            stmt = session.prepare(stmt_cql);
        }

        DateTime dateFrom = date.minusMonths(2);

        BoundStatement bound = stmt.bind().setString("acct_id",accountNo+"|"+sortCode)
                    .setString("ts_date_from",dateFrom.toString("dd/MM/yyyy"))
                    .setString("ts_date_to",date.toString("dd/MM/yyyy"));

        ResultSet rs = session.execute(bound);

        rs.isExhausted();



        return null;









    }







    public static void main(String...args) {









    }




}
