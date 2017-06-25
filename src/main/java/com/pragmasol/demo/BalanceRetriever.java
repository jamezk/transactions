package com.pragmasol.demo;

import com.datastax.driver.core.*;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Set;

import static com.pragmasol.demo.TransactionType.*;

/**
 * Created by jamezk on 10/06/2017.
 */
public class BalanceRetriever {

    private final Session session;
    private PreparedStatement stmt = null;

    private String stmt_cql = "select * from transactions where " +
                                "acct_id = :acct_id and " +
                                "transaction_ts >= :ts_date_from and transaction_ts < :ts_date_to;";


    public BalanceRetriever(Session session) {
        this.session = session;
    }


    public BalanceCalculator getCurrentBalance(DateTime date, String accountNo, String sortCode) {

        if(stmt == null) {
            stmt = session.prepare(stmt_cql);
        }

        DateTime dateFrom = date.minusMonths(2);
        //Create a statement
        BoundStatement bound = stmt.bind().setString("acct_id",accountNo+"|"+sortCode)
                    .setTimestamp("ts_date_from",dateFrom.toDate())
                    .setTimestamp("ts_date_to",date.toDate());


        ResultSet rs = session.execute(bound);
        BalanceCalculator calculator = new BalanceCalculator(date);

        while(!rs.isExhausted()) {
            Row row = rs.one();

            TransactionType txnType = TransactionType.valueOf(row.getString("transaction_type"));
            BigDecimal txnAmount = row.getDecimal("transaction_amount");
            LocalDate ttlDate = row.getDate("ttl_date");
            Set<PendingTxn> pendingTxns = null;
            if(txnType.equals(BALANCE_CALC)) {
                pendingTxns = row.getSet("pending_txns", PendingTxn.class);
                calculator.addToCalculation(txnType,txnAmount,pendingTxns);
                return calculator;
            } else {
                calculator.addToCalculation(txnType,txnAmount,ttlDate);
            }

        }

        return calculator;
    }

    public static void main(String...args) {

        CassandraSession.init("192.168.56.101");
        BalanceRetriever retriever = new BalanceRetriever(CassandraSession.getSession());

        BalanceCalculator balance = retriever.getCurrentBalance(DateTime.now().plusDays(2),"38976122","911241");
        System.out.println(balance.toString());

        CassandraSession.close();

    }




}
