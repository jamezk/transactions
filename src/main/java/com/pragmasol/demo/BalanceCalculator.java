package com.pragmasol.demo;

import com.datastax.driver.core.LocalDate;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Set;

/**
 * Created by jamezk on 19/06/2017.
 */
public class BalanceCalculator {

    private BigDecimal running = BigDecimal.ZERO;
    private BigDecimal available = BigDecimal.ZERO;
    private DateTime asAt;

    public BalanceCalculator(DateTime balanceDate) {
        asAt = balanceDate;
    }

    public void addToCalculation(TransactionType type, BigDecimal amount, LocalDate ttlDate) {
        addToCalculation(type,amount,ttlDate,null);
    }

    public void addToCalculation(TransactionType type, BigDecimal amount, Set<PendingTxn> pendingTxns) {
        addToCalculation(type,amount,null,pendingTxns);
    }

    public void addToCalculation(TransactionType type, BigDecimal amount, LocalDate ttlDate, Set<PendingTxn> pendingTxns) {
        switch(type) {
            case CC:
            case DD:
            case CHQ_DEPOSIT_CLEARED:
                addToRunningAndAvailable(amount);
                break;
            case PENDING_AUTH:
                addPendingIfRequired(amount,ttlDate);
                break;
            case BALANCE_CALC:
                pendingTxns.forEach(t -> {
                        if(t.valid_until.getMillisSinceEpoch() > asAt.getMillis()) {
                                addPendingIfRequired(t.amount, t.valid_until);
                        }
                });
                addToRunningAndAvailable(amount);
                break;
            case CHQ_DEPOSIT_PENDING:
                //Do nothing here because we don't want to count cheques until they have cleared anyway
                break;
            default:
                //DO NOTHING
        }
    }

    private void addPendingIfRequired(BigDecimal amount, LocalDate ttlDate) {
        if(ttlDate.getMillisSinceEpoch() > asAt.getMillis()) {
            available = available.add(amount);
        }
    }

    private void addToRunningAndAvailable(BigDecimal amount) {
        running = running.add(amount);
        available = available.add(amount);
    }

    public BigDecimal getRunning() {
        return running;
    }

    public BigDecimal getAvailable() {
        return available;
    }

    public DateTime balanceDate() {
        return asAt;
    }

    @Override
    public String toString() {
        return "Balance at: " + asAt.toString("dd-MM-yyyy H:m:s") + "\n" +
                "Running: " + running.toPlainString() + "\n" +
                "Available: " + available.toPlainString() + "\n";
    }


}
