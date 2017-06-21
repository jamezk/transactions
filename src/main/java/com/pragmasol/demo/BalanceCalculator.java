package com.pragmasol.demo;

import java.math.BigDecimal;

/**
 * Created by jamezk on 19/06/2017.
 */
public class BalanceCalculator {

    private BigDecimal running = BigDecimal.ZERO;
    private BigDecimal available = BigDecimal.ZERO;

    public BalanceCalculator() {

    }

    public void addToCalculation(TransactionType type, BigDecimal amount, BigDecimal avail) {
        switch(type) {
            case CC:
            case DD:
                running = running.add(amount);
                available = available.add(amount);
                break;
            case CHQ_DEPOSIT_PENDING:
                running = running.add(amount);
                available = available.subtract(amount);
                break;
            case PENDING_AUTH:
            case CHQ_DEPOSIT_CLEARED:
                available = available.add(amount);
                break;
            case BALANCE_CALC:
                running = running.add(amount);
                available = available.add(avail);
                break;
            default:
                //DO NOTHING
        }
    }

}
