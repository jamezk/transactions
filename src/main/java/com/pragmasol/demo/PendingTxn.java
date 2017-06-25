package com.pragmasol.demo;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * UDT Class for Pending Transactions
 */
@UDT(name = "Pending")
public class PendingTxn {
    @Field
    public UUID transaction_id;
    @Field
    public BigDecimal amount;
    @Field
    public LocalDate valid_until;

}
