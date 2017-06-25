
CREATE KEYSPACE txns_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;


CREATE TYPE txns_demo.pending (
    transaction_id uuid,
    amount decimal,
    valid_until date
);


CREATE TABLE txns_demo.transactions (
    acct_id text,
    transaction_ts timestamp,
    transaction_id uuid,
    available_amount decimal,
    balance_pending set<frozen<pending>>,
    cc_auth_code text,
    cc_merchant_category text,
    cc_merchant_name text,
    cc_pan_suffix text,
    cc_pin_entry_mode text,
    cc_txn_type text,
    cheque_number text,
    currency_code text,
    description text,
    transaction_amount decimal,
    transaction_type text,
    ttl_date date,
    PRIMARY KEY (acct_id, transaction_ts, transaction_id)
) WITH CLUSTERING ORDER BY (transaction_date DESC, transaction_ts DESC, transaction_id ASC);


CREATE TABLE txns_demo.transaction_history (
    acct_id text,
    transaction_date date,
    transaction_ts timestamp,
    transaction_id uuid,
    available_amount decimal,
    cc_auth_code text,
    cc_merchant_category text,
    cc_merchant_name text,
    cc_pan_suffix text,
    cc_pin_entry_mode text,
    cc_txn_type text,
    cheque_number text,
    currency_code text,
    description text,
    transaction_amount decimal,
    transaction_type text,
    ttl_date date,
    PRIMARY KEY ((acct_id, transaction_date), transaction_ts, transaction_id)
) WITH CLUSTERING ORDER BY (transaction_date DESC, transaction_ts DESC, transaction_id ASC);





CREATE OR REPLACE FUNCTION txns_demo.balance_inc(previous tuple<decimal, decimal>, amt decimal, txn_type text, ttl_date date);
    CALLED ON NULL INPUT
    RETURNS tuple<decimal, decimal>
    LANGUAGE java
    AS 'if (txn_type.equals("PENDING_AUTH") && System.currentTimeMillis() < ttl_date.getMillisSinceEpoch()) {
            previous.setDecimal(1, previous.getDecimal(1).add(amt));
        } else if (txn_type.equals("DD") || txn_type.equals("CC") || txn_type.equals("CHQ_DEPOSIT_CLEARED")) {
            previous.setDecimal(0, previous.getDecimal(0).add(amt));
        }
        return previous;';


CREATE OR REPLACE FUNCTION balance_final ( state tuple<decimal,decimal> ) CALLED ON NULL INPUT RETURNS tuple<decimal,decimal> LANGUAGE java AS
  'state.setDecimal(1,state.getDecimal(0).add(state.getDecimal(1)));
   return state;';


CREATE OR REPLACE AGGREGATE balance (decimal, text, date)
SFUNC balance_inc STYPE tuple<decimal,decimal> FINALFUNC balance_final INITCOND (0,0);



