# Storing and Retrieving Financial Transactions in Cassandra

This project provides an example of how to store transactions in Cassandra
and calculate a balance roll-up.

## Creating the schema
There is a `ddl.cql` file which you can use to create the keyspace, associated tables, types and user defined aggregates 
and functions. Copy this file to one of your Cassandra nodes and run:

    cqlsh -f ddl.cql
    
*Note:* The keyspace is defined using SimpleStrategy as it's designed to run on a single node. 
Change the definition appropriately to increase the replication factor and/or strategy (if you're running with a multi-dc)
cluster

## Importing sample data

A sample CSV file is included under resources which you can use. Copy this to a Cassandra node and run cqlsh and use the following `COPY` command:

    COPY txns_demo.transactions (acct_id,transaction_ts,transaction_id,transaction_amount,transaction_type,description,ttl_date) FROM 'sample-import.csv' WITH HEADER = true ;

## Building
The project is built using sbt. In order to compile locally run sbt:

    sbt
    
Then

    compile
    
## Running

### Spark Roll Up
To run the Spark roll up, from the sbt prompt type:

    runMain com.pragmasol.demo.SparkRollUp "cassandra.host=192.168.56.101"
    
Where you can pass the parameter specifying the cassandra host you're trying to connect to.
Note, you must surround this parameter with quotes (as shown) or the parameters will not be passed correctly and an error will occur

Also be aware that this spark job runs locally. If you want to run with distributed workers/executors then you'll
need to deploy spark appropriately and change the configuration to take advantage.


### Balance Retrieval
To run the balance retrieval:

    runMain com.pragmasol.demo.BalanceRetriever 38976122 911241
    
Supplying the account number and sortcode as parameters

