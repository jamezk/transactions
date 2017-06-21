package com.pragmasol.demo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

/**
 * Created by jamezk on 19/06/2017.
 */
public class CassandraSession {

    private static Session session;
    private static boolean initialized;

    public static void init(String...hosts) {
        if(session == null) {
            Cluster cluster = Cluster.builder().addContactPoints(hosts)
                    .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("Test Cluster").build()))
                    .build();
            session = cluster.connect("txn_demo");
            initialized = true;
        }
    }

    public static Session getSession() {
        if(!initialized) {
            throw new IllegalStateException("Cluster not yet initialized");
        }
        return session;
    }

}
