package com.pragmasol.demo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.mapping.MappingManager;

/**
 * Created by jamezk on 19/06/2017.
 */
public class CassandraSession {

    private static Cluster cluster;
    private static Session session;
    private static boolean initialized;

    public synchronized static void init(String...hosts) {
        if(session == null && initialized == false) {
            cluster = Cluster.builder().addContactPoints(hosts)
                                                .withClusterName("Test Cluster")
                                                .withLoadBalancingPolicy(
                                                        new TokenAwarePolicy(
                                                                DCAwareRoundRobinPolicy.builder().withLocalDc("datacenter1").build())).build();
            session = cluster.connect("txns_demo");
            //Add a mapping for the PendingTxn class into the Codec registry so we can deserialize
            MappingManager mappingManager = new MappingManager(session);
            TypeCodec<PendingTxn> pendingTxnTypeCodec = mappingManager.udtCodec(PendingTxn.class);
            cluster.getConfiguration().getCodecRegistry().register(pendingTxnTypeCodec);

            initialized = true;
        }
    }

    public static Session getSession() {
        if(!initialized) {
            throw new IllegalStateException("Cluster not yet initialized");
        }
        return session;
    }

    public static void close() {
        session.close();
        cluster.close();
    }

}
