/*
 * Copyright 2017 Pragmasol
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
