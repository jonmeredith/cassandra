/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.distributed.impl.InstanceConfig.GOSSIP;
import static org.apache.cassandra.distributed.impl.InstanceConfig.NETWORK;

public class MultiDcClusterTest extends DistributedTestBase
{

    @Test
    public void testSimple()
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withDCs(2)
                                      .start())
        {
            System.out.println("@@@@@@" + cluster + "@@@@@");
            Assert.assertEquals(1, cluster.get(1).config().num());
            Assert.assertEquals(2, cluster.get(2).config().num());
            Assert.assertEquals(3, cluster.get(3).config().num());
//            Assert.assertEquals("datacenter0", cluster.get(1).sync(() -> DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort())).call());
//            Assert.assertEquals("datacenter0", cluster.get(2).sync(() -> DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort())).call());
//            Assert.assertEquals("datacenter1", cluster.get(3).sync(() -> DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort())).call());
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void sendMessagesToNonlocalDCRandomizesTargetTest()
    {
        Map<Pair<String,String>,Integer> topologyCount = new HashMap<>(4);
        topologyCount.put(Pair.create("dc0", "r1"), 1);
        topologyCount.put(Pair.create("dc1", "r1"), 1);
        topologyCount.put(Pair.create("dc1", "r2"), 1);
        topologyCount.put(Pair.create("dc1", "r3"), 1);
        try (Cluster cluster = init(Cluster.build(4)
                                      .withDCsAndRacks(topologyCount)
                                      .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
            UUID sessionId = UUIDGen.getTimeUUID();
            cluster.coordinator(1).executeTraced(sessionId, "INSERT INTO " + KEYSPACE + ".tbl(pk,ck,v) VALUES (1, 1, 'x')", ConsistencyLevel.ALL);
            Object[][] result = cluster.coordinator(1).execute("SELECT keyspace_name FROM system_schema.keyspaces", ConsistencyLevel.ALL, sessionId);
            result = cluster.coordinator(1).execute("SELECT * FROM system_traces.events WHERE session_id = ?", ConsistencyLevel.ALL, sessionId);
            System.out.println(result);
//
//
//            System.out.println("@@@@@@" + cluster + "@@@@@");
//            Assert.assertEquals(1, cluster.get(1).config().num());
//            Assert.assertEquals(2, cluster.get(2).config().num());
//            Assert.assertEquals(3, cluster.get(3).config().num());
//            Assert.assertEquals(4, cluster.get(4).config().num());
//            cluster.coordinator(1).execute("")
//            Assert.assertEquals("datacenter0", cluster.get(1).sync(
//            () -> {
//                final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
//                return snitch.getDatacenter(FBUtilities.getBroadcastAddressAndPort());
//            }
//            ).call());
//            Assert.assertEquals("datacenter0", cluster.get(2).sync(() -> DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort())).call());
//            Assert.assertEquals("datacenter1", cluster.get(3).sync(() -> DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort())).call());
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
