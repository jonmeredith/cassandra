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
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.SimpleSeedProvider;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.shared.AssertUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JVMDTestTest extends TestBaseImpl
{
    @Test
    public void insertTimestampTest() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            long now = FBUtilities.timestampMicros();
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (id int primary key, i int)");
            cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, i) VALUES (1,1)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, i) VALUES (2,2) USING TIMESTAMP 1000", ConsistencyLevel.ALL);

            Object [][] res = cluster.coordinator(1).execute("SELECT writetime(i) FROM "+KEYSPACE+".tbl WHERE id = 1", ConsistencyLevel.ALL);
            assertEquals(1, res.length);
            assertTrue("ts="+res[0][0], (long)res[0][0] >= now);

            res = cluster.coordinator(1).execute("SELECT writetime(i) FROM "+KEYSPACE+".tbl WHERE id = 2", ConsistencyLevel.ALL);
            assertEquals(1, res.length);
            assertEquals(1000, (long) res[0][0]);
        }
    }

    @Test
    public void useYamlFragmentInConfigTest() throws IOException
    {
        String newKeystore = "/new/path/to/keystore";
        String newTruststore= "/tnew/path/to/truststore";
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.set("server_encryption_options",
                                                             "keystore: " + newKeystore + "\n" +
                                                             "truststore: " + newTruststore + "\n")).start())
        {
            cluster.get(1).runOnInstance(() -> {
                Assert.assertEquals(newKeystore, DatabaseDescriptor.getServerEncryptionOptions().keystore);
                Assert.assertEquals(newTruststore, DatabaseDescriptor.getServerEncryptionOptions().truststore);
            });
        }
    }

    @Test
    public void modifySchemaWithStoppedNode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build().withNodes(2).withConfig(c -> c.with(Feature.GOSSIP).with(Feature.NETWORK)).start()))
        {
            Assert.assertFalse(cluster.get(1).isShutdown());
            Assert.assertFalse(cluster.get(2).isShutdown());
            cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE "+KEYSPACE+".tbl1 (id int primary key, i int)");

            cluster.get(2).shutdown(true).get(1, TimeUnit.MINUTES);
            Assert.assertFalse(cluster.get(1).isShutdown());
            Assert.assertTrue(cluster.get(2).isShutdown());
            cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE "+KEYSPACE+".tbl2 (id int primary key, i int)");

            cluster.get(1).shutdown(true).get(1, TimeUnit.MINUTES);
            Assert.assertTrue(cluster.get(1).isShutdown());
            Assert.assertTrue(cluster.get(2).isShutdown());

            // both nodes down, nothing to record a schema change so should get an exception
            Throwable thrown = null;
            try
            {
                cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE " + KEYSPACE + ".tblX (id int primary key, i int)");
            }
            catch (Throwable tr)
            {
                thrown = tr;
            }
            Assert.assertNotNull("Expected to fail with all nodes down", thrown);

            // Have to restart instance1 before instance2 as it is hard-coded as the seed in in-JVM configuration.
            cluster.get(1).startup();
            cluster.get(2).startup();
            Assert.assertFalse(cluster.get(1).isShutdown());
            Assert.assertFalse(cluster.get(2).isShutdown());
            cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE "+KEYSPACE+".tbl3 (id int primary key, i int)");

            assertRows(cluster.get(1).executeInternal("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", KEYSPACE),
                       row("tbl1"), row("tbl2"), row("tbl3"));
            assertRows(cluster.get(2).executeInternal("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", KEYSPACE),
                       row("tbl1"), row("tbl2"), row("tbl3"));

            // Finally test schema can be changed with the first node down
            cluster.get(1).shutdown(true).get(1, TimeUnit.MINUTES);
            cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE "+KEYSPACE+".tbl4 (id int primary key, i int)");
            assertRows(cluster.get(2).executeInternal("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", KEYSPACE),
                       row("tbl1"), row("tbl2"), row("tbl3"), row("tbl4"));
        }
    }
}
