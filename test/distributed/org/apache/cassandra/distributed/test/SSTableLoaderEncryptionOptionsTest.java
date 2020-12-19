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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.shaded.netty.handler.ssl.SslContextBuilder;
import com.datastax.shaded.netty.handler.ssl.SslProvider;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.tools.BulkLoader;
import org.apache.cassandra.tools.ToolRunner;


public class SSTableLoaderEncryptionOptionsTest extends AbstractEncryptionOptionsImpl
{
    public static void checkDriverCanConnect(String host, int port)
    {
        com.datastax.driver.core.Cluster cluster = null;
        try
        {
            KeyStore ks = KeyStore.getInstance("JKS");
            // make sure you close this stream properly (not shown here for brevity)
            InputStream trustStore = new FileInputStream(validKeyStorePath);
            ks.load(trustStore, validKeyStorePassword.toCharArray());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);

            SslContextBuilder builder = SslContextBuilder
                                        .forClient()
                                        .sslProvider(SslProvider.JDK)
                                        .trustManager(tmf);

            SSLOptions sslOptions = new RemoteEndpointAwareNettySSLOptions(builder.build());

            cluster = com.datastax.driver.core.Cluster.builder().addContactPoints(host).withPort(port)
                                                      .withSSL(sslOptions)
                                                      .build();

            // The Session is what you use to execute queries. Likewise, it is thread-safe and should be
            // reused.
            Session session = cluster.connect();

            // We use execute to send a query to Cassandra. This returns a ResultSet, which is essentially
            // a collection
            // of Row objects.
            ResultSet rs = session.execute("select release_version from system.local");
            //  Extract the first row (which is the only one in this case).
            Row row = rs.one();

            // Extract the value of the first (and only) column from the row.
            String releaseVersion = row.getString("release_version");
            System.out.printf("Cassandra version is: %s%n", releaseVersion);

            cluster.close();
            trustStore.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (CertificateException e)
        {
            e.printStackTrace();
        }
        catch (NoSuchAlgorithmException e)
        {
            e.printStackTrace();
        }
        catch (KeyStoreException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void bulkLoaderConnectOverSsl() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL, Feature.NETWORK, Feature.GOSSIP); // need gossip to get hostid for java driver
            c.set("server_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("internode_encryption", "all")
                              .put("optional", false)
                              .build());
            c.set("client_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("enabled", true)
                              .put("optional", false)
                              .build());
        }).start())
        {
            String nodes = cluster.get(1).config().broadcastAddress().getHostString();
            int nativePort = cluster.get(1).callOnInstance(DatabaseDescriptor::getNativeTransportPort);
            int storagePort = cluster.get(1).callOnInstance(DatabaseDescriptor::getStoragePort);

            checkDriverCanConnect(nodes, nativePort);

            ToolRunner.ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                                "--nodes", nodes,
                                                                "--port", Integer.toString(nativePort),
                                                                "--storage-port", Integer.toString(storagePort),
                                                                "--keystore", validKeyStorePath,
                                                                "--keystore-password", validKeyStorePassword,
                                                                "--truststore", validTrustStorePath,
                                                                "--truststore-password", validTrustStorePassword,
                                                                "test/data/legacy-sstables/na/legacy_tables/legacy_na_clust");
            tool.assertOnCleanExit();

//            System.out.println(String.join(" ", "--nodes", nodes,
//                                           "--port", Integer.toString(nativePort),
//                                           "--storage-port", Integer.toString(storagePort),
//                                           "--keystore", validKeyStorePath,
//                                           "--keystore-password", validKeyStorePassword,
//                                           "--truststore", validTrustStorePath,
//                                           "--truststore-password", validTrustStorePassword,
//                                           "test/data/legacy-sstables/na/legacy_tables/legacy_na_clust"));
//            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MINUTES);

            //            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
//            ToolRunner.ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
//                                                                "--nodes", nodes,
//                                                                "-f", "/tmp/cassandra.yaml",
//                                                                "test/data/legacy-sstables/na/legacy_tables/legacy_na_clust");


        }
    }
}
