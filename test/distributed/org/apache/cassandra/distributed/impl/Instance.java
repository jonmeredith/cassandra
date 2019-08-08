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

package org.apache.cassandra.distributed.impl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.BatchlogManager;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IListen;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.IndexSummaryManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageDeliveryTask;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.distributed.impl.InstanceConfig.GOSSIP;
import static org.apache.cassandra.distributed.impl.InstanceConfig.NETWORK;

public class Instance extends IsolatedExecutor implements IInvokableInstance
{
    public final IInstanceConfig config;

    // should never be invoked directly, so that it is instantiated on other class loader;
    // only visible for inheritance
    Instance(IInstanceConfig config, ClassLoader classLoader)
    {
        super("node" + config.num(), classLoader);
        this.config = config;
        InstanceIDDefiner.setInstanceId(config.num());
        FBUtilities.setBroadcastInetAddress(config.broadcastAddressAndPort().address);
    }

    public IInstanceConfig config()
    {
        return config;
    }

    public ICoordinator coordinator()
    {
        return new Coordinator(this);
    }

    public IListen listen()
    {
        return new Listen(this);
    }

    @Override
    public InetAddressAndPort broadcastAddressAndPort() { return config.broadcastAddressAndPort(); }

    public Object[][] executeInternal(String query, Object... args)
    {
        return sync(() -> {
            ParsedStatement.Prepared prepared = QueryProcessor.prepareInternal(query);
            ResultMessage result = prepared.statement.executeInternal(QueryProcessor.internalQueryState(),
                                                                      QueryProcessor.makeInternalOptions(prepared, args));

            if (result instanceof ResultMessage.Rows)
                return RowUtil.toObjects((ResultMessage.Rows)result);
            else
                return null;
        }).call();
    }

    @Override
    public UUID schemaVersion()
    {
        // we do not use method reference syntax here, because we need to sync on the node-local schema instance
        //noinspection Convert2MethodRef
        return Schema.instance.getVersion();
    }

    public void startup()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isShutdown()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void schemaChangeInternal(String query)
    {
        sync(() -> {
            try
            {
                ClientState state = ClientState.forInternalCalls();
                state.setKeyspace(SystemKeyspace.NAME);
                QueryState queryState = new QueryState(state);

                CQLStatement statement = QueryProcessor.parseStatement(query, queryState).statement;
                statement.validate(state);

                QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
                statement.executeInternal(queryState, options);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error setting schema for test (query was: " + query + ")", e);
            }
        }).run();
    }

    // unnecessary if registerMockMessaging used
    private void registerFilter(ICluster cluster)
    {
//        MessagingService.instance().outboundSink.add((message, to) -> cluster.filters().permit(this, cluster.get(to), message.verb().id));
    }
    private void registerMockMessaging(ICluster cluster)
    {
        BiConsumer<InetAddressAndPort, IMessage> deliverToInstance = (to, message) -> cluster.get(to).receiveMessage(message);
        BiConsumer<InetAddressAndPort, IMessage> deliverToInstanceIfNotFiltered = cluster.filters().filter(deliverToInstance);

        Map<InetAddress, InetAddressAndPort> addressAndPortMap = new HashMap<>();
        cluster.stream().forEach(instance -> {
            InetAddressAndPort addressAndPort = instance.broadcastAddressAndPort();
            if (!addressAndPort.equals(instance.config().broadcastAddressAndPort()))
                throw new IllegalStateException("addressAndPort mismatch: " + addressAndPort + " vs " + instance.config().broadcastAddressAndPort());
            InetAddressAndPort prev = addressAndPortMap.put(addressAndPort.address, addressAndPort);
            if (null != prev)
                throw new IllegalStateException("This version of Cassandra does not support multiple nodes with the same InetAddress: " + addressAndPort + " vs " + prev);
        });

        MessagingService.instance().addMessageSink(
                new MessageDeliverySink(deliverToInstanceIfNotFiltered, addressAndPortMap::get));
    }

    private class MessageDeliverySink implements IMessageSink
    {
        private final BiConsumer<InetAddressAndPort, IMessage> deliver;
        private final Function<InetAddress, InetAddressAndPort> lookupAddressAndPort;
        MessageDeliverySink(BiConsumer<InetAddressAndPort, IMessage> deliver, Function<InetAddress, InetAddressAndPort> lookupAddressAndPort)
        {
            this.deliver = deliver;
            this.lookupAddressAndPort = lookupAddressAndPort;
        }

        public boolean allowOutgoingMessage(MessageOut messageOut, int id, InetAddress to)
        {
            try (DataOutputBuffer out = new DataOutputBuffer(1024))
            {
                InetAddressAndPort from = broadcastAddressAndPort();
                assert from.equals(lookupAddressAndPort.apply(messageOut.from));
                InetAddressAndPort toFull = lookupAddressAndPort.apply(to);
                int version = MessagingService.instance().getVersion(to);
                messageOut.serialize(out, version);
                deliver.accept(toFull, new Message(messageOut.verb.ordinal(), out.toByteArray(), id, version, from));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return false;
        }

        public boolean allowIncomingMessage(MessageIn message, int id)
        {
            // we can filter to our heart's content on the outgoing message; no need to worry about incoming
            return true;
        }
    }

    public void receiveMessage(IMessage message)
    {
        sync(() -> {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(message.bytes())))
            {
                MessageIn<?> messageIn = MessageIn.read(in, message.version(), message.id());
                Runnable deliver = new MessageDeliveryTask(messageIn, message.id(), System.currentTimeMillis(), false);
                deliver.run();
            }
            catch (Throwable t)
            {
                throw new RuntimeException("Exception occurred on node " + broadcastAddressAndPort(), t);
            }
        }).run();
    }

    public int getMessagingVersion()
    {
        return callsOnInstance(() -> MessagingService.current_version).call();
    }

    public void setMessagingVersion(InetAddressAndPort endpoint, int version)
    {
        runOnInstance(() -> MessagingService.instance().setVersion(endpoint.address, version));
    }

    @Override
    public void startup(ICluster cluster)
    {
        sync(() -> {
            try
            {
                mkdirs();

                Config.setOverrideLoadConfig(() -> loadConfig(config));
                DatabaseDescriptor.setDaemonInitialized();
                DatabaseDescriptor.createAllDirectories();

                // We need to persist this as soon as possible after startup checks.
                // This should be the first write to SystemKeyspace (CASSANDRA-11742)
                SystemKeyspace.persistLocalMetadata();

                try
                {
                    // load schema from disk
                    Schema.instance.loadFromDisk();
                }
                catch (Exception e)
                {
                    throw e;
                }

                Keyspace.setInitialized();

                // Replay any CommitLogSegments found on disk
                try
                {
                    CommitLog.instance.recover();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }

                if (config.has(NETWORK))
                {
                    registerFilter(cluster);
                    MessagingService.instance().listen();
                }
                else
                {
                    // Even though we don't use MessagingService, access the static SocketFactory
                    // instance here so that we start the static event loop state
//                    -- not sure what that means?  SocketFactory.instance.getClass();
                    registerMockMessaging(cluster);
                }

                // TODO: this is more than just gossip
                if (config.has(GOSSIP))
                {
                    StorageService.instance.initServer();
                }
                else
                {
                    initializeRing(cluster);
                }

                SystemKeyspace.finishStartup();

                if (!FBUtilities.getBroadcastAddress().equals(broadcastAddressAndPort().address))
                    throw new IllegalStateException();
                if (DatabaseDescriptor.getStoragePort() != broadcastAddressAndPort().port)
                    throw new IllegalStateException();
            }
            catch (Throwable t)
            {
                if (t instanceof RuntimeException)
                    throw (RuntimeException) t;
                throw new RuntimeException(t);
            }
        }).run();
    }

    private void mkdirs()
    {
        new File(config.getString("saved_caches_directory")).mkdirs();
        new File(config.getString("commitlog_directory")).mkdirs();
        for (String dir : (String[]) config.get("data_file_directories"))
            new File(dir).mkdirs();
    }

    private static Config loadConfig(IInstanceConfig overrides)
    {
        Config config = new Config();
        overrides.propagate(config);
        return config;
    }

    private void initializeRing(ICluster cluster)
    {
        // This should be done outside instance in order to avoid serializing config
        String partitionerName = config.getString("partitioner");
        List<String> initialTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();
        for (int i = 1 ; i <= cluster.size() ; ++i)
        {
            IInstanceConfig config = cluster.get(i).config();
            initialTokens.add(config.getString("initial_token"));
            hosts.add(config.broadcastAddressAndPort());
            hostIds.add(config.hostId());
        }

        try
        {
            IPartitioner partitioner = FBUtilities.newPartitioner(partitionerName);
            StorageService storageService = StorageService.instance;
            List<Token> tokens = new ArrayList<>();
            for (String token : initialTokens)
                tokens.add(partitioner.getTokenFactory().fromString(token));

            for (int i = 0; i < tokens.size(); i++)
            {
                InetAddressAndPort ep = hosts.get(i);
                Gossiper.instance.initializeNodeUnsafe(ep.address, hostIds.get(i), 1);
                Gossiper.instance.injectApplicationState(ep.address,
                        ApplicationState.TOKENS,
                        new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(tokens.get(i))));
                storageService.onChange(ep.address,
                        ApplicationState.STATUS,
                        new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(tokens.get(i))));
                Gossiper.instance.realMarkAlive(ep.address, Gossiper.instance.getEndpointStateForEndpoint(ep.address));

                int version = Math.min(MessagingService.current_version, cluster.get(ep).getMessagingVersion());
                MessagingService.instance().setVersion(ep.address, version);
            }

            // check that all nodes are in token metadata
            for (int i = 0; i < tokens.size(); ++i)
                assert storageService.getTokenMetadata().isMember(hosts.get(i).address);
        }
        catch (Throwable e) // UnknownHostException
        {
            throw new RuntimeException(e);
        }
    }

    public Future<Void> shutdown()
    {
        return shutdown(true);
    }

    public Future<Void> shutdown(boolean graceful)
    {
        Future<?> future = async((ExecutorService executor) -> {
            Throwable error = null;

            if (config.has(GOSSIP) || config.has(NETWORK))
            {
                StorageService.instance.shutdownServer();

                error = parallelRun(error, executor,
                    () -> NanoTimeToCurrentTimeMillis.shutdown(MINUTES.toMillis(1L))
                );
            }

            error = parallelRun(error, executor,
                    () -> Gossiper.instance.stopShutdownAndWait(1L, MINUTES),
                    CompactionManager.instance::forceShutdown,
                    BatchlogManager::shutdown,
                    () -> HintedHandOffManager.instance.shutdownAndWait(1L, MINUTES),
                    () -> StreamCoordinator.shutdownAndWait(1L, MINUTES),
                    () -> IndexSummaryManager.instance.shutdownAndWait(1L, MINUTES),
                    () -> ColumnFamilyStore.shutdownExecutorsAndWait(1L, MINUTES),
                    PendingRangeCalculatorService.instance::shutdownExecutor,
                    StorageService.instance::shutdownBGMonitor,
                    Ref::shutdownReferenceReaper,
                    Memtable.MEMORY_POOL::shutdown,
                    SSTableReader::shutdownBlocking
            );
            error = parallelRun(error, executor,
                                ScheduledExecutors::shutdownAndWait,
                                CommitLog.instance::shutdownBlocking,
                                MessagingService.instance()::shutdown
            );
            error = parallelRun(error, executor,
                                StageManager::shutdownAndWait,
                                SharedExecutorPool.SHARED::shutdown
            );

            Throwables.maybeFail(error);
        }).apply(isolatedExecutor);

        return CompletableFuture.runAsync(ThrowingRunnable.toRunnable(future::get), isolatedExecutor)
                                .thenRun(super::shutdown);
    }

    private static Throwable parallelRun(Throwable accumulate, ExecutorService runOn, ThrowingRunnable ... runnables)
    {
        List<Future<Throwable>> results = new ArrayList<>();
        for (ThrowingRunnable runnable : runnables)
        {
            results.add(runOn.submit(() -> {
                try
                {
                    runnable.run();
                    return null;
                }
                catch (Throwable t)
                {
                    return t;
                }
            }));
        }
        for (Future<Throwable> future : results)
        {
            try
            {
                Throwable t = future.get();
                if (t != null)
                    throw t;
            }
            catch (Throwable t)
            {
                accumulate = Throwables.merge(accumulate, t);
            }
        }
        return accumulate;
    }
}
