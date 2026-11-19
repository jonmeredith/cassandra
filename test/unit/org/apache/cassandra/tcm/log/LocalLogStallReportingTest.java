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

package org.apache.cassandra.tcm.log;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.Test;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.CapturingAppender;
import org.apache.cassandra.utils.concurrent.ReportingWaits;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Checks that the log names the epoch when a listener waits on a future while the follower enacts
 * it. The first report comes after ten seconds, so this test takes about eleven.
 */
public class LocalLogStallReportingTest extends CQLTester
{
    private static final long BOUND_SECONDS = 60;

    @Test
    public void listenerWaitIsReportedWithTheEpochBeingEnacted() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v int)");

        Logger reportLogger = (Logger) LoggerFactory.getLogger(ReportingWaits.class);
        Level originalLevel = reportLogger.getLevel();
        reportLogger.setLevel(Level.DEBUG);
        CapturingAppender appender = new CapturingAppender();
        appender.start();
        reportLogger.addAppender(appender);

        AsyncPromise<Object> never = new AsyncPromise<>();
        AtomicBoolean armed = new AtomicBoolean(true);
        AtomicReference<Epoch> held = new AtomicReference<>();
        ChangeListener holder = new ChangeListener()
        {
            @Override
            public void notifyPreCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
            {
                if (armed.compareAndSet(true, false))
                {
                    held.set(next.epoch);
                    FBUtilities.waitOnFuture(never);
                }
            }
        };

        ClusterMetadataService.instance().log().addListener(holder);
        ExecutorService ddl = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "LocalLogStallReportingTest-ddl");
            thread.setDaemon(true);
            return thread;
        });
        try
        {
            Future<?> alter = ddl.submit(() -> schemaChange("ALTER TABLE " + KEYSPACE + '.' + currentTable() + " ADD v2 int"));

            ILoggingEvent stall = awaitEvent(appender, e -> e.getThreadName().contains("GlobalLogFollower")
                                                            && e.getFormattedMessage().startsWith("Still waiting on "));
            String message = stall.getFormattedMessage();
            assertEquals(message, Level.WARN, stall.getLevel());
            assertTrue(message, message.contains("enacting epoch " + held.get()));

            never.setSuccess(null);
            ILoggingEvent closing = awaitEvent(appender, e -> e.getThreadName().contains("GlobalLogFollower")
                                                              && e.getFormattedMessage().contains(" completed after "));
            assertEquals(Level.WARN, closing.getLevel());
            alter.get(BOUND_SECONDS, TimeUnit.SECONDS);
        }
        finally
        {
            never.trySuccess(null);
            ClusterMetadataService.instance().log().removeListener(holder);
            ddl.shutdownNow();
            reportLogger.detachAppender(appender);
            reportLogger.setLevel(originalLevel);
        }
    }

    private static ILoggingEvent awaitEvent(CapturingAppender appender, Predicate<ILoggingEvent> matches) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(BOUND_SECONDS);
        while (System.nanoTime() < deadline)
        {
            List<ILoggingEvent> found = appender.events().stream().filter(matches).collect(Collectors.toList());
            if (!found.isEmpty())
                return found.get(0);
            Thread.sleep(50);
        }
        fail("No matching log event within " + BOUND_SECONDS + "s. Saw: " + appender.events());
        return null;
    }
}
