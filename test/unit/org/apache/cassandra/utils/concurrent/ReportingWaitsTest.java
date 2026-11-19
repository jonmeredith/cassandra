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

package org.apache.cassandra.utils.concurrent;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;

import org.apache.cassandra.utils.Closeable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReportingWaitsTest
{
    private static final long FIRST_MILLIS = 20;
    private static final long MAX_MILLIS = 80;
    private static final long BOUND_MILLIS = 20_000;
    private static final String SITE = "test site";
    private static final Pattern CLOSING = Pattern.compile("Wait on test site (\\w+) after (\\d+)ms");

    private Logger logger;
    private Level originalLevel;
    private CapturingAppender appender;
    private ExecutorService executor;

    @Before
    public void setUp()
    {
        logger = (Logger) LoggerFactory.getLogger(ReportingWaits.class);
        originalLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        appender = new CapturingAppender();
        appender.start();
        logger.addAppender(appender);
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown()
    {
        logger.detachAppender(appender);
        logger.setLevel(originalLevel);
        executor.shutdownNow();
    }

    @Test
    public void nextIntervalDoublesUpToTheCap()
    {
        assertEquals(20, ReportingWaits.nextIntervalMillis(10, 40));
        assertEquals(40, ReportingWaits.nextIntervalMillis(20, 40));
        assertEquals(40, ReportingWaits.nextIntervalMillis(40, 40));
        assertEquals(40, ReportingWaits.nextIntervalMillis(30, 40));
    }

    @Test
    public void defaultsAreTenSecondsAndFiveMinutes()
    {
        assertEquals(10_000, ReportingWaits.FIRST_INTERVAL_MILLIS);
        assertEquals(300_000, ReportingWaits.MAX_INTERVAL_MILLIS);
    }

    @Test
    public void futureThatCompletesBeforeFirstIntervalLogsNothing() throws Exception
    {
        AsyncPromise<String> promise = new AsyncPromise<>();
        promise.setSuccess("done");

        assertEquals("done", ReportingWaits.get(promise, () -> SITE, null, 5_000, 5_000));
        assertTrue(appender.events().isEmpty());
    }

    @Test
    public void stalledFutureReportsRepeatedlyThenLogsCompletedWithDuration() throws Exception
    {
        AsyncPromise<String> promise = new AsyncPromise<>();
        Future<String> waiter = executor.submit(() -> ReportingWaits.get(promise, () -> SITE, null, FIRST_MILLIS, MAX_MILLIS));

        awaitEvents(e -> e.getFormattedMessage().startsWith("Still waiting on test site"), 2);
        promise.setSuccess("done");

        assertEquals("done", waiter.get(BOUND_MILLIS, TimeUnit.MILLISECONDS));
        Matcher closing = awaitClosing();
        assertEquals("completed", closing.group(1));
        assertTrue("duration covers the two intervals that expired", Long.parseLong(closing.group(2)) >= FIRST_MILLIS);
    }

    @Test
    public void failedFutureLogsFailed() throws Exception
    {
        AsyncPromise<String> promise = new AsyncPromise<>();
        Future<String> waiter = executor.submit(() -> ReportingWaits.get(promise, () -> SITE, null, FIRST_MILLIS, MAX_MILLIS));

        awaitEvents(e -> e.getFormattedMessage().startsWith("Still waiting on test site"), 1);
        promise.setFailure(new IllegalStateException("boom"));

        try
        {
            waiter.get(BOUND_MILLIS, TimeUnit.MILLISECONDS);
            fail("expected the failure to propagate");
        }
        catch (ExecutionException e)
        {
            assertTrue(e.getCause() instanceof ExecutionException);
        }
        assertEquals("failed", awaitClosing().group(1));
    }

    @Test
    public void interruptedFutureWaitLogsInterruptedAndRethrows() throws Exception
    {
        AsyncPromise<String> promise = new AsyncPromise<>();
        AtomicReference<Thread> waiterThread = new AtomicReference<>();
        Future<String> waiter = executor.submit(() -> {
            waiterThread.set(Thread.currentThread());
            return ReportingWaits.get(promise, () -> SITE, null, FIRST_MILLIS, MAX_MILLIS);
        });

        awaitEvents(e -> e.getFormattedMessage().startsWith("Still waiting on test site"), 1);
        waiterThread.get().interrupt();

        try
        {
            waiter.get(BOUND_MILLIS, TimeUnit.MILLISECONDS);
            fail("expected the interrupt to propagate");
        }
        catch (ExecutionException e)
        {
            assertTrue(e.getCause() instanceof InterruptedException);
        }
        assertEquals("interrupted", awaitClosing().group(1));
    }

    @Test
    public void stalledAwaitableLogsCompleted() throws Exception
    {
        Condition condition = Condition.newOneTimeCondition();
        Future<?> waiter = executor.submit(() -> ReportingWaits.await(condition, () -> SITE, null, FIRST_MILLIS, MAX_MILLIS));

        awaitEvents(e -> e.getFormattedMessage().startsWith("Still waiting on test site"), 1);
        condition.signal();

        waiter.get(BOUND_MILLIS, TimeUnit.MILLISECONDS);
        assertEquals("completed", awaitClosing().group(1));
    }

    @Test
    public void interruptedAwaitableLogsInterruptedAndRethrows() throws Exception
    {
        Condition condition = Condition.newOneTimeCondition();
        AtomicReference<Thread> waiterThread = new AtomicReference<>();
        Future<?> waiter = executor.submit(() -> {
            waiterThread.set(Thread.currentThread());
            ReportingWaits.await(condition, () -> SITE, null, FIRST_MILLIS, MAX_MILLIS);
        });

        awaitEvents(e -> e.getFormattedMessage().startsWith("Still waiting on test site"), 1);
        waiterThread.get().interrupt();

        try
        {
            waiter.get(BOUND_MILLIS, TimeUnit.MILLISECONDS);
            fail("expected the interrupt to propagate");
        }
        catch (ExecutionException e)
        {
            assertTrue(e.getCause() instanceof UncheckedInterruptedException);
        }
        assertEquals("interrupted", awaitClosing().group(1));
    }

    @Test
    public void withoutContextStallAndClosingLinesAreDebug() throws Exception
    {
        AsyncPromise<String> promise = new AsyncPromise<>();
        Future<String> waiter = executor.submit(() -> ReportingWaits.get(promise, () -> SITE, null, FIRST_MILLIS, MAX_MILLIS));

        awaitEvents(e -> e.getFormattedMessage().startsWith("Still waiting on test site"), 1);
        promise.setSuccess("done");
        waiter.get(BOUND_MILLIS, TimeUnit.MILLISECONDS);
        awaitClosing();

        for (ILoggingEvent event : appender.events())
            assertEquals(Level.DEBUG, event.getLevel());
    }

    @Test
    public void withContextStallAndClosingLinesAreWarnAndNameTheContext() throws Exception
    {
        AsyncPromise<String> promise = new AsyncPromise<>();
        Callable<String> task = () -> {
            try (Closeable ignored = WaitContext.enter(() -> "enacting epoch 42"))
            {
                return ReportingWaits.get(promise, () -> SITE, () -> "pending=[43]", FIRST_MILLIS, MAX_MILLIS);
            }
        };
        Future<String> waiter = executor.submit(task);

        List<ILoggingEvent> stalls = awaitEvents(e -> e.getFormattedMessage().startsWith("Still waiting on test site"), 1);
        promise.setSuccess("done");
        waiter.get(BOUND_MILLIS, TimeUnit.MILLISECONDS);
        awaitClosing();

        String stall = stalls.get(0).getFormattedMessage();
        assertTrue(stall, stall.contains("Context: enacting epoch 42."));
        assertTrue(stall, stall.endsWith(" pending=[43]"));
        for (ILoggingEvent event : appender.events())
            assertEquals(Level.WARN, event.getLevel());
    }

    @Test
    public void throwingSiteSupplierFallsBackToTheWaitedOnClass() throws Exception
    {
        AsyncPromise<String> promise = new AsyncPromise<>();
        Future<String> waiter = executor.submit(() -> ReportingWaits.get(promise, () -> { throw new IllegalStateException("boom"); },
                                                                         null, FIRST_MILLIS, MAX_MILLIS));

        List<ILoggingEvent> stalls = awaitEvents(e -> e.getFormattedMessage().startsWith("Still waiting on AsyncPromise"), 1);
        assertTrue(stalls.get(0).getFormattedMessage().startsWith("Still waiting on AsyncPromise after "));
        promise.setSuccess("done");
        waiter.get(BOUND_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Test
    public void anonymousFutureFallsBackToTheFullClassName() throws Exception
    {
        AsyncPromise<String> promise = new AsyncPromise<String>() {};
        Future<String> waiter = executor.submit(() -> ReportingWaits.get(promise, null, null, FIRST_MILLIS, MAX_MILLIS));

        List<ILoggingEvent> stalls = awaitEvents(e -> e.getFormattedMessage().startsWith("Still waiting on "), 1);
        String stall = stalls.get(0).getFormattedMessage();
        assertTrue(stall, stall.startsWith("Still waiting on " + promise.getClass().getName() + " after "));
        assertTrue(stall, stall.contains("ReportingWaitsTest$"));
        promise.setSuccess("done");
        waiter.get(BOUND_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Test
    public void cancelledFutureLogsFailedAndRethrows() throws Exception
    {
        AsyncPromise<String> promise = new AsyncPromise<>();
        Future<String> waiter = executor.submit(() -> ReportingWaits.get(promise, () -> SITE, null, FIRST_MILLIS, MAX_MILLIS));

        awaitEvents(e -> e.getFormattedMessage().startsWith("Still waiting on test site"), 1);
        promise.cancel(true);

        try
        {
            waiter.get(BOUND_MILLIS, TimeUnit.MILLISECONDS);
            fail("expected the cancellation to propagate");
        }
        catch (ExecutionException e)
        {
            assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof CancellationException);
        }
        assertEquals("failed", awaitClosing().group(1));
    }

    @Test
    public void suppliersAreNotCalledWhenTheWaitCompletesBeforeTheFirstInterval() throws Exception
    {
        AtomicInteger siteCalls = new AtomicInteger();
        AtomicInteger detailCalls = new AtomicInteger();
        AsyncPromise<String> promise = new AsyncPromise<>();
        promise.setSuccess("done");

        assertEquals("done", ReportingWaits.get(promise, () -> { siteCalls.incrementAndGet(); return SITE; },
                                                () -> { detailCalls.incrementAndGet(); return "detail"; }, 5_000, 5_000));
        assertEquals(0, siteCalls.get());
        assertEquals(0, detailCalls.get());
    }

    private List<ILoggingEvent> awaitEvents(Predicate<ILoggingEvent> matches, int count) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(BOUND_MILLIS);
        while (System.nanoTime() < deadline)
        {
            List<ILoggingEvent> found = appender.events().stream().filter(matches).collect(Collectors.toList());
            if (found.size() >= count)
                return found;
            Thread.sleep(5);
        }
        fail("Expected " + count + " matching log events, saw: " + appender.events());
        return null;
    }

    private Matcher awaitClosing() throws InterruptedException
    {
        List<ILoggingEvent> closing = awaitEvents(e -> CLOSING.matcher(e.getFormattedMessage()).matches(), 1);
        assertEquals("exactly one closing line", 1, closing.size());
        Matcher matcher = CLOSING.matcher(closing.get(0).getFormattedMessage());
        assertTrue(matcher.matches());
        return matcher;
    }
}
