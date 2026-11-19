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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Waits that would otherwise block with no deadline. Each wait runs as a loop of timed waits, so a
 * stalled thread says what it waits on and for how long. The loop never abandons the wait. Each
 * expiry logs and waits again.
 * <p>
 * The first report comes after {@link #FIRST_INTERVAL_MILLIS}. Each later interval doubles, up to
 * {@link #MAX_INTERVAL_MILLIS}, so a stall of hours logs a few lines an hour. A wait that reported
 * at least once logs one closing line when it ends. The line gives the outcome and the total time
 * blocked.
 * <p>
 * With a {@link WaitContext} active the lines are warnings. Without one the lines are debug, which
 * keeps them out of system.log for callers of shared utilities that did not opt in.
 */
public class ReportingWaits
{
    private static final Logger logger = LoggerFactory.getLogger(ReportingWaits.class);

    @VisibleForTesting
    static final long FIRST_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(10);
    @VisibleForTesting
    static final long MAX_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(5);

    private static final String COMPLETED = "completed";
    private static final String FAILED = "failed";
    private static final String INTERRUPTED = "interrupted";

    private ReportingWaits() {}

    /**
     * Awaits the given {@link Awaitable} and reports until it is signalled. An interrupt propagates
     * as {@link UncheckedInterruptedException}, as
     * {@link Awaitable#awaitThrowUncheckedOnInterrupt()} does.
     *
     * @param site names the wait. The method calls it only when it logs a report. Null names the
     *             wait by its class.
     * @param detail adds state to the report. The method calls it only when it logs a report. May
     *               be null.
     */
    public static void await(Awaitable awaitable, Supplier<String> site, Supplier<String> detail)
    {
        await(awaitable, site, detail, FIRST_INTERVAL_MILLIS, MAX_INTERVAL_MILLIS);
    }

    /**
     * Waits for the given future and reports until it completes. An interrupt propagates as
     * {@link InterruptedException} and a failure as {@link ExecutionException}, as
     * {@link Future#get()} does.
     *
     * @param site names the wait. The method calls it only when it logs a report. Null names the
     *             wait by its class.
     * @param detail adds state to the report. The method calls it only when it logs a report. May
     *               be null.
     */
    public static <T> T get(Future<T> future, Supplier<String> site, Supplier<String> detail) throws InterruptedException, ExecutionException
    {
        return get(future, site, detail, FIRST_INTERVAL_MILLIS, MAX_INTERVAL_MILLIS);
    }

    @VisibleForTesting
    static void await(Awaitable awaitable, Supplier<String> site, Supplier<String> detail, long firstIntervalMillis, long maxIntervalMillis)
    {
        Stall stall = new Stall(awaitable, site, detail, firstIntervalMillis, maxIntervalMillis);
        String outcome = FAILED;
        try
        {
            while (!awaitable.awaitThrowUncheckedOnInterrupt(stall.intervalMillis, TimeUnit.MILLISECONDS))
                stall.expired();
            outcome = COMPLETED;
        }
        catch (UncheckedInterruptedException e)
        {
            outcome = INTERRUPTED;
            throw e;
        }
        finally
        {
            stall.finished(outcome);
        }
    }

    @VisibleForTesting
    static <T> T get(Future<T> future, Supplier<String> site, Supplier<String> detail, long firstIntervalMillis, long maxIntervalMillis) throws InterruptedException, ExecutionException
    {
        Stall stall = new Stall(future, site, detail, firstIntervalMillis, maxIntervalMillis);
        String outcome = FAILED;
        try
        {
            while (true)
            {
                try
                {
                    T result = future.get(stall.intervalMillis, TimeUnit.MILLISECONDS);
                    outcome = COMPLETED;
                    return result;
                }
                catch (TimeoutException e)
                {
                    stall.expired();
                }
            }
        }
        catch (InterruptedException e)
        {
            outcome = INTERRUPTED;
            throw e;
        }
        finally
        {
            stall.finished(outcome);
        }
    }

    @VisibleForTesting
    static long nextIntervalMillis(long currentMillis, long maxMillis)
    {
        return Math.min(currentMillis * 2, maxMillis);
    }

    /** Tracks one wait. It holds the start time, the current interval and the reported flag. */
    private static final class Stall
    {
        private final Object waitingOn;
        private final Supplier<String> site;
        private final Supplier<String> detail;
        private final long maxIntervalMillis;
        private final long startNanos = nanoTime();
        private long intervalMillis;
        private boolean reported;

        private Stall(Object waitingOn, Supplier<String> site, Supplier<String> detail, long firstIntervalMillis, long maxIntervalMillis)
        {
            this.waitingOn = waitingOn;
            this.site = site;
            this.detail = detail;
            this.intervalMillis = firstIntervalMillis;
            this.maxIntervalMillis = maxIntervalMillis;
        }

        private void expired()
        {
            reported = true;
            log("Still waiting on {} after {}ms. Context: {}.{}", name(), elapsedMillis(), WaitContext.describe(), detailSuffix());
            intervalMillis = nextIntervalMillis(intervalMillis, maxIntervalMillis);
        }

        private void finished(String outcome)
        {
            if (reported)
                log("Wait on {} {} after {}ms", name(), outcome, elapsedMillis());
        }

        private long elapsedMillis()
        {
            return TimeUnit.NANOSECONDS.toMillis(nanoTime() - startNanos);
        }

        private String name()
        {
            String named = safeGet(site);
            if (!named.isEmpty())
                return named;
            Class<?> waitedOn = waitingOn.getClass();
            String simple = waitedOn.getSimpleName();
            return simple.isEmpty() ? waitedOn.getName() : simple;
        }

        private String detailSuffix()
        {
            String described = safeGet(detail);
            return described.isEmpty() ? "" : " " + described;
        }
    }

    private static void log(String format, Object... args)
    {
        if (WaitContext.isActive())
            logger.warn(format, args);
        else
            logger.debug(format, args);
    }

    private static String safeGet(Supplier<String> supplier)
    {
        if (supplier == null)
            return "";

        try
        {
            String value = supplier.get();
            return value == null ? "" : value;
        }
        catch (Throwable t)
        {
            return "";
        }
    }
}
