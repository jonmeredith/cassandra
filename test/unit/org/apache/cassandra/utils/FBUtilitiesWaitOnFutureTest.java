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

package org.apache.cassandra.utils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FBUtilitiesWaitOnFutureTest
{
    @Test
    public void waitOnFutureReturnsTheValue()
    {
        AsyncPromise<String> promise = new AsyncPromise<>();
        promise.setSuccess("done");
        assertEquals("done", FBUtilities.waitOnFuture(promise));
    }

    @Test
    public void waitOnFutureThrowsTheCleanedCauseOfAFailure()
    {
        AsyncPromise<String> promise = new AsyncPromise<>();
        promise.setFailure(new IllegalStateException("boom"));
        try
        {
            FBUtilities.waitOnFuture(promise);
            fail("expected the failure");
        }
        catch (IllegalStateException e)
        {
            assertEquals("boom", e.getMessage());
        }
    }

    @Test
    public void waitOnFutureThrowsUncheckedInterruptedWhenInterrupted()
    {
        AsyncPromise<String> promise = new AsyncPromise<>();
        Thread.currentThread().interrupt();
        try
        {
            FBUtilities.waitOnFuture(promise);
            fail("expected the interrupt");
        }
        catch (UncheckedInterruptedException expected)
        {
            // expected
        }
        finally
        {
            Thread.interrupted();
        }
    }

    @Test
    public void untimedWaitOnFuturesReturnsResultsInOrder()
    {
        AsyncPromise<Integer> first = new AsyncPromise<>();
        AsyncPromise<Integer> second = new AsyncPromise<>();
        first.setSuccess(1);
        second.setSuccess(2);
        List<Integer> results = FBUtilities.waitOnFutures(Arrays.asList(first, second), 0, TimeUnit.SECONDS);
        assertEquals(Arrays.asList(1, 2), results);
    }

    @Test
    public void untimedWaitOnFuturesFailsAfterWaitingOnEveryFuture() throws InterruptedException
    {
        AsyncPromise<Integer> failed = new AsyncPromise<>();
        AsyncPromise<Integer> pending = new AsyncPromise<>();
        failed.setFailure(new IllegalStateException("boom"));
        Thread completer = new Thread(() -> {
            try
            {
                Thread.sleep(200);
            }
            catch (InterruptedException e)
            {
                return;
            }
            pending.setSuccess(2);
        }, "FBUtilitiesWaitOnFutureTest-completer");
        completer.setDaemon(true);
        completer.start();
        try
        {
            FBUtilities.waitOnFutures(Arrays.asList(failed, pending), 0, TimeUnit.SECONDS);
            fail("expected the failure");
        }
        catch (RuntimeException e)
        {
            boolean found = false;
            for (Throwable t = e; t != null; t = t.getCause())
                found |= "boom".equals(t.getMessage());
            assertTrue("failure chain should contain boom: " + e, found);
            assertTrue("the call waits on the pending future before it throws", pending.isDone());
        }
        finally
        {
            completer.interrupt();
            completer.join(5_000);
        }
    }
}
