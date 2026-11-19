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

import org.junit.Test;

import org.apache.cassandra.utils.Closeable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WaitContextTest
{
    @Test
    public void inactiveByDefault()
    {
        assertFalse(WaitContext.isActive());
        assertEquals("unknown", WaitContext.describe());
    }

    @Test
    public void scopeSetsAndClearsDescription()
    {
        try (Closeable ignored = WaitContext.enter(() -> "committing"))
        {
            assertTrue(WaitContext.isActive());
            assertEquals("committing", WaitContext.describe());
        }
        assertFalse(WaitContext.isActive());
    }

    @Test
    public void nestedScopeRestoresOuterDescription()
    {
        try (Closeable outer = WaitContext.enter(() -> "outer"))
        {
            try (Closeable inner = WaitContext.enter(() -> "inner"))
            {
                assertEquals("inner", WaitContext.describe());
            }
            assertEquals("outer", WaitContext.describe());
        }
        assertFalse(WaitContext.isActive());
    }

    @Test
    public void throwingSupplierDescribesAsUnknown()
    {
        try (Closeable ignored = WaitContext.enter(() -> { throw new IllegalStateException("boom"); }))
        {
            assertEquals("unknown", WaitContext.describe());
        }
    }

    @Test
    public void descriptionIsPerThread() throws Exception
    {
        try (Closeable ignored = WaitContext.enter(() -> "mine"))
        {
            boolean[] otherThreadActive = new boolean[1];
            Thread other = new Thread(() -> otherThreadActive[0] = WaitContext.isActive());
            other.start();
            other.join();
            assertFalse(otherThreadActive[0]);
        }
    }
}
