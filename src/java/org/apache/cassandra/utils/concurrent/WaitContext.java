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

import java.util.function.Supplier;

import org.apache.cassandra.utils.Closeable;

/**
 * Describes what the current thread is doing. {@link ReportingWaits} names that description when a
 * wait on this thread stalls. The caller that enters the scope supplies the text, so this class
 * does not depend on TCM or any other subsystem. Callers must enter a scope with
 * try-with-resources, so a pooled thread does not keep a stale description.
 */
public class WaitContext
{
    private static final ThreadLocal<Supplier<String>> DESCRIPTION = new ThreadLocal<>();

    private WaitContext() {}

    /**
     * Describes what this thread is doing until the returned {@link Closeable} closes. Scopes nest.
     * Closing a scope restores the description that was in place before it.
     */
    public static Closeable enter(Supplier<String> description)
    {
        Supplier<String> previous = DESCRIPTION.get();
        DESCRIPTION.set(description);
        return () -> {
            if (previous == null)
                DESCRIPTION.remove();
            else
                DESCRIPTION.set(previous);
        };
    }

    public static boolean isActive()
    {
        return DESCRIPTION.get() != null;
    }

    /**
     * @return the current description, or "unknown" when none is set or the supplier throws
     */
    public static String describe()
    {
        Supplier<String> description = DESCRIPTION.get();
        if (description == null)
            return "unknown";

        try
        {
            return description.get();
        }
        catch (Throwable t)
        {
            return "unknown";
        }
    }
}
