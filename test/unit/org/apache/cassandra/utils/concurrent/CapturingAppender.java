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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

/**
 * Captures log events from any thread. The Logback {@code ListAppender} keeps an {@code ArrayList},
 * so a test that reads it while another thread logs can fail with a concurrent modification.
 */
public class CapturingAppender extends AppenderBase<ILoggingEvent>
{
    private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

    @Override
    protected void append(ILoggingEvent event)
    {
        events.add(event);
    }

    public List<ILoggingEvent> events()
    {
        return new ArrayList<>(events);
    }
}
