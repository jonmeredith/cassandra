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
package org.apache.cassandra.net;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FixedMonotonicClock;
import org.apache.cassandra.utils.Isolator;
import org.apache.cassandra.utils.SchemaIsolator;
import org.assertj.core.api.Assertions;
import org.quicktheories.core.Gen;

import static org.apache.cassandra.net.Message.serializer;
import static org.apache.cassandra.net.MessagingService.versionToHumanReadable;
import static org.apache.cassandra.utils.CassandraGenerators.MESSAGE_GEN;
import static org.apache.cassandra.utils.FailingConsumer.orFail;
import static org.quicktheories.QuickTheory.qt;

// REVIEW Maybe more accurately MessageSerializationPropertyTest
public class MessagePropertyTest implements Serializable
{
    static
    {
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
        // message serialization uses the MonotonicClock class for precise and approx timestamps, so mock it out
        System.setProperty("cassandra.monotonic_clock.precise", FixedMonotonicClock.class.getName());
        System.setProperty("cassandra.monotonic_clock.approx", FixedMonotonicClock.class.getName());

        DatabaseDescriptor.daemonInitialization();
    }

    private static final Isolator SCHEMA_ISOLATOR = SchemaIsolator.create();

    /**
     * Validates that {@link Message#serializedSize(int)} == {@link Message.Serializer#serialize(Message, DataOutputPlus, int)} size.
     */
    @Test
    public void serializeSizeProperty()
    {
        try (DataOutputBuffer out = new DataOutputBuffer(1024))
        {
            qt().forAll(MESSAGE_GEN).checkAssert(orFail(message -> {
                for (int version : MessagingService.supportedVersions())
                {
                    out.clear();
                    serializer.serialize(message, out, version);
                    Assertions.assertThat(out.getLength())
                              .as("Property serialize(out, version).length == serializedSize(version) " +
                                  "was violated for version %s and verb %s",
                                  versionToHumanReadable(version), message.header.verb)
                              .isEqualTo(message.serializedSize(version));
                }
            }));
        }
    }

    /**
     * Message and payload don't define equals, so have to rely on another way to define equality; serialized bytes!
     * The assumption is that serialize(deserialize(serialize(message))) == serialize(message)
     */
    @Test
    public void serdeBytes() throws Exception
    {
        SCHEMA_ISOLATOR.runIn(() -> {
            try (DataOutputBuffer first = new DataOutputBuffer(1024);
                 DataOutputBuffer second = new DataOutputBuffer(1024))
            {
                qt().forAll(MESSAGE_GEN).checkAssert(orFail(message -> {
                    withTable(message, orFail(ignore -> {
                        for (int version : MessagingService.supportedVersions())
                        {
                            first.clear();
                            second.clear();

                            serializer.serialize(message, first, version);
                            Message<Object> read = serializer.deserialize(new DataInputBuffer(first.buffer(), true), FBUtilities.getBroadcastAddressAndPort(), version);
                            serializer.serialize(read, second, version);
                            // using hex as byte buffer equality kept failing, and was harder to debug difference
                            // using hex means the specific section of the string that is different will be shown
                            Assertions.assertThat(ByteBufferUtil.bytesToHex(second.buffer()))
                                      .as("Property serialize(deserialize(serialize(message))) == serialize(message) "
                                          + "was violated for version %s and verb %s"
                                          + "\n first=%s"
                                          + "\nsecond=%s\n",
                                          versionToHumanReadable(version),
                                          message.header.verb,
                                          // toString methods are not relyable for messages, so use reflection to generate one
                                          new Object() { public String toString() { return CassandraGenerators.toStringRecursive(message); } },
                                          new Object() { public String toString() { return CassandraGenerators.toStringRecursive(read); } })
                                      .isEqualTo(ByteBufferUtil.bytesToHex(first.buffer()));
                        }
                    }));
                }));
            }
        });
    }

    private void withTable(Message<?> message, Consumer<TableMetadata> fn)
    {
        TableMetadata metadata = null;
        if (message.payload instanceof ReadQuery)
        {
            metadata = ((ReadQuery) message.payload).metadata();
        }
        if (metadata != null)
            SchemaIsolator.addTable(metadata);
        try
        {
            fn.accept(metadata);
        }
        finally
        {
            if (metadata != null)
                SchemaIsolator.removeTable(metadata);
        }
    }
}
