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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.PingRequest;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

public final class CassandraGenerators
{
    // utility generators for creating more complex types
    private static final Gen<String> TABLE_NAME_GEN = Generators.regexWord(SourceDSL.integers().between(1, 50));
    private static final Gen<Integer> SMALL_POSSITIVE_SIZE_GEN = SourceDSL.integers().between(1, 30);
    private static final Gen<Boolean> BOOLEAN_GEN = SourceDSL.booleans().all();

    // types
    private static final ImmutableMap<AbstractType<?>, Gen<ByteBuffer>> PRIMITIVE_TYPE_DATA_GENS = ImmutableMap.<AbstractType<?>, Gen<ByteBuffer>>builder()
                                                                                                   .put(BooleanType.instance, BOOLEAN_GEN.map(BooleanType.instance::decompose))
                                                                                                   .put(ByteType.instance, SourceDSL.integers().between(Byte.MIN_VALUE, Byte.MAX_VALUE).map(Integer::byteValue).map(ByteType.instance::decompose))
                                                                                                   .put(ShortType.instance, SourceDSL.integers().between(Short.MIN_VALUE, Short.MAX_VALUE).map(Integer::shortValue).map(ShortType.instance::decompose))
                                                                                                   .put(Int32Type.instance, SourceDSL.integers().allPositive().map(Int32Type.instance::decompose))
                                                                                                   .put(LongType.instance, SourceDSL.longs().all().map(LongType.instance::decompose))
                                                                                                   .put(FloatType.instance, SourceDSL.floats().any().map(FloatType.instance::decompose))
                                                                                                   .put(DoubleType.instance, SourceDSL.doubles().any().map(DoubleType.instance::decompose))
                                                                                                   .put(BytesType.instance, Generators.bytes(0, 1024))
                                                                                                   .put(UUIDType.instance, Generators.UUID_RANDOM_GEN.map(UUIDType.instance::decompose))
                                                                                                   //TODO add the following
                                                                                                   // IntegerType.instance,
                                                                                                   // DecimalType.instance,
                                                                                                   // TimeUUIDType.instance,
                                                                                                   // LexicalUUIDType.instance,
                                                                                                   // InetAddressType.instance,
                                                                                                   // SimpleDateType.instance,
                                                                                                   // TimestampType.instance,
                                                                                                   // TimeType.instance,
                                                                                                   // DurationType.instance,
                                                                                                   // AsciiType.instance,
                                                                                                   // UTF8Type.instance,
                                                                                                   .build();
    public static final Gen<AbstractType<?>> PRIMITIVE_TYPE_GEN = SourceDSL.arbitrary()
                                                                           .pick(
                                                                           Stream.concat(PRIMITIVE_TYPE_DATA_GENS.keySet().stream(),
                                                                                         PRIMITIVE_TYPE_DATA_GENS.keySet().stream().map(ReversedType::getInstance))
                                                                                 .toArray(AbstractType<?>[]::new));
    public static final Gen<AbstractType<?>> TYPE_GEN = PRIMITIVE_TYPE_GEN; //TODO add complex


    private static final Gen<IPartitioner> PARTITIONER_GEN = SourceDSL.arbitrary().pick(Murmur3Partitioner.instance,
                                                                                        ByteOrderedPartitioner.instance,
                                                                                        new LocalPartitioner(TimeUUIDType.instance),
                                                                                        OrderPreservingPartitioner.instance,
                                                                                        RandomPartitioner.instance);


    private static final Gen<TableMetadata.Kind> TABLE_KIND_GEN = SourceDSL.arbitrary().enumValues(TableMetadata.Kind.class);
    public static final Gen<TableMetadata> TABLE_METADATA_GEN = rnd -> createTableMetadata(TABLE_NAME_GEN.generate(rnd), rnd);

    private static final Gen<SinglePartitionReadCommand> SINGLE_PARTITION_READ_COMMAND_GEN = rnd -> {
        TableMetadata metadata = TABLE_METADATA_GEN.generate(rnd);
        int nowInSec = (int) rnd.next(Constraint.between(1, Integer.MAX_VALUE));
        ByteBuffer key = partitionKeyDataGen(metadata).generate(rnd);
        return SinglePartitionReadCommand.create(metadata, nowInSec, key, Slices.ALL);
    };
    private static final Gen<? extends ReadCommand> READ_COMMAND_GEN = Generate.oneOf(SINGLE_PARTITION_READ_COMMAND_GEN);

    // Outbound messages
    private static final Gen<ConnectionType> CONNECTION_TYPE_GEN = SourceDSL.arbitrary().pick(ConnectionType.URGENT_MESSAGES, ConnectionType.SMALL_MESSAGES, ConnectionType.LARGE_MESSAGES);
    public static final Gen<Message<PingRequest>> MESSAGE_PING_GEN = CONNECTION_TYPE_GEN.map(t -> Message.builder(Verb.PING_REQ, PingRequest.get(t)).build());
    public static final Gen<Message<? extends ReadCommand>> MESSAGE_READ_COMMAND_GEN = SINGLE_PARTITION_READ_COMMAND_GEN.map(c -> Message.builder(Verb.READ_REQ, c).build());

    /**
     * Hacky workaround to make sure different generic MessageOut types can be used for {@link #MESSAGE_GEN}.
     */
    private static final Gen<Message<?>> cast(Gen<? extends Message<?>> gen)
    {
        return (Gen<Message<?>>) gen;
    }

    public static final Gen<Message<?>> MESSAGE_GEN = Generate.oneOf(cast(MESSAGE_PING_GEN),
                                                                     cast(MESSAGE_READ_COMMAND_GEN));

    private CassandraGenerators()
    {

    }

    public static TableMetadata createTableMetadata(String ks, RandomnessSource rnd)
    {
        String tableName = TABLE_NAME_GEN.generate(rnd);
        TableMetadata.Builder builder = TableMetadata.builder(ks, tableName, TableId.fromUUID(Generators.UUID_RANDOM_GEN.generate(rnd)))
                                                     .partitioner(PARTITIONER_GEN.generate(rnd))
                                                     .kind(TABLE_KIND_GEN.generate(rnd))
                                                     .isCounter(BOOLEAN_GEN.generate(rnd))
                                                     .params(TableParams.builder().build());

        // generate columns
        // must have a non-zero amount of partition columns, but may have 0 for the rest; SMALL_POSSITIVE_SIZE_GEN won't return 0
        int numPartitionColumns = SMALL_POSSITIVE_SIZE_GEN.generate(rnd);
        int numClusteringColumns = SMALL_POSSITIVE_SIZE_GEN.generate(rnd) - 1;
        int numRegularColumns = SMALL_POSSITIVE_SIZE_GEN.generate(rnd) - 1;
        int numStaticColumns = SMALL_POSSITIVE_SIZE_GEN.generate(rnd) - 1;

        Set<String> createdColumnNames = new HashSet<>();
        for (int i = 0; i < numPartitionColumns; i++)
            builder.addColumn(createColumnDefinition(ks, tableName, ColumnMetadata.Kind.PARTITION_KEY, createdColumnNames, rnd));
        for (int i = 0; i < numClusteringColumns; i++)
            builder.addColumn(createColumnDefinition(ks, tableName, ColumnMetadata.Kind.CLUSTERING, createdColumnNames, rnd));
        for (int i = 0; i < numStaticColumns; i++)
            builder.addColumn(createColumnDefinition(ks, tableName, ColumnMetadata.Kind.STATIC, createdColumnNames, rnd));
        for (int i = 0; i < numRegularColumns; i++)
            builder.addColumn(createColumnDefinition(ks, tableName, ColumnMetadata.Kind.REGULAR, createdColumnNames, rnd));

        return builder.build();
    }

    private static ColumnMetadata createColumnDefinition(String ks, String table,
                                                         ColumnMetadata.Kind kind,
                                                         Set<String> createdColumnNames,
                                                         RandomnessSource rnd)
    {
        return createColumnDefinition(ks, table, kind, TYPE_GEN.generate(rnd), createdColumnNames, rnd);
    }

    private static ColumnMetadata createColumnDefinition(String ks, String table,
                                                         ColumnMetadata.Kind kind, AbstractType<?> type,
                                                         Set<String> createdColumnNames,
                                                         RandomnessSource rnd)
    {
        // filter for unique names
        String str;
        while (!createdColumnNames.add(str = TABLE_NAME_GEN.generate(rnd)))
        {
        }
        ColumnIdentifier name = new ColumnIdentifier(str, true);
        int position = !kind.isPrimaryKeyKind() ? -1 : (int) rnd.next(Constraint.between(0, 30));
        return new ColumnMetadata(ks, table, name, type, position, kind);
    }

    public static Gen<ByteBuffer> partitionKeyDataGen(TableMetadata metadata)
    {
        ImmutableList<ColumnMetadata> columns = metadata.partitionKeyColumns();
        assert !columns.isEmpty() : "Unable to find partition key columns";
        if (columns.size() == 1)
            return typeDataGen(columns.get(0).type);
        List<Gen<ByteBuffer>> columnGens = new ArrayList<>(columns.size());
        for (ColumnMetadata cm : columns)
            columnGens.add(typeDataGen(cm.type));
        return rnd -> {
            ByteBuffer[] buffers = new ByteBuffer[columnGens.size()];
            for (int i = 0; i < columnGens.size(); i++)
                buffers[i] = columnGens.get(i).generate(rnd);
            return CompositeType.build(buffers);
        };
    }

    private static Gen<ByteBuffer> typeDataGen(AbstractType<?> type)
    {
        if (type.isReversed())
            type = ((ReversedType<?>) type).baseType;
        Gen<ByteBuffer> gen = PRIMITIVE_TYPE_DATA_GENS.get(type);
        if (gen == null)
            throw new UnsupportedOperationException("Unsupported type: " + type);
        return gen;
    }
}
