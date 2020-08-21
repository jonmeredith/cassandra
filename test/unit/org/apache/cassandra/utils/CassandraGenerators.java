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

import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SchemaCQLHelper;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
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
    private static final Gen<Integer> VERY_SMALL_POSSITIVE_SIZE_GEN = SourceDSL.integers().between(1, 3);
    private static final Gen<Integer> SMALL_POSSITIVE_SIZE_GEN = SourceDSL.integers().between(1, 30);
    private static final Gen<Boolean> BOOLEAN_GEN = SourceDSL.booleans().all();

    // types

    private static final Map<AbstractType<?>, TypeSupport> PRIMITIVE_TYPE_DATA_GENS =
    Arrays.asList(TypeSupport.of(BooleanType.instance, BOOLEAN_GEN),
                  TypeSupport.of(ByteType.instance, SourceDSL.integers().between(Byte.MIN_VALUE, Byte.MAX_VALUE).map(Integer::byteValue)),
                  TypeSupport.of(ShortType.instance, SourceDSL.integers().between(Short.MIN_VALUE, Short.MAX_VALUE).map(Integer::shortValue)),
                  TypeSupport.of(Int32Type.instance, SourceDSL.integers().allPositive()),
                  TypeSupport.of(LongType.instance, SourceDSL.longs().all()),
                  TypeSupport.of(FloatType.instance, SourceDSL.floats().any()),
                  TypeSupport.of(DoubleType.instance, SourceDSL.doubles().any()),
                  TypeSupport.of(BytesType.instance, Generators.bytes(1, 1024)),
                  TypeSupport.of(UUIDType.instance, Generators.UUID_RANDOM_GEN),
                  TypeSupport.of(InetAddressType.instance, Generators.INET_ADDRESS_GEN),
                  TypeSupport.of(AsciiType.instance, SourceDSL.strings().ascii().ofLengthBetween(1, 1024)),
                  TypeSupport.of(UTF8Type.instance, Generators.UTF_8_GEN),
                  TypeSupport.of(TimestampType.instance, Generators.DATE_GEN)
                  //TODO add the following
                  // IntegerType.instance,
                  // DecimalType.instance,
                  // TimeUUIDType.instance,
                  // LexicalUUIDType.instance,
                  // SimpleDateType.instance,
                  // TimeType.instance,
                  // DurationType.instance,
    ).stream().collect(Collectors.toMap(t -> t.type, t -> t));
    public static final Gen<AbstractType<?>> PRIMITIVE_TYPE_GEN = SourceDSL.arbitrary()
                                                                           .pick(
                                                                           Stream.concat(PRIMITIVE_TYPE_DATA_GENS.keySet().stream(),
                                                                                         PRIMITIVE_TYPE_DATA_GENS.keySet().stream().map(ReversedType::getInstance))
                                                                                 .toArray(AbstractType<?>[]::new));
//    public static final Gen<AbstractType<?>> TYPE_GEN = PRIMITIVE_TYPE_GEN; //TODO add complex


    private static final Gen<IPartitioner> PARTITIONER_GEN = SourceDSL.arbitrary().pick(Murmur3Partitioner.instance,
                                                                                        ByteOrderedPartitioner.instance,
                                                                                        new LocalPartitioner(TimeUUIDType.instance),
                                                                                        OrderPreservingPartitioner.instance,
                                                                                        RandomPartitioner.instance);


    private static final Gen<TableMetadata.Kind> TABLE_KIND_GEN = SourceDSL.arbitrary().pick(TableMetadata.Kind.REGULAR, TableMetadata.Kind.INDEX, TableMetadata.Kind.VIRTUAL);
    public static final Gen<TableMetadata> TABLE_METADATA_GEN = gen(rnd -> createTableMetadata(TABLE_NAME_GEN.generate(rnd), rnd)).describedAs(CassandraGenerators::toStringRecursive);

    private static final Gen<SinglePartitionReadCommand> SINGLE_PARTITION_READ_COMMAND_GEN = gen(rnd -> {
        TableMetadata metadata = TABLE_METADATA_GEN.generate(rnd);
        int nowInSec = (int) rnd.next(Constraint.between(1, Integer.MAX_VALUE));
        ByteBuffer key = partitionKeyDataGen(metadata).generate(rnd);
        return SinglePartitionReadCommand.create(metadata, nowInSec, key, Slices.ALL);
    }).describedAs(CassandraGenerators::toStringRecursive);
    private static final Gen<? extends ReadCommand> READ_COMMAND_GEN = Generate.oneOf(SINGLE_PARTITION_READ_COMMAND_GEN)
                                                                               .describedAs(CassandraGenerators::toStringRecursive);

    // Outbound messages
    private static final Gen<ConnectionType> CONNECTION_TYPE_GEN = SourceDSL.arbitrary().pick(ConnectionType.URGENT_MESSAGES, ConnectionType.SMALL_MESSAGES, ConnectionType.LARGE_MESSAGES);
    public static final Gen<Message<PingRequest>> MESSAGE_PING_GEN = CONNECTION_TYPE_GEN
                                                                     .map(t -> Message.builder(Verb.PING_REQ, PingRequest.get(t)).build())
                                                                     .describedAs(CassandraGenerators::toStringRecursive);
    public static final Gen<Message<? extends ReadCommand>> MESSAGE_READ_COMMAND_GEN = READ_COMMAND_GEN
                                                                                       .<Message<? extends ReadCommand>>map(c -> Message.builder(Verb.READ_REQ, c).build())
                                                                                       .describedAs(CassandraGenerators::toStringRecursive);

    public static final Gen<Message<?>> MESSAGE_GEN = Generate.oneOf(cast(MESSAGE_PING_GEN),
                                                                     cast(MESSAGE_READ_COMMAND_GEN))
                                                              .describedAs(CassandraGenerators::toStringRecursive);

    private CassandraGenerators()
    {

    }

    private enum TypeKind
    {PRIMITIVE, SET, LIST, MAP, TUPLE, STRUCT}

    private static final Gen<TypeKind> TYPE_KIND_GEN = SourceDSL.arbitrary().enumValuesWithNoOrder(TypeKind.class);

    public static Gen<AbstractType<?>> getType()
    {
        return getType(3);
    }

    public static Gen<AbstractType<?>> getType(int maxDepth)
    {
        assert maxDepth >= 0 : "max depth must be positive or zero; given " + maxDepth;
        boolean atBottom = maxDepth == 0;
        return rnd -> {
            // figure out type to get
            TypeKind kind = TYPE_KIND_GEN.generate(rnd);
            switch (kind)
            {
                case PRIMITIVE:
                    return PRIMITIVE_TYPE_GEN.generate(rnd);
                case SET:
                {
                    boolean multiCell = BOOLEAN_GEN.generate(rnd);
                    AbstractType<?> elements = atBottom ? PRIMITIVE_TYPE_GEN.generate(rnd) : getType(maxDepth - 1).generate(rnd);
                    return SetType.getInstance(elements, multiCell);
                }
                case LIST:
                {
                    boolean multiCell = BOOLEAN_GEN.generate(rnd);
                    AbstractType<?> elements = atBottom ? PRIMITIVE_TYPE_GEN.generate(rnd) : getType(maxDepth - 1).generate(rnd);
                    return ListType.getInstance(elements, multiCell);
                }
                case MAP:
                {
                    boolean multiCell = BOOLEAN_GEN.generate(rnd);
                    AbstractType<?> key = atBottom ? PRIMITIVE_TYPE_GEN.generate(rnd) : getType(maxDepth - 1).generate(rnd);
                    AbstractType<?> value = atBottom ? PRIMITIVE_TYPE_GEN.generate(rnd) : getType(maxDepth - 1).generate(rnd);
                    return MapType.getInstance(key, value, multiCell);
                }
                case TUPLE:
                {
                    int numElements = VERY_SMALL_POSSITIVE_SIZE_GEN.generate(rnd);
                    List<AbstractType<?>> elements = new ArrayList<>(numElements);
                    Gen<AbstractType<?>> elementGen = atBottom ? PRIMITIVE_TYPE_GEN : getType(maxDepth - 1);
                    for (int i = 0; i < numElements; i++)
                        elements.add(elementGen.generate(rnd));
                    return new TupleType(elements);
                }
                case STRUCT:
                {
                    boolean multiCell = BOOLEAN_GEN.generate(rnd);
                    int numElements = VERY_SMALL_POSSITIVE_SIZE_GEN.generate(rnd);
                    List<AbstractType<?>> fieldTypes = new ArrayList<>(numElements);
                    List<FieldIdentifier> fieldNames = new ArrayList<>(numElements);
                    Gen<AbstractType<?>> elementGen = atBottom ? PRIMITIVE_TYPE_GEN : getType(maxDepth - 1);
                    String ks = TABLE_NAME_GEN.generate(rnd);
                    ByteBuffer name = AsciiType.instance.decompose(TABLE_NAME_GEN.generate(rnd));

                    for (int i = 0; i < numElements; i++)
                    {
                        fieldTypes.add(elementGen.generate(rnd));
                        fieldNames.add(FieldIdentifier.forQuoted(TABLE_NAME_GEN.generate(rnd)));
                    }
                    return new UserType(ks, name, fieldNames, fieldTypes, multiCell);
                }
                default:
                    throw new IllegalArgumentException("Unknown kind: " + kind);
            }
        };
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
        return createColumnDefinition(ks, table, kind, getType().generate(rnd), createdColumnNames, rnd);
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
            return getTypeSupport(columns.get(0).type).bytesGen();
        List<Gen<ByteBuffer>> columnGens = new ArrayList<>(columns.size());
        for (ColumnMetadata cm : columns)
            columnGens.add(getTypeSupport(cm.type).bytesGen());
        return rnd -> {
            ByteBuffer[] buffers = new ByteBuffer[columnGens.size()];
            for (int i = 0; i < columnGens.size(); i++)
                buffers[i] = columnGens.get(i).generate(rnd);
            return CompositeType.build(buffers);
        };
    }

    public static TypeSupport getTypeSupport(AbstractType<?> type)
    {
        // this doesn't affect the data, only sort order, so drop it
        if (type.isReversed())
            type = ((ReversedType<?>) type).baseType;
        TypeSupport gen = PRIMITIVE_TYPE_DATA_GENS.get(type);
        if (gen != null)
            return gen;
        // might be... complex...
        if (type instanceof SetType)
        {
            SetType setType = (SetType) type;
            TypeSupport elementSupport = getTypeSupport(setType.getElementsType());
            return TypeSupport.of(setType, rnd -> {
                int size = VERY_SMALL_POSSITIVE_SIZE_GEN.generate(rnd);
                HashSet<Object> set = Sets.newHashSetWithExpectedSize(size);
                for (int i = 0; i < size; i++)
                    set.add(elementSupport.valueGen.generate(rnd));
                return set;
            });
        }
        else if (type instanceof ListType)
        {
            ListType listType = (ListType) type;
            TypeSupport elementSupport = getTypeSupport(listType.getElementsType());
            return TypeSupport.of(listType, rnd -> {
                int size = VERY_SMALL_POSSITIVE_SIZE_GEN.generate(rnd);
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++)
                    list.add(elementSupport.valueGen.generate(rnd));
                return list;
            });
        }
        else if (type instanceof MapType)
        {
            MapType mapType = (MapType) type;
            TypeSupport keySupport = getTypeSupport(mapType.getKeysType());
            TypeSupport valueSupport = getTypeSupport(mapType.getValuesType());
            return TypeSupport.of(mapType, rnd -> {
                int size = VERY_SMALL_POSSITIVE_SIZE_GEN.generate(rnd);
                Map<Object, Object> map = Maps.newHashMapWithExpectedSize(size);
                // if there is conflict thats fine
                for (int i = 0; i < size; i++)
                    map.put(keySupport.valueGen.generate(rnd), valueSupport.valueGen.generate(rnd));
                return map;
            });
        }
        else if (type instanceof TupleType) // includes UserType
        {
            TupleType tupleType = (TupleType) type;
            List<TypeSupport> elementsSupport = tupleType.allTypes().stream().map(CassandraGenerators::getTypeSupport).collect(Collectors.toList());
            return TypeSupport.of(tupleType, rnd -> {
                ByteBuffer[] elements = new ByteBuffer[elementsSupport.size()];
                for (int i = 0; i < elementsSupport.size(); i++)
                    elements[i] = elementsSupport.get(i).toBytes.apply(elementsSupport.get(i).valueGen.generate(rnd));
                return TupleType.buildValue(elements);
            }, i -> i);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    public static final class TypeSupport
    {
        public final AbstractType<Object> type;
        public final Gen<Object> valueGen;
        public final Function<Object, ByteBuffer> toBytes;

        public TypeSupport(AbstractType<? extends Object> type, Gen<? extends Object> valueGen)
        {
            this.type = (AbstractType<Object>) Objects.requireNonNull(type);
            this.valueGen = (Gen<Object>) Objects.requireNonNull(valueGen);
            this.toBytes = this.type::decompose;
        }

        public TypeSupport(AbstractType<? extends Object> type, Gen<? extends Object> valueGen, Function<Object, ByteBuffer> toBytes)
        {
            this.type = (AbstractType<Object>) Objects.requireNonNull(type);
            this.valueGen = (Gen<Object>) Objects.requireNonNull(valueGen);
            this.toBytes = Objects.requireNonNull(toBytes);
        }

        public static <T> TypeSupport of(AbstractType<T> type, Gen<T> valueGen)
        {
            return new TypeSupport(type, valueGen);
        }

        public static <T> TypeSupport of(AbstractType<?> type, Gen<T> valueGen, Function<? extends T, ByteBuffer> toBytes)
        {
            return new TypeSupport(type, valueGen, (Function<Object, ByteBuffer>) toBytes);
        }

        public Gen<ByteBuffer> bytesGen()
        {
            return rnd -> toBytes.apply(valueGen.generate(rnd));
        }

        public String toString()
        {
            return "TypeSupport{" +
                   "type=" + type +
                   '}';
        }
    }

    /**
     * Hacky workaround to make sure different generic MessageOut types can be used for {@link #MESSAGE_GEN}.
     */
    private static final Gen<Message<?>> cast(Gen<? extends Message<?>> gen)
    {
        return (Gen<Message<?>>) gen;
    }

    /**
     * Java's type inferrence with chaining doesn't work well, so this is used to inferr the root type early in cases
     * where javac can't figure it out
     */
    private static <T> Gen<T> gen(Gen<T> fn)
    {
        return fn;
    }

    private static String toStringRecursive(Object o)
    {
        return ReflectionToStringBuilder.toString(o, new MultilineRecursiveToStringStyle()
        {
            private String spacer = "";

            {
                setArrayStart("[");
                setArrayEnd("]");
                setContentStart("{");
                setContentEnd("}");
                setUseIdentityHashCode(false);
                setUseShortClassName(true);
            }

            protected boolean accept(Class<?> clazz)
            {
                return !clazz.isEnum()
                       && Stream.of(clazz.getDeclaredFields()).filter(f -> !Modifier.isStatic(f.getModifiers())).findAny().isPresent();
            }

            public void appendDetail(StringBuffer buffer, String fieldName, Object value)
            {
                if (value instanceof ByteBuffer)
                {
                    value = ByteBufferUtil.bytesToHex((ByteBuffer) value);
                }
                else if (value instanceof Token || value instanceof InetAddressAndPort)
                {
                    value = value.toString();
                }
                else if (value instanceof TableMetadata)
                {
                    String cql = SchemaCQLHelper.getTableMetadataAsCQL((TableMetadata) value, true, true, false);
                    cql = cql.replace("\n", "\n  " + spacer);
                    cql = "\n  " + spacer + cql;
                    value = cql;
                }
                super.appendDetail(buffer, fieldName, value);
            }

            // MultilineRecursiveToStringStyle doesn't look at what was set and instead hard codes the values when it "resets" the level
            protected void setArrayStart(String arrayStart)
            {
                super.setArrayStart(arrayStart.replace("{", "["));
            }

            protected void setArrayEnd(String arrayEnd)
            {
                super.setArrayEnd(arrayEnd.replace("}", "]"));
            }

            protected void setContentStart(String contentStart)
            {
                // use this to infer the spacer since it isn't exposed.
                String[] split = contentStart.split("\n", 2);
                spacer = split.length == 2 ? split[1] : "";
                super.setContentStart(contentStart.replace("[", "{"));
            }

            protected void setContentEnd(String contentEnd)
            {
                super.setContentEnd(contentEnd.replace("]", "}"));
            }
        }, true);
    }
}
