package org.apache.cassandra.utils;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * Mocks out some of the methods on {@link Schema} to be easier for tests to manipulate.
 *
 * @see #addTable(TableMetadata)
 * @see #removeTable(TableMetadata)
 */
public final class SchemaIsolator implements Isolator
{
    private static final ConcurrentMap<TableId, TableMetadata> TABLES = new ConcurrentHashMap<>();

    private SchemaIsolator()
    {

    }

    public static Isolator create()
    {
        IsolatorClassLoader cl = new IsolatorClassLoader();

        // redefine Schema
        new ByteBuddy().redefine(Schema.class)
                       .method(named("getExistingTableMetadata"))
                       .intercept(MethodDelegation.to(SchemaIsolator.class))
                       .make()
                       .load(cl, ClassLoadingStrategy.Default.INJECTION);

        // load in class loader
        try
        {
            Class<?> klass = cl.loadClass("org.apache.cassandra.utils.SchemaIsolator");
            Constructor<?> c = klass.getDeclaredConstructor(new Class[0]);
            c.setAccessible(true);
            return (Isolator) c.newInstance(new Object[0]);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void checkState()
    {
        if (!Lazy.isLoaded())
            throw new IllegalStateException("Unable to redefine Schema");
    }

    private <T> T load(byte[] bytes) throws Exception
    {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bais))
        {
            return (T) ois.readObject();
        }
    }

    public void transferAndRunExecute(byte[] bytes) throws Exception
    {
        checkState();
        Execute execute = load(bytes);
        execute.execute();
    }

    /**
     * Used by ByteBuddy when ever {@link Schema#getExistingTableMetadata(TableId)} is called
     */
    @UsedByByteBuddy("org.apache.cassandra.schema.Schema")
    public static TableMetadata getExistingTableMetadata(TableId id) throws UnknownTableException
    {
        TableMetadata meta = TABLES.get(id);
        if (meta == null)
            throw new UnknownTableException("Unable to find table " + id + "; known tables " + TABLES.keySet(), id);
        return TABLES.get(id);
    }

    public static void addTable(TableMetadata metadata)
    {
        TABLES.put(metadata.id, metadata);
    }

    public static void removeTable(TableMetadata metadata)
    {
        TABLES.remove(metadata.id);
    }

    private static final class Lazy {
        static
        {
            DatabaseDescriptor.daemonInitialization();
            DatabaseDescriptor.setCrossNodeTimeout(true);
        }

        private static boolean isLoaded()
        {
            return true;
        }
    }
}
