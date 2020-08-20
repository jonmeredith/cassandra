package org.apache.cassandra.utils;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URLClassLoader;

import org.apache.cassandra.distributed.shared.Versions;

/**
 * Isolator is used to isolation a section of the code to allow mocking.  These classes normally rely on class loader
 * for isolation and will attempt to run the provided code in this class loader.
 *
 * Usage:
 * <ul>
 *     <li>{@link #runIn(Execute)} will run a runnable in the class loader</li>
 * </ul>
 */
public interface Isolator
{
    default void runIn(Execute execute) throws Exception
    {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos))
        {
            oos.writeObject(execute);
            oos.close();
            transferAndRunExecute(baos.toByteArray());
        }
    }

    void transferAndRunExecute(byte[] bytes) throws Exception;

    @FunctionalInterface
    public interface Execute extends Serializable
    {
        public void execute() throws Exception;
    }

    static final class IsolatorClassLoader extends URLClassLoader
    {
        public IsolatorClassLoader()
        {
            super(Versions.getClassPath(), null);
        }

        public Class<?> loadClass(String name) throws ClassNotFoundException
        {
            switch (name)
            {
                case "org.apache.cassandra.utils.Isolator":
                    return Thread.currentThread().getContextClassLoader().loadClass(name);
                default:
                    return super.loadClass(name);
            }
        }
    }
}
