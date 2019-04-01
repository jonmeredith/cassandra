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
package org.apache.cassandra.cql3.functions;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.junit.Test;

public class CustomFctsTest// extends CQLTester
{

    public static class MissingAllMethod
    {
    };

    public static final class InaccessibleAllMethod
    {
        private static Collection<Function> all()
        {
            return null;
        }
    };

    public static final class InstanceAllMethod
    {
        public Collection<Function> all()
        {
            return null;
        }
    };

    private static Function mockFunction = new NativeScalarFunction("dummy", null)
    {

        @Override
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
                throws InvalidRequestException
        {
            return null;
        }
    };

    public static final class CorrectAllMethod
    {
        public static Collection<Function> all()
        {
            return Collections.singleton(mockFunction);
        }
    };

    private static void testPattern(String clazz, String message)
    {
        try
        {
            CustomFcts.loadCustomFunctions(clazz);
            fail("Didn't throw correct exception for bad configuration!");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage().startsWith(message));
        }
        catch (Exception e)
        {
            fail("Didn't throw correct exception for bad configuration!");
        }
    }

    @Test
    public void missingAllMethod()
    {
        testPattern("org.apache.cassandra.cql3.functions.CustomFctsTest$MissingAllMethod", "Inaccessible all()");
    }

    @Test
    public void instanceAllMethod()
    {
        testPattern("org.apache.cassandra.cql3.functions.CustomFctsTest$InstanceAllMethod", "Non-static all()");
    }

    @Test
    public void privateAllMethod()
    {
        testPattern("org.apache.cassandra.cql3.functions.CustomFctsTest$InaccessibleAllMethod", "Inaccessible all()");
    }

    @Test
    public void correctAllMethod()
    {
        CustomFcts.loadCustomFunctions("org.apache.cassandra.cql3.functions.CustomFctsTest$CorrectAllMethod");
    }
}
