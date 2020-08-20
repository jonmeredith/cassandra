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
import java.util.Random;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.quicktheories.core.Gen;
import org.quicktheories.impl.Constraint;

public final class Generators
{
    private static final Logger logger = LoggerFactory.getLogger(Generators.class);
    private static final char[] REGEX_WORD_DOMAIN = createRegexWordDomain();

    public static final Gen<UUID> UUID_RANDOM_GEN = rnd -> {
        long most = rnd.next(Constraint.none());
        most &= 0x0f << 8; /* clear version        */
        most += 0x40 << 8; /* set to version 4     */
        long least = rnd.next(Constraint.none());
        least &= 0x3fl << 56; /* clear variant        */
        least |= 0x80l << 56;  /* set to IETF variant  */
        return new UUID(most, least);
    };

    private Generators()
    {

    }

    public static Gen<String> regexWord(Gen<Integer> sizes)
    {
        return string(sizes, REGEX_WORD_DOMAIN);
    }

    public static Gen<String> string(Gen<Integer> sizes, char[] domain)
    {
        // note, map is overloaded so String::new is ambugious to javac, so need a lambda here
        return charArray(sizes, domain).map(c -> new String(c));
    }

    public static Gen<char[]> charArray(Gen<Integer> sizes, char[] domain)
    {
        Constraint constraints = Constraint.between(0, domain.length - 1).withNoShrinkPoint();
        Gen<char[]> gen = td -> {
            int size = sizes.generate(td);
            char[] is = new char[size];
            for (int i = 0; i != size; i++)
            {
                int idx = (int) td.next(constraints);
                is[i] = domain[idx];
            }
            return is;
        };
        gen.describedAs(String::new);
        return gen;
    }

    private static char[] createRegexWordDomain()
    {
        // \w == [a-zA-Z_0-9]
        char[] domain = new char[26 * 2 + 10 + 1];

        int offset = 0;
        // _
        domain[offset++] = (char) 95;
        // 0-9
        for (int c = 48; c < 58; c++)
            domain[offset++] = (char) c;
        // A-Z
        for (int c = 65; c < 91; c++)
            domain[offset++] = (char) c;
        // a-z
        for (int c = 97; c < 123; c++)
            domain[offset++] = (char) c;
        return domain;
    }

    public static Gen<ByteBuffer> bytes(int min, int max)
    {
        if (min < 0)
            throw new IllegalArgumentException("Asked for negative bytes; given " + min);
        if (max > LazySharedBlob.SHARED_BYTES.length)
            throw new IllegalArgumentException("Requested bytes larger than shared bytes allowed; " +
                                               "asked for " + max + " but only have " + LazySharedBlob.SHARED_BYTES.length);
        if (max < min)
            throw new IllegalArgumentException("Max was less than min; given min=" + min + " and max=" + max);
        Constraint sizeConstraint = Constraint.between(min, max);
        return rnd -> {
            // since Constraint is immutable and the max was checked, its already proven to be int
            int size = (int) rnd.next(sizeConstraint);
            // to add more randomness, also shift offset in the array so the same size doesn't yield the same bytes
            int offset = (int) rnd.next(Constraint.between(0, LazySharedBlob.SHARED_BYTES.length - size));

            return ByteBuffer.wrap(LazySharedBlob.SHARED_BYTES, offset, size);
        };
    }

    private static final class LazySharedBlob
    {
        private static final byte[] SHARED_BYTES;

        static
        {
            int maxBlobLength = 1 * 1024 * 1024;
            long blobSeed = Long.parseLong(System.getProperty("cassandra.test.blob.shared.seed", Long.toString(System.currentTimeMillis())));
            logger.info("Shared blob Gen used seed {}", blobSeed);

            Random random = new Random(blobSeed);
            byte[] bytes = new byte[maxBlobLength];
            random.nextBytes(bytes);

            SHARED_BYTES = bytes;
        }
    }
}
