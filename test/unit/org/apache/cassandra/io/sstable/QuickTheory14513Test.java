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

package org.apache.cassandra.io.sstable;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.quicktheories.WithQuickTheories;
import org.quicktheories.impl.stateful.StatefulTheory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class QuickTheory14513Test implements WithQuickTheories
{
    private static final Logger logger = LoggerFactory.getLogger(QuickTheory14513Test.class);

    public static class SUT extends CQLTester
    {
        private String tableName;

        // NB not adding annotation to automatically call here
        @Override
        public void beforeTest()
        {
            try
            {
                super.beforeTest();
            }
            catch (Throwable t)
            {
                fail(t.getMessage());
            }
        }

        // NB not adding annotation to automatically call after test here
        @Override
        public void afterTest()
        {
            try
            {
                super.afterTest();
            }
            catch (Throwable t)
            {
                fail(t.getMessage());
            }
        }

        public UntypedResultSet execute2(String query, Object... values)
        {
            UntypedResultSet result = null;
            try
            {
                result = execute(query, values);
            }
            catch (Throwable t)
            {
                fail(t.getMessage());
            }
            return result;
        }

        void assertCount(long expected, String query)
        {

            logger.info(String.format("Checking for expected count of %d for %s", expected, query));
            UntypedResultSet result = execute2(query);
            assertRows(result, row(expected));
        }

        void assertForwardAndReverseIteratorsReturnSame(String baseQuery)
        {
            String forwardQuery = baseQuery + " ORDER BY year ASC";
            String reverseQuery = baseQuery + " ORDER BY year DESC";
            UntypedResultSet forwardResult = execute2(forwardQuery);
            UntypedResultSet reverseResult = execute2(reverseQuery);
            assertEquals(1, forwardResult.size());
            logger.info(String.format("%s query forward result: %d reverse result: %d",
                                      baseQuery,
                                      forwardResult.one().getLong("c"),
                                      reverseResult.one().getLong("c")));
            assertEquals(String.format("Query '%s' returns same results in ascending and descending order", baseQuery),
                         forwardResult.one().getLong("c"), reverseResult.one().getLong("c"));
        }

        // Create Keyspace and Table
        void prepareTable()
        {
            tableName = createTable("CREATE TABLE %s (user text, year int, month int, day int, title text, body text, PRIMARY KEY ((user), year, month, day, title))");
            logger.info(String.format("Created table %s", tableName));
        }

        // Write entries
        void writeEntries()
        {
            String query = "INSERT INTO %s (user, year, month, day, title, body) VALUES (?, ?, ?, ?, ?, ?)";
            for (int year = 2011; year < 2018; year++)
            {
                for (int month = 1; month < 13; month++)
                {
                    for (int day = 1; day < 31; day++)
                    {
                        execute2(query, "beobal", year, month, day, "title", "Lorem ipsum dolor sit amet");
                    }
                }
            }
        }

        // Check entry count
        // Delete ranges of entries
        void deleteEntries()
        {
            StringBuilder sb = new StringBuilder();

            sb.append("BEGIN UNLOGGED BATCH\n");
            for (int day = 1; day < 31; day++)
            {
                sb.append(String.format("DELETE FROM %s.%s WHERE user = 'beobal' AND year = %d AND month = %d AND day = %d;\n",
                                        KEYSPACE, tableName, 2018, 1, day));
            }
            sb.append("APPLY BATCH");
            String query = sb.toString();
            execute2(query);
        }

        @Override
        public void flush()
        {
            super.flush();
        }
    }

    @Test
    public void inJVM14513()
    {
        DatabaseDescriptor.setColumnIndexSizeInKB(1);
        assertEquals(1024, DatabaseDescriptor.getColumnIndexSize());
        SUT sut = new SUT();
        sut.beforeTest();
        try
        {
            sut.prepareTable();
            sut.writeEntries();
            sut.flush();
            long expectedRows = 7 * 12 * 30;
            sut.assertCount(expectedRows, "SELECT COUNT(*) FROM %s WHERE user = 'beobal' AND year < 2018");
            sut.assertForwardAndReverseIteratorsReturnSame(
            "SELECT COUNT(*) AS c FROM %s WHERE user = 'beobal' AND year < 2018");
            sut.deleteEntries();
            sut.flush();
            sut.assertForwardAndReverseIteratorsReturnSame(
            "SELECT COUNT(*) AS c FROM %s WHERE user = 'beobal' AND year < 2018");
        }
        finally
        {
            sut.afterTest();
        }
    }


    @Test
    public void qtTest()
    {
        CQLTester.setUpClass();
        try
        {
            DatabaseDescriptor.setColumnIndexSizeInKB(1);
            assertEquals(1024, DatabaseDescriptor.getColumnIndexSize());
            qt().withExamples(1000).stateful(() -> new Model());
        }
        finally
        {
            CQLTester.tearDownClass();
        }
    }

    public static class Model extends StatefulTheory.StepBased
    {
        private SUT sut = null;

        public boolean isSetup()
        {
            return sut != null;
        }

        public void setup()
        {
            sut = new SUT();
            try
            {
                sut.beforeTest();
            }
            catch (Throwable t)
            {
                fail(t.toString());
            }
            sut.prepareTable();
            sut.flush(); // Not removing keyspace now, so make sure flushed
        }

        public void writeEntries()
        {
            sut.writeEntries();
        }

        public void deleteEntries()
        {
            sut.deleteEntries();
        }

        public void flush()
        {
            sut.flush();
        }

        public void compareCount()
        {
            sut.assertForwardAndReverseIteratorsReturnSame(
            "SELECT COUNT(*) AS c FROM %s WHERE user = 'beobal' AND year < 2018");
        }

        public void checkCount(long expectedRows)
        {
            sut.assertCount(expectedRows, "SELECT COUNT(*) AS c FROM %s WHERE user = 'beobal' AND year < 2018");
        }

        public void guaranteedGlory()
        {
            checkCount(0L);
            writeEntries();
            flush();
            long expectedRows = 7 * 12 * 30;
            checkCount(expectedRows);
            deleteEntries();
            flush();
            checkCount(expectedRows);
            compareCount();
            logger.info("booo, made it to the end");
        }

        protected void initSteps()
        {
            addStep(step("setup", () -> !this.isSetup(), this::setup, null));
            addStep(step("guaranteedGlory", () -> this.isSetup(), this::guaranteedGlory, null));
//            addStep(step("writeEntries", () -> this.isSetup(), this::writeEntries, null));
//            addStep(step("deleteEntries", () -> this.isSetup(), this::deleteEntries, null));
//            addStep(step("flush", () -> this.isSetup(), this::flush, null));
//            addStep(step("compareCount", () -> this.isSetup(), this::compareCount, null));
        }

        @Override
        public void teardown()
        {
            sut.afterTest();
        }
    }
}