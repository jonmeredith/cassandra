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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.assertEquals;

public class QuickTheory14513Test extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(QuickTheory14513Test.class);
    private static final String TABLE_NAME = "logs";
    private String tableName;

    void assertForwardAndReverseIteratorsReturnSame(String baseQuery) throws Throwable
    {
        String forwardQuery = baseQuery + " ORDER BY year ASC";
        String reverseQuery = baseQuery + " ORDER BY year DESC";
        UntypedResultSet forwardResult = execute(forwardQuery);
        UntypedResultSet reverseResult = execute(reverseQuery);
        assertEquals(1, forwardResult.size());
        logger.info(String.format("%s query result: %d", forwardQuery, forwardResult.one().getLong("c")));
        assertEquals(forwardResult.one().getLong("c"), reverseResult.one().getLong("c"));
    }

    // Create Keyspace and Table
    void prepareTable() throws Throwable
    {
        tableName = createTable("CREATE TABLE %s (user text, year int, month int, day int, title text, body text, PRIMARY KEY ((user), year, month, day, title))");
        logger.info(String.format("Created table %s", tableName));

    }

    // Write entries
    void writeEntries() throws Throwable
    {
        String query = "INSERT INTO %s (user, year, month, day, title, body) VALUES (?, ?, ?, ?, ?, ?)";
        for (int year = 2011; year < 2018; year++)
        {
            for (int month = 1; month < 13; month++)
            {
                for (int day = 1; day < 31; day++)
                {
                    execute(query, "beobal", year, month, day, "title", "Lorem ipsum dolor sit amet");
                }
            }
        }
    }

    // Check entry count
    // Delete ranges of entries
    void deleteEntries() throws Throwable
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
        executeInternal(query);
    }

    @Test
    public void inJVM14513() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexSizeInKB(1);
        assertEquals(1024, DatabaseDescriptor.getColumnIndexSize());
        prepareTable();
        writeEntries();
        flush();
        long expectedRows = 7 * 12 * 30;
        assertRows(execute("SELECT COUNT(*) FROM %s WHERE user = 'beobal' AND year < 2018"), row(expectedRows));
        assertForwardAndReverseIteratorsReturnSame(
        "SELECT COUNT(*) AS c FROM %s WHERE user = 'beobal' AND year < 2018");
        deleteEntries();
        flush();
        assertForwardAndReverseIteratorsReturnSame(
        "SELECT COUNT(*) AS c FROM %s WHERE user = 'beobal' AND year < 2018");
        assert(true);
    }
}
