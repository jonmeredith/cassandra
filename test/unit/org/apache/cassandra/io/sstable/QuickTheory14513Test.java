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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import com.google.common.io.Files;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Message;

import static org.apache.cassandra.cql3.CQLTester.cleanupAndLeaveDirs;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.db.marshal.CompositeType.build;
import static org.junit.Assert.assertEquals;

public class QuickTheory14513Test extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(QuickTheory14513Test.class);
    private static final String KS_NAME = "journals";
    private static final String TABLE_NAME = "logs";

    void assertForwardAndReverseIteratorsReturnSame(String baseQuery)
    {
        String forwardQuery = baseQuery + " ORDER BY year ASC";
        String reverseQuery = baseQuery + " ORDER BY year DESC";
        UntypedResultSet forwardResult = executeOnceInternal(forwardQuery);
        UntypedResultSet reverseResult = executeOnceInternal(reverseQuery);
        assertEquals(1, forwardResult.size());
        assertEquals(forwardResult.one().getInt("c"), reverseResult.one().getInt("c"));
    }

    // Create Keyspace and Table
    void createKeyspaceAndTable() throws Throwable
    {
        execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}", KS_NAME));
        execute(String.format("CREATE TABLE %s.%s (user text, year int, month int, day int, title text, body text, PRIMARY KEY ((user), year, month, day, title))", KS_NAME, TABLE_NAME));
    }
    void dropKeyspaceAndTable() throws Throwable
    {
        execute(String.format("DROP KEYSPACE IF EXISTS %s", KS_NAME));
    }

    // Write entries
    void writeEntries() throws Throwable
    {
        String query = String.format("INSERT INTO %s.%s (user, year, month, day, title, body) VALUES (?, ?, ?, ?, ?, ?)",
                              KS_NAME, TABLE_NAME);
        for (int year = 2011; year <= 2018; year++)
        {
            for (int month = 1; month <= 13; month++)
            {
                for (int day = 1; day <= 31; day++)
                {
                    execute(query,"beobal", year, month, day, "title", "Lorem ipsum dolor sit amet");
                }
            }
        }
    }

    // Check entry count
    // Delete ranges of entries
    void deleteEntries() throws Throwable
    {
        String query = String.format("DELETE FROM %s.%s WHERE user = ? AND year = ? AND month = ? AND day = ?",
                                     KS_NAME, TABLE_NAME);
        for (int day = 1; day <= 31; day++)
        {
            execute(query,"beobal", 2018, 1, day);
        }
    }

    @Test
    public void inJVM14513() throws Throwable
    {
        createKeyspaceAndTable();
        writeEntries();
        flush(KS_NAME);
        assertForwardAndReverseIteratorsReturnSame(
        "SELECT COUNT(*) AS c FROM journals.logs WHERE user = 'beobal' AND year < 2018");
        deleteEntries();
        flush(KS_NAME);
        assertForwardAndReverseIteratorsReturnSame(
        "SELECT COUNT(*) AS c FROM journals.logs WHERE user = 'beobal' AND year < 2018");
        assert(true);
    }
}
