/*
 *  Copyright 2023 Curity AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.curity.identityserver.plugin.dynamodb

import io.curity.identityserver.plugin.dynamodb.DynamoDBDatabaseClientDataAccessProvider.DatabaseClientsTable.clientIdFrom
import io.curity.identityserver.plugin.dynamodb.DynamoDBDatabaseClientDataAccessProvider.DatabaseClientsTable.clientIdKeyFor
import io.curity.identityserver.plugin.dynamodb.DynamoDBDatabaseClientDataAccessProvider.DatabaseClientsTable.clientNameKeyFor
import io.curity.identityserver.plugin.dynamodb.DynamoDBDatabaseClientDataAccessProvider.DatabaseClientsTable.splitKeyComponents
import io.curity.identityserver.plugin.dynamodb.DynamoDBDatabaseClientDataAccessProvider.DatabaseClientsTable.tagKeyFor
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class DynamoDBDatabaseClientDataAccessProviderTest {
    @Test
    fun testGetTagKeyFor() {
        assertEquals("profile\\#foo#tag#bar", tagKeyFor("profile#foo", "tag#bar"))
        assertEquals("\\#foo#bar", tagKeyFor("#foo", "bar"))
        assertEquals("foo\\##tag#bar", tagKeyFor("foo#", "tag#bar"))
        assertEquals("profile\\\\#foo#tag#bar", tagKeyFor("profile\\#foo", "tag#bar"))
    }

    @Test
    fun testGetClientIdKeyFor() {
        assertEquals("client\\#foo#tag#bar", clientIdKeyFor("client#foo", "tag#bar"))
        assertEquals("\\#foo#bar", clientIdKeyFor("#foo", "bar"))
        assertEquals("foo\\##tag#bar", clientIdKeyFor("foo#", "tag#bar"))
        assertEquals("client\\\\#foo#tag#bar", clientIdKeyFor("client\\#foo", "tag#bar"))
    }

    @Test
    fun testGetClientNameKeyFor() {
        assertEquals("profile\\#foo#client#bar", clientNameKeyFor("profile#foo", "client#bar"))
        assertEquals("\\#foo#bar", clientNameKeyFor("#foo", "bar"))
        assertEquals("foo\\##client#bar", clientNameKeyFor("foo#", "client#bar"))
        assertEquals("profile\\\\#foo#client#bar", clientNameKeyFor("profile\\#foo", "client#bar"))
    }

    @Test
    fun testGetClientIdFrom() {
        assertEquals("client#foo", clientIdFrom("client\\#foo#tag\\#bar"))
        assertEquals("client\\#foo", clientIdFrom("client\\\\#foo#tag\\#bar"))
        assertEquals("#foo", clientIdFrom("\\#foo#tag\\#bar"))
        assertEquals("client#", clientIdFrom("client\\##tag\\#bar"))
        assertEquals("", clientIdFrom("#tag\\#bar"))
        assertEquals("#", clientIdFrom("\\##tag\\#bar"))
    }

    @Test
    fun testSplitKeyComponents() {
        assertEquals(Pair("foo", "bar"), "foo#bar".splitKeyComponents())
        assertEquals(Pair("key#foo", "subKey\\#bar"), "key\\#foo#subKey\\#bar".splitKeyComponents())
        assertEquals(Pair("key", "foo#subKey#bar"), "key#foo#subKey#bar".splitKeyComponents())
        assertEquals(Pair("", "bar"), "#bar".splitKeyComponents())
        assertEquals(Pair("foo", ""), "foo#".splitKeyComponents())
        assertEquals(Pair("foo", ""), "foo".splitKeyComponents())
        assertEquals(Pair("#foo", ""), "\\#foo".splitKeyComponents())
    }
}