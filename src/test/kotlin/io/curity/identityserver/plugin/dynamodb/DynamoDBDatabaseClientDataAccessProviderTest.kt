/*
 * Copyright (C) 2023 Curity AB. All rights reserved.
 *
 * The contents of this file are the property of Curity AB.
 * You may not copy or use this file, in either source code
 * or executable form, except in compliance with terms
 * set by Curity AB.
 *
 * For further information, please contact Curity AB.
 */

package io.curity.identityserver.plugin.dynamodb

import io.curity.identityserver.plugin.dynamodb.DynamoDBDatabaseClientDataAccessProvider.DatabaseClientsTable.clientIdFrom
import io.curity.identityserver.plugin.dynamodb.DynamoDBDatabaseClientDataAccessProvider.DatabaseClientsTable.clientIdKeyFor
import io.curity.identityserver.plugin.dynamodb.DynamoDBDatabaseClientDataAccessProvider.DatabaseClientsTable.clientNameKeyFor
import io.curity.identityserver.plugin.dynamodb.DynamoDBDatabaseClientDataAccessProvider.DatabaseClientsTable.escape
import io.curity.identityserver.plugin.dynamodb.DynamoDBDatabaseClientDataAccessProvider.DatabaseClientsTable.tagKeyFor
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class DynamoDBDatabaseClientDataAccessProviderTest {

    @Test
    fun testEscapeSeparator() {
        assertEquals("first\\#part", "first#part".escape())
    }

    @Test
    fun testGetTagKeyFor() {
        assertEquals("profile\\#foo#tag\\#bar", tagKeyFor("profile#foo", "tag#bar"))
    }

    @Test
    fun testGetClientIdKeyFor() {
        assertEquals("client\\#foo#tag\\#bar", clientIdKeyFor("client#foo", "tag#bar"))
    }

    @Test
    fun testGetClientIdFrom() {
        assertEquals("client#foo", clientIdFrom("client\\#foo#tag\\#bar"))
    }

    @Test
    fun testGetClientNameKeyFor() {
        assertEquals("profile\\#foo#client\\#bar", clientNameKeyFor("profile#foo", "client#bar"))
    }
}