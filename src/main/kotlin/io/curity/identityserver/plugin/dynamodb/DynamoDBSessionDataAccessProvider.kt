/*
 *  Copyright 2020 Curity AB
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

import io.curity.identityserver.plugin.dynamodb.token.DynamoDBTokenDataAccessProvider
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.data.Session
import se.curity.identityserver.sdk.datasource.SessionDataAccessProvider
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant

class DynamoDBSessionDataAccessProvider(private val dynamoDBClient: DynamoDBClient) : SessionDataAccessProvider
{
    object SessionTable : Table("curity-sessions")
    {
        val id = StringAttribute("id")
        val data = StringAttribute("sessionData")
        val expiresAt = NumberLongAttribute("expiresAt")

        fun key(id: String) = mapOf(this.id.toNameValuePair(id))
    }

    override fun getSessionById(id: String): Session?
    {
        val request = GetItemRequest.builder()
            .tableName(SessionTable.name)
            .key(SessionTable.key(id))
            .build()

        val response = dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty())
        {
            return null
        }

        val item = response.item()

        return Session(
            SessionTable.id.from(item),
            Instant.ofEpochSecond(SessionTable.expiresAt.from(item)),
            SessionTable.data.from(item)
        )
    }

    override fun insertSession(session: Session)
    {
        val item = mapOf(
            SessionTable.id.toNameValuePair(session.id),
            SessionTable.expiresAt.toNameValuePair(session.expiresAt.epochSecond),
            SessionTable.data.toNameValuePair(session.data)
        )

        val request = PutItemRequest.builder()
            .tableName(SessionTable.name)
            .item(item)
            .build()

        dynamoDBClient.putItem(request)
    }

    override fun updateSession(updatedSession: Session)
    {
        val request = UpdateItemRequest.builder()
            .tableName(SessionTable.name)
            .key(SessionTable.key(updatedSession.id))
            .conditionExpression("attribute_exists(${SessionTable.id})")
            .updateExpression(
                "SET ${SessionTable.data} = ${SessionTable.data.colonName}," +
                        " ${SessionTable.expiresAt} = ${SessionTable.expiresAt.colonName}"
            )
            .expressionAttributeValues(
                mapOf(
                    SessionTable.expiresAt.toExpressionNameValuePair(updatedSession.expiresAt.epochSecond),
                    SessionTable.data.toExpressionNameValuePair(updatedSession.data)
                )
            )
            .build()

        try
        {
            dynamoDBClient.updateItem(request)
        } catch (_: ConditionalCheckFailedException)
        {
            // this exceptions means the entry does not exists, which isn't an error
            _logger.debug("updateSession on a nonexistent session")
        }
    }

    override fun updateSessionExpiration(id: String, expiresAt: Instant)
    {
        val request = UpdateItemRequest.builder()
            .tableName(SessionTable.name)
            .key(SessionTable.key(id))
            .conditionExpression("attribute_exists(${SessionTable.id})")
            .updateExpression("SET ${SessionTable.expiresAt} = ${SessionTable.expiresAt.colonName}")
            .expressionAttributeValues(
                mapOf(
                    SessionTable.expiresAt.toExpressionNameValuePair(expiresAt.epochSecond)
                )
            )
            .build()

        try
        {
            dynamoDBClient.updateItem(request)
        } catch (_: ConditionalCheckFailedException)
        {
            // this exceptions means the entry does not exists, which isn't an error
            _logger.debug("updateSessionExpiration on a nonexistent session")
        }
    }

    override fun deleteSessionState(id: String)
    {
        val request = DeleteItemRequest.builder()
            .tableName(SessionTable.name)
            .key(SessionTable.key(id))
            .build()

        dynamoDBClient.deleteItem(request)
    }

    companion object
    {
        val _logger = LoggerFactory.getLogger(DynamoDBSessionDataAccessProvider.javaClass)
    }
}
