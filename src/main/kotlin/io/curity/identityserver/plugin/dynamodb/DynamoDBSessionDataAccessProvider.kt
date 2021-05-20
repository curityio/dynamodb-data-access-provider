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

import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.data.Session
import se.curity.identityserver.sdk.datasource.SessionDataAccessProvider
import se.curity.identityserver.sdk.errors.ConflictException
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant

class DynamoDBSessionDataAccessProvider(
    private val _dynamoDBClient: DynamoDBClient,
    private val _configuration: DynamoDBDataAccessProviderConfiguration
) : SessionDataAccessProvider
{
    object SessionTable : Table("curity-sessions")
    {
        val id = StringAttribute("id")
        val data = StringAttribute("sessionData")
        val expiresAt = NumberLongAttribute("expiresAt")
        val deletableAt = NumberLongAttribute("deletableAt")

        fun key(id: String) = mapOf(this.id.toNameValuePair(id))
    }

    override fun getSessionById(id: String): Session?
    {
        val request = GetItemRequest.builder()
            .tableName(SessionTable.name)
            .key(SessionTable.key(id))
            .build()

        val response = _dynamoDBClient.getItem(request)

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
        val item = mutableMapOf(
            SessionTable.id.toNameValuePair(session.id),
            SessionTable.expiresAt.toNameValuePair(session.expiresAt.epochSecond),
            SessionTable.data.toNameValuePair(session.data)
        )
        ttlFor(session.expiresAt)?.let {
            SessionTable.deletableAt.addTo(item, it.epochSecond)
        }

        val request = PutItemRequest.builder()
            .tableName(SessionTable.name)
            .item(item)
            .conditionExpression("attribute_not_exists(${SessionTable.id})")
            .build()
        try
        {
            _dynamoDBClient.putItem(request)
        } catch (_: ConditionalCheckFailedException)
        {
            throw ConflictException("There is already a session with the same id.")
        }
    }

    override fun updateSession(updatedSession: Session)
    {
        val maybeTtl = ttlFor(updatedSession.expiresAt)
        // if TTL is configured with update the session with that attribute...
        val ttlUpdatePart = if (maybeTtl != null)
        {
            ", ${SessionTable.deletableAt} = ${SessionTable.deletableAt.colonName}"
        } else
        // ... otherwise we need to remove it, otherwise it would be there with an invalid TTL value
        {
            " REMOVE ${SessionTable.deletableAt}"
        }
        val attributeValuesMap = if (maybeTtl != null)
        {
            mapOf(
                SessionTable.expiresAt.toExpressionNameValuePair(updatedSession.expiresAt.epochSecond),
                SessionTable.data.toExpressionNameValuePair(updatedSession.data),
                SessionTable.deletableAt.toExpressionNameValuePair(maybeTtl.epochSecond)
            )
        } else
        {
            mapOf(
                SessionTable.expiresAt.toExpressionNameValuePair(updatedSession.expiresAt.epochSecond),
                SessionTable.data.toExpressionNameValuePair(updatedSession.data)
            )
        }
        val request = UpdateItemRequest.builder()
            .tableName(SessionTable.name)
            .key(SessionTable.key(updatedSession.id))
            .updateExpression(
                "SET ${SessionTable.data} = ${SessionTable.data.colonName}," +
                        " ${SessionTable.expiresAt} = ${SessionTable.expiresAt.colonName}" +
                        ttlUpdatePart
            )
            .expressionAttributeValues(attributeValuesMap)
            .build()

        _dynamoDBClient.updateItem(request)
    }

    override fun updateSessionExpiration(id: String, expiresAt: Instant)
    {
        val maybeTtl = ttlFor(expiresAt)
        // see comments on the updateSession
        val ttlUpdatePart = if (maybeTtl != null)
        {
            ", ${SessionTable.deletableAt} = ${SessionTable.deletableAt.colonName}"
        } else
        {
            " REMOVE ${SessionTable.deletableAt}"
        }
        val attributeValuesMap = if (maybeTtl != null)
        {
            mapOf(
                SessionTable.expiresAt.toExpressionNameValuePair(expiresAt.epochSecond),
                SessionTable.deletableAt.toExpressionNameValuePair(maybeTtl.epochSecond)
            )
        } else
        {
            mapOf(
                SessionTable.expiresAt.toExpressionNameValuePair(expiresAt.epochSecond)
            )
        }
        val request = UpdateItemRequest.builder()
            .tableName(SessionTable.name)
            .key(SessionTable.key(id))
            .conditionExpression("attribute_exists(${SessionTable.id})")
            .updateExpression(
                "SET ${SessionTable.expiresAt} = ${SessionTable.expiresAt.colonName}" +
                        ttlUpdatePart
            )
            .expressionAttributeValues(attributeValuesMap)
            .build()

        try
        {
            _dynamoDBClient.updateItem(request)
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

        _dynamoDBClient.deleteItem(request)
    }

    private fun ttlFor(expiration: Instant): Instant?
    {
        val maybeRetainDuration = _configuration.getSessionsTtlRetainDuration().orElse(null) ?: return null
        if (maybeRetainDuration < 0)
        {
            throw _configuration.getExceptionFactory()
                .configurationException("sessions retain duration cannot be negative")
        }
        return expiration.plusSeconds(maybeRetainDuration)
    }

    companion object
    {
        val _logger = LoggerFactory.getLogger(DynamoDBSessionDataAccessProvider.javaClass)
    }
}
