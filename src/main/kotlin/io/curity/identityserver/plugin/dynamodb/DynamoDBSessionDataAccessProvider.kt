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
) : SessionDataAccessProvider {
    object SessionTable : Table("curity-sessions") {
        val id = StringAttribute("id")
        val data = StringAttribute("sessionData")
        val expiresAt = NumberLongAttribute("expiresAt")
        val deletableAt = NumberLongAttribute("deletableAt")

        fun key(id: String) = mapOf(this.id.toNameValuePair(id))
    }

    override fun getSessionById(id: String): Session? {
        val request = GetItemRequest.builder()
            .tableName(SessionTable.name(_configuration))
            .key(SessionTable.key(id))
            .consistentRead(true)
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        val item = response.item()

        return Session(
            SessionTable.id.from(item),
            Instant.ofEpochSecond(SessionTable.expiresAt.from(item)),
            SessionTable.data.from(item)
        )
    }

    override fun insertSession(session: Session) {
        val item = mapOf(
            SessionTable.id.toNameValuePair(session.id),
            SessionTable.expiresAt.toNameValuePair(session.expiresAt.epochSecond),
            SessionTable.data.toNameValuePair(session.data),
            SessionTable.deletableAt.toNameValuePair(getDeletableAt(session.expiresAt))
        )

        val request = PutItemRequest.builder()
            .tableName(SessionTable.name(_configuration))
            .item(item)
            .conditionExpression("attribute_not_exists(${SessionTable.id})")
            .build()
        try {
            _dynamoDBClient.putItem(request)
        } catch (_: ConditionalCheckFailedException) {
            throw ConflictException("There is already a session with the same id.")
        }
    }

    override fun updateSession(updatedSession: Session) {
        val item = mapOf(
            SessionTable.id.toNameValuePair(updatedSession.id),
            SessionTable.expiresAt.toNameValuePair(updatedSession.expiresAt.epochSecond),
            SessionTable.data.toNameValuePair(updatedSession.data),
            SessionTable.deletableAt.toNameValuePair(getDeletableAt(updatedSession.expiresAt))
        )

        // The PutItem does not have any condition, so:
        // - if the item already exists, it will be completely overwritten
        // - otherwise, a new item will be created
        val request = PutItemRequest.builder()
            .tableName(SessionTable.name(_configuration))
            .item(item)
            .build()

        _dynamoDBClient.putItem(request)
    }

    override fun updateSessionExpiration(id: String, expiresAt: Instant) {
        val request = UpdateItemRequest.builder()
            .tableName(SessionTable.name(_configuration))
            .key(SessionTable.key(id))
            .conditionExpression("attribute_exists(${SessionTable.id})")
            .updateExpression(
                "SET ${SessionTable.expiresAt} = ${SessionTable.expiresAt.colonName}" +
                        ", ${SessionTable.deletableAt} = ${SessionTable.deletableAt.colonName}"
            )
            .expressionAttributeValues(
                mapOf(
                    SessionTable.expiresAt.toExpressionNameValuePair(expiresAt.epochSecond),
                    SessionTable.deletableAt.toExpressionNameValuePair(getDeletableAt(expiresAt))
                )
            )
            .build()

        try {
            _dynamoDBClient.updateItem(request)
        } catch (_: ConditionalCheckFailedException) {
            // this exceptions means the entry does not exists, which isn't an error
            _logger.debug("updateSessionExpiration on a nonexistent session")
        }
    }

    override fun deleteSessionState(id: String) {
        val request = DeleteItemRequest.builder()
            .tableName(SessionTable.name(_configuration))
            .key(SessionTable.key(id))
            .build()

        _dynamoDBClient.deleteItem(request)
    }

    private fun getDeletableAt(expiration: Instant) =
        expiration.epochSecond + _configuration.getSessionsTtlRetainDuration()

    companion object {
        val _logger = LoggerFactory.getLogger(DynamoDBSessionDataAccessProvider::class.java)
    }
}
