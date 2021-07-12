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
package io.curity.identityserver.plugin.dynamodb.token

import io.curity.identityserver.plugin.dynamodb.DynamoDBClient
import io.curity.identityserver.plugin.dynamodb.NumberLongAttribute
import io.curity.identityserver.plugin.dynamodb.StringAttribute
import io.curity.identityserver.plugin.dynamodb.Table
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.datasource.NonceDataAccessProvider
import se.curity.identityserver.sdk.errors.ConflictException
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant

class DynamoDBNonceDataAccessProvider(
    private val _dynamoDBClient: DynamoDBClient,
    private val _configuration: DynamoDBDataAccessProviderConfiguration
) : NonceDataAccessProvider
{
    private val _nonceTableName = _configuration.getNonceTableNameOverride()
    init {
        NonceTable.name = _nonceTableName
    }
    object NonceTable : Table("curity-nonces")
    {
        val nonce = StringAttribute("nonce")
        val nonceStatus = StringAttribute("nonceStatus")
        val createAt = NumberLongAttribute("createdAt")
        val nonceTtl = NumberLongAttribute("nonceTtl")
        val nonceValue = StringAttribute("nonceValue")
        val consumedAt = NumberLongAttribute("consumedAt")
        val deletableAt = NumberLongAttribute("deletableAt")

        fun key(nonce: String) = mapOf(this.nonce.toNameValuePair(nonce))
    }

    override fun get(nonce: String): String?
    {
        val request = GetItemRequest.builder()
            .tableName(NonceTable.name)
            .key(NonceTable.key(nonce))
            .consistentRead(true)
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty())
        {
            return null
        }
        val item = response.item()

        val status = NonceStatus.valueOf(NonceTable.nonceStatus.from(item))
        if (status != NonceStatus.issued)
        {
            return null
        }

        val createdAt = NonceTable.createAt.from(item)
        val ttl = NonceTable.nonceTtl.from(item)
        val now = Instant.now().epochSecond

        _logger.trace("Nonce createdAt: {}, ttl: {}, now: {}", createdAt, ttl, now)

        if (createdAt + ttl <= now)
        {
            expireNonce(nonce)
            return null
        }

        return NonceTable.nonceValue.from(item)
    }

    override fun save(nonce: String, value: String, createdAt: Long, ttl: Long)
    {
        val item = mapOf(
            NonceTable.nonce.toNameValuePair(nonce),
            NonceTable.nonceValue.toNameValuePair(value),
            NonceTable.createAt.toNameValuePair(createdAt),
            NonceTable.nonceTtl.toNameValuePair(ttl),
            NonceTable.nonceStatus.toNameValuePair(NonceStatus.issued.name),
            NonceTable.deletableAt.toNameValuePair(createdAt + ttl + _configuration.getNoncesTtlRetainDuration())
        )

        val request = PutItemRequest.builder()
            .tableName(NonceTable.name)
            .item(item)
            .conditionExpression("attribute_not_exists(${NonceTable.nonce})")
            .build()

        try
        {
            _dynamoDBClient.putItem(request)
        } catch (_: ConditionalCheckFailedException)
        {
            throw ConflictException("Nonce already exists")
        }
    }

    override fun consume(nonce: String, consumedAt: Long)
    {
        consumeNonce(nonce, consumedAt)
    }

    private fun consumeNonce(nonce: String, consumedAt: Long) = changeStatus(nonce, NonceStatus.consumed, consumedAt)
    private fun expireNonce(nonce: String) = changeStatus(nonce, NonceStatus.expired, null)

    // This method doesn't change the deletableAt attribute, if already present
    // - The time-to-live of a nonce is immutable (i.e. not extendable), so the deletableAt is never increased.
    // - Eventually we could reduce the deletableAt when the nonce is consumed,
    // making it `deletableAt = consumedAt + retainDuration`. However we opted out for not doing it since there is
    // no clear advantage and introduces more complexity.
    // Also, if the deletableAt was enabled when the nonce was created and disabled when the nonce is updated,
    // then the original deletableAt is kept.
    // We also don't add a deletableAt when the nonce is updated.
    private fun changeStatus(nonce: String, status: NonceStatus, maybeConsumedAt: Long?)
    {
        val requestBuilder = UpdateItemRequest.builder()
            .tableName(NonceTable.name)
            .conditionExpression("attribute_exists(${NonceTable.nonce.name})")
            .key(NonceTable.key(nonce))

        if (status == NonceStatus.consumed)
        {
            val consumedAt = maybeConsumedAt ?: throw IllegalArgumentException("consumedAt cannot be null")
            requestBuilder
                .updateExpression(
                    "SET ${NonceTable.nonceStatus.name} = ${NonceTable.nonceStatus.colonName}, " +
                            "${NonceTable.consumedAt.name} = ${NonceTable.consumedAt.colonName}"
                )
                .expressionAttributeValues(
                    mapOf(
                        NonceTable.nonceStatus.toExpressionNameValuePair(status.name),
                        NonceTable.consumedAt.toExpressionNameValuePair(consumedAt)
                    )
                )
        } else
        {
            requestBuilder
                .updateExpression("SET ${NonceTable.nonceStatus.name} = ${NonceTable.nonceStatus.colonName}")
                .expressionAttributeValues(
                    mapOf(
                        NonceTable.nonceStatus.toExpressionNameValuePair(status.name)
                    )
                )
        }

        try
        {
            _dynamoDBClient.updateItem(requestBuilder.build())
        } catch (_: ConditionalCheckFailedException)
        {
            _logger.trace("Trying to update a nonexistent nonce")
        }
    }

    private enum class NonceStatus
    {
        // The string entries in the DB needs to be lowercase
        // there are integration tests that depend on that
        issued, consumed, expired
    }

    companion object
    {
        val _logger = LoggerFactory.getLogger(DynamoDBNonceDataAccessProvider::class.java)
    }
}
