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
import io.curity.identityserver.plugin.dynamodb.UpdateExpressionsBuilder
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.data.authorization.TokenStatus
import se.curity.identityserver.sdk.data.tokens.NonceState
import se.curity.identityserver.sdk.datasource.NonceDataAccessProvider
import se.curity.identityserver.sdk.errors.ConflictException
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Duration
import java.time.Instant

class DynamoDBNonceDataAccessProvider(
    private val _dynamoDBClient: DynamoDBClient,
    private val _configuration: DynamoDBDataAccessProviderConfiguration
) : NonceDataAccessProvider {
    object NonceTable : Table("curity-nonces") {
        val nonce = StringAttribute("nonce")
        val nonceStatus = StringAttribute("nonceStatus")
        val createAt = NumberLongAttribute("createdAt")
        val nonceTtl = NumberLongAttribute("nonceTtl")
        val nonceValue = StringAttribute("nonceValue")
        val consumedAt = NumberLongAttribute("consumedAt")
        val deletableAt = NumberLongAttribute("deletableAt")

        fun key(nonce: String) = mapOf(this.nonce.toNameValuePair(nonce))
    }

    override fun getNonce(nonce: String): NonceState? {
        val request = GetItemRequest.builder()
            .tableName(NonceTable.name(_configuration))
            .key(NonceTable.key(nonce))
            .consistentRead(true)
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }
        val item = response.item()

        val createdAt = NonceTable.createAt.from(item)
        val ttl = NonceTable.nonceTtl.from(item)
        val now = Instant.now().epochSecond

        _logger.trace("Nonce createdAt: {}, ttl: {}, now: {}", createdAt, ttl, now)

        if (createdAt + ttl <= now) {
            expireNonce(nonce)
        }

        return DefaultNonce(
            createdAt,
            NonceTable.consumedAt.optionalFrom(item),
            ttl,
            NonceStatus.valueOf(NonceTable.nonceStatus.from(item)),
            NonceTable.nonceValue.from(item)
        )
    }

    override fun save(nonce: String, value: String, createdAt: Long, ttl: Long) {
        val item = mapOf(
            NonceTable.nonce.toNameValuePair(nonce),
            NonceTable.nonceValue.toNameValuePair(value),
            NonceTable.createAt.toNameValuePair(createdAt),
            NonceTable.nonceTtl.toNameValuePair(ttl),
            NonceTable.nonceStatus.toNameValuePair(NonceStatus.issued.name),
            NonceTable.deletableAt.toNameValuePair(createdAt + ttl + _configuration.getNoncesTtlRetainDuration())
        )

        val request = PutItemRequest.builder()
            .tableName(NonceTable.name(_configuration))
            .item(item)
            .conditionExpression("attribute_not_exists(${NonceTable.nonce})")
            .build()

        try {
            _dynamoDBClient.putItem(request)
        } catch (_: ConditionalCheckFailedException) {
            throw ConflictException("Nonce already exists")
        }
    }

    override fun consume(nonce: String, consumedAt: Instant): Boolean {
        return consumeNonce(nonce, consumedAt.epochSecond)
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
    private fun changeStatus(nonce: String, status: NonceStatus, maybeConsumedAt: Long?): Boolean {
        val updateBuilder = UpdateExpressionsBuilder().apply {
            onlyIfExists(NonceTable.nonce)
            onlyIf(NonceTable.nonceStatus, NonceStatus.issued.name)

            update(NonceTable.nonceStatus, status.name)
        }

        if (status == NonceStatus.consumed) {
            val consumedAt = maybeConsumedAt ?: throw IllegalArgumentException("consumedAt cannot be null")

            updateBuilder.update(NonceTable.consumedAt, consumedAt)
        }

        val requestBuilder = UpdateItemRequest.builder()
            .tableName(NonceTable.name(_configuration))
            .key(NonceTable.key(nonce))

        updateBuilder.applyTo(requestBuilder)

        try {
            _dynamoDBClient.updateItem(requestBuilder.build())
            return true
        } catch (_: ConditionalCheckFailedException) {
            _logger.trace("Trying to update a nonexistent or already consumed nonce")
            return false
        }
    }

    private enum class NonceStatus {
        // The string entries in the DB needs to be lowercase
        // there are integration tests that depend on that
        issued, consumed, expired
    }

    private class DefaultNonce(
        private val createdAt: Instant,
        private val consumedAt: Instant?,
        private val ttl: Duration,
        private val status: TokenStatus,
        private val value: String
    ) : NonceState {

        constructor(
            createdAt: Long,
            consumedAt: Long?,
            ttl: Long,
            status: NonceStatus,
            value: String
        ) : this(
            Instant.ofEpochSecond(createdAt),
            consumedAt?.let { Instant.ofEpochSecond(it) },
            Duration.ofSeconds(ttl),
            when(status) {
                NonceStatus.consumed -> TokenStatus.used
                NonceStatus.issued -> TokenStatus.issued
                NonceStatus.expired -> TokenStatus.issued // server checks expiration
            },
            value
        )

        override fun createdAt(): Instant = createdAt
        override fun expiresAt(): Instant = createdAt + ttl
        override fun consumedAt(): Instant? = consumedAt
        override fun status(): TokenStatus = status
        override fun value(): String = value
    }

    companion object {
        val _logger = LoggerFactory.getLogger(DynamoDBNonceDataAccessProvider::class.java)
    }
}
