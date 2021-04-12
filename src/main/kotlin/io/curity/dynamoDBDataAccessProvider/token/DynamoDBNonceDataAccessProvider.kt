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
package io.curity.dynamoDBDataAccessProvider.token

import io.curity.dynamoDBDataAccessProvider.DynamoDBClient
import se.curity.identityserver.sdk.datasource.NonceDataAccessProvider
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant

class DynamoDBNonceDataAccessProvider(private val dynamoDBClient: DynamoDBClient): NonceDataAccessProvider
{
    override fun get(nonce: String): String?
    {
        val request = GetItemRequest.builder()
                .tableName(tableName)
                .key(getKey(nonce))
                .build()

        val response = dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        val item = response.item()
        val createAt = item["createdAt"]?.s()?.toLong() ?: -1L
        val ttl = item["nonceTtl"]?.s()?.toLong() ?: 0L
        val now = Instant.now().epochSecond

        if (item["nonceStatus"]?.s() != NonceStatus.ISSUED.value) {
            return null
        }

        if (createAt + ttl < now) {
            expireNonce(nonce)
            return null
        }

        return item["nonceValue"]?.s()
    }

    override fun save(nonce: String, value: String, createdAt: Long, ttl: Long)
    {
        val request = PutItemRequest.builder()
                .tableName(tableName)
                .item(mapOf(
                        Pair("nonce", AttributeValue.builder().s(nonce).build()),
                        Pair("nonceValue", AttributeValue.builder().s(value).build()),
                        Pair("createdAt", AttributeValue.builder().s(createdAt.toString()).build()),
                        Pair("nonceTtl", AttributeValue.builder().s(ttl.toString()).build()),
                        Pair("nonceStatus", AttributeValue.builder().s(NonceStatus.ISSUED.value).build())
                ))
                .build()

        dynamoDBClient.putItem(request)
    }

    override fun consume(nonce: String, consumedAt: Long)
    {
        consumeNonce(nonce, consumedAt)
    }

    private fun consumeNonce(nonce: String, consumedAt: Long) = changeStatus(nonce, NonceStatus.CONSUMED, consumedAt)
    private fun expireNonce(nonce: String) = changeStatus(nonce, NonceStatus.EXPIRED, null)

    private fun changeStatus(nonce: String, status: NonceStatus, consumedAt: Long?) {
        val requestBuilder = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(getKey(nonce))

        if (status == NonceStatus.CONSUMED) {
            requestBuilder
                    .updateExpression("SET nonceStatus = :status, consumedAt = :consumedAt")
                    .expressionAttributeValues(mapOf(
                            Pair(":status", AttributeValue.builder().s(status.value).build()),
                            Pair(":consumedAt", AttributeValue.builder().s(consumedAt!!.toString()).build())
                    ))
        } else {
            requestBuilder
                    .updateExpression("SET nonceStatus = :status")
                    .expressionAttributeValues(mapOf(Pair(":status", AttributeValue.builder().s(status.value).build())))
        }

        dynamoDBClient.updateItem(requestBuilder.build())
    }

    private fun getKey(nonce: String): Map<String, AttributeValue> =
            mapOf(Pair("nonce", AttributeValue.builder().s(nonce).build()))

    companion object {
        private const val tableName = "curity-nonces"
    }

    enum class NonceStatus(val value: String) {
        ISSUED("issued"), CONSUMED("consumed"), EXPIRED("expired")
    }
}
