/*
 *  Copyright 2022 Curity AB
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

package io.curity.identityserver.plugin.dynamodb.query

import io.curity.identityserver.plugin.dynamodb.DynamoDBAttribute
import io.curity.identityserver.plugin.dynamodb.DynamoDBClient
import io.curity.identityserver.plugin.dynamodb.DynamoDBClient.Companion.logger
import io.curity.identityserver.plugin.dynamodb.DynamoDBItem
import io.curity.identityserver.plugin.dynamodb.TableWithCapabilities
import io.curity.identityserver.plugin.dynamodb.queryPartialSequence
import se.curity.identityserver.sdk.service.Json
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import java.util.Base64

object QueryHelper {

    fun query(
        _dynamoDBClient: DynamoDBClient,
        json: Json,
        tableName: String,
        table: TableWithCapabilities,
        potentialKeys: Pair<Map<DynamoDBAttribute<Any>, Any>, Map<DynamoDBAttribute<Any>, Any>>,
        ascendingOrder: Boolean,
        pageCount: Int?,
        pageCursor: String?
    ): Pair<Sequence<DynamoDBItem>, String?> {

        val indexAndKeys = findIndexAndKeysFrom(table, potentialKeys)
            ?: // TODO IS-6705 handle as a Scan if enabled, or report as unsupported
            throw java.lang.IllegalArgumentException("No index found fitting provided request")

        val queryRequestBuilder = QueryRequest.builder()
            .tableName(tableName)
            .indexName(indexAndKeys.index.indexName)
            .keyConditionExpression(indexAndKeys.keyConditionExpression(ascendingOrder))
            // TODO IS-6705 add filter expression along with names & values, with activeClientOnly and left apart potential keys
            // .filterExpression("")
            .expressionAttributeNames(indexAndKeys.expressionNameMap())
            .expressionAttributeValues(indexAndKeys.expressionValueMap())
            .scanIndexForward(ascendingOrder)

        queryRequestBuilder.limit(pageCount ?: DEFAULT_PAGE_SIZE)

        val exclusiveStartKey = getExclusiveStartKey(json, pageCursor)
        if (exclusiveStartKey.isNotEmpty()) {
            queryRequestBuilder.exclusiveStartKey(exclusiveStartKey)
        }

        val queryRequest = queryRequestBuilder.build()

        val (sequence, lastEvaluationKey) = queryPartialSequence(queryRequest, _dynamoDBClient)
        val result = linkedMapOf<String, Map<String, AttributeValue>>()
        sequence.forEach {
            result[table.keyAttribute().from(it)] = it
        }

        val encodedCursor = getEncodedCursor(json, lastEvaluationKey)
        return Pair(result.values.asSequence(), encodedCursor)
    }

    private const val DEFAULT_PAGE_SIZE = 50

    /**
     * @param potentialKeys as [Triple]s including both potential partition keys and potential sort keys
     * @return a [Pair] of [Map]s, the first holding only potential partition keys having an actual value,
     * the second holding only potential sort keys having an actual value
     */
    fun createPotentialKeys(vararg potentialKeys: Triple<DynamoDBAttribute<Any>, Any?, Boolean>):
    // TODO IS-6705 create an intermediary object?
            Pair<Map<DynamoDBAttribute<Any>, Any>, Map<DynamoDBAttribute<Any>, Any>> {
        val potentialPartitionKeys: MutableMap<DynamoDBAttribute<Any>, Any> = mutableMapOf()
        val potentialSortKeys: MutableMap<DynamoDBAttribute<Any>, Any> = mutableMapOf()
        potentialKeys.forEach { potentialKey ->
            if (potentialKey.attributeValue != null) {
                if (potentialKey.potentialPartitionKey) {
                    potentialPartitionKeys[potentialKey.attribute] = potentialKey.attributeValue!!
                } else {
                    potentialSortKeys[potentialKey.attribute] = potentialKey.attributeValue!!
                }
            }
        }
        return potentialPartitionKeys to potentialSortKeys
    }

    private val Triple<DynamoDBAttribute<Any>, Any?, Boolean>.attribute
        get() = first
    private val Triple<DynamoDBAttribute<Any>, Any?, Boolean>.attributeValue
        get() = second
    private val Triple<DynamoDBAttribute<Any>, Any?, Boolean>.potentialPartitionKey
        get() = third

    private val Pair<Map<DynamoDBAttribute<Any>, Any>, Map<DynamoDBAttribute<Any>, Any>>.partitionKeys
        get() = first
    private val Pair<Map<DynamoDBAttribute<Any>, Any>, Map<DynamoDBAttribute<Any>, Any>>.sortKeys
        get() = second

    /**
     * Finds the index to use based on provided keys
     *
     * @param table             providing indexes
     * @param potentialKeys     [Pair] of potential partition and sort keys, as returned by [createPotentialKeys]
     * @return an [IndexAndKeys] helper object to be used with [QueryRequest.Builder]
     */
    private fun findIndexAndKeysFrom(
        table: TableWithCapabilities,
        // TODO IS-6705 use intermediary object to be created?
        potentialKeys: Pair<Map<DynamoDBAttribute<Any>, Any>, Map<DynamoDBAttribute<Any>, Any>>
    ): IndexAndKeys<Any, Any>? {
        var lastPotentialPartitionOnlyIndex: IndexAndKeys<Any, Any>? = null
        potentialKeys.partitionKeys.forEach { potentialPartitionKey ->
            val potentialIndexes = table.queryCapabilities().indexes.asSequence()
                .filter { index ->
                    index.partitionAttribute.name == potentialPartitionKey.key.name
                }.toSet()
            potentialKeys.sortKeys.forEach { potentialSortKey ->
                val foundIndex = potentialIndexes.filter { index ->
                    index.sortAttribute?.name == potentialSortKey.key.name
                }.firstOrNull()
                if (foundIndex != null) {
                    return IndexAndKeys(foundIndex, potentialPartitionKey.toPair(), potentialSortKey.toPair())
                }
            }
            lastPotentialPartitionOnlyIndex =
                IndexAndKeys(potentialIndexes.first(), potentialPartitionKey.toPair(), null)
        }
        return lastPotentialPartitionOnlyIndex
    }

    private fun getEncodedCursor(json: Json, cursor: Map<String, Any>?): String? {
        if (cursor?.isEmpty() != false) {
            return null
        }

        // Convert to mutable Map in case cursor isn't, and while keeping ordering!
        val modifiedCursor: MutableMap<String, Any> = java.util.LinkedHashMap<String, Any>(cursor)
        val serializedCursor = json.toJson(modifiedCursor, true)
        return Base64.getEncoder().encodeToString(serializedCursor.toByteArray())
    }

    private fun getDecodedJson(cursor: String?): String =
        if (cursor.isNullOrBlank()) {
            ""
        } else String(Base64.getDecoder().decode(cursor))


    private fun getDeserializedMap(jsonDeserializer: Json, decodedJson: String): Map<String, Any> =
        if (decodedJson.isBlank()) {
            mapOf()
        } else {
            try {
                jsonDeserializer.fromJson(decodedJson, true)
            } catch (e: Json.JsonException) {
                logger.debug("Invalid Cursor. {}", e.message)
                throw IllegalArgumentException(String.format("Invalid pagination cursor. %s", decodedJson))
            }
        }

    private fun getExclusiveStartKey(
        jsonDeserializer: Json,
        encodedCursor: String?
    ): Map<String, AttributeValue?> {
        val cursor = getDeserializedMap(jsonDeserializer, getDecodedJson(encodedCursor))
        @Suppress("UNCHECKED_CAST")
        if ((cursor as Map<String, Map<String, Any?>>).isNotEmpty()) {
            return cursor.toExclusiveStartKey()
        }
        return mapOf()
    }

    private fun Map<String, Map<String, Any?>>.toExclusiveStartKey(): Map<String, AttributeValue> {
        val exclusiveStartKey: MutableMap<String, AttributeValue> = mutableMapOf()
        forEach nextKey@{ (key, value) ->
            val builder = AttributeValue.builder()
            value.forEach { (subKey, subValue) ->
                if (
                    (subValue != null)
                    && ((subValue !is Collection<*>) || !subValue.isEmpty())
                ) {
                    @Suppress("UNCHECKED_CAST")
                    when (subKey) {
                        "s" -> builder.s(subValue as String)
                        "n" -> builder.n(subValue as String)
                        "b" -> builder.b(subValue as SdkBytes)
                        "ss" -> builder.ss(subValue as Collection<String>)
                        "ns" -> builder.ns(subValue as Collection<String>)
                        "bs" -> builder.bs(subValue as Collection<SdkBytes>)
                        "m" -> builder.m(subValue as Map<String, AttributeValue>)
                        "l" -> builder.l(subValue as Collection<AttributeValue>)
                        "bool" -> builder.bool(subValue as Boolean)
                        "nul" -> builder.nul(subValue as Boolean)
                    }
                    exclusiveStartKey[key] = builder.build()
                    return@nextKey
                }
            }
        }
        return exclusiveStartKey
    }

    /**
     * Helper object holding found index and partition and (optional) sort keys, along with their respective value.
     *
     * Provides methods generating parameters needed by a [QueryRequest.Builder]
     *
     * @param T1                partition key's type
     * @param T2                sort key's type
     * @property index          fitting provided keys
     * @property partitionKey   [Pair] with [DynamoDBAttribute] & value
     * @property sortKey        [Pair] with [DynamoDBAttribute] & value
     */
    private class IndexAndKeys<T1, T2>(
        val index: Index,
        val partitionKey: Pair<DynamoDBAttribute<T1>, T1>,
        val sortKey: Pair<DynamoDBAttribute<T2>, T2>? = null
    ) {
        fun expressionValueMap(): Map<String, AttributeValue> {
            val expressionValueMap =
                mutableMapOf<String, AttributeValue>(
                    partitionKey.first.toExpressionNameValuePair(partitionKey.second)
                )
            if (sortKey != null) {
                expressionValueMap += sortKey.first.toExpressionNameValuePair(sortKey.second)
            }
            return expressionValueMap
        }

        fun expressionNameMap(): Map<String?, String?> {
            val expressionNameMap =
                mutableMapOf<String?, String?>(partitionKey.first.hashName to partitionKey.first.name)
            if (sortKey != null) {
                expressionNameMap += sortKey.first.hashName to sortKey.first.name
            }
            return expressionNameMap
        }

        fun keyConditionExpression(ascendingOrder: Boolean): String {
            val keyConditionExpression = "${partitionKey.first.hashName} = ${partitionKey.first.colonName}"
            return if (sortKey != null) {
                val operator = if (ascendingOrder) {
                    " > "
                } else {
                    " < "
                }
                keyConditionExpression + " AND ${sortKey.first.hashName}" + operator + sortKey.first.colonName
            } else {
                keyConditionExpression
            }
        }
    }
}