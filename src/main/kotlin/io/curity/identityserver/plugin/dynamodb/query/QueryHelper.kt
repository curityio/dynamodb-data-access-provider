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
import io.curity.identityserver.plugin.dynamodb.TableWithCapabilities
import io.curity.identityserver.plugin.dynamodb.count
import io.curity.identityserver.plugin.dynamodb.queryPartialSequence
import se.curity.identityserver.sdk.datasource.db.TableCapabilities
import se.curity.identityserver.sdk.datasource.errors.DataSourceCapabilityException
import se.curity.identityserver.sdk.service.Json
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.Select
import java.util.Base64

object QueryHelper {

    data class PotentialKeys(
        val partitionKeys: Map<DynamoDBAttribute<Any>, Any>,
        val sortKeys: Map<DynamoDBAttribute<Any>, Any> = mapOf(),
        val filterKeys: Map<DynamoDBAttribute<Any>, Any> = mapOf()
    )

    fun list(
        _dynamoDBClient: DynamoDBClient,
        json: Json,
        tableName: String,
        table: TableWithCapabilities,
        potentialKeys: PotentialKeys,
        ascendingOrder: Boolean,
        pageCount: Int?,
        pageCursor: String?
    ): Pair<Sequence<Map<String, AttributeValue>>, String?> {

        val listRequestBuilder = QueryRequest.builder().init(tableName, table, potentialKeys, ascendingOrder)

        listRequestBuilder.limit(pageCount ?: DEFAULT_PAGE_SIZE)

        val exclusiveStartKey = getExclusiveStartKey(json, pageCursor)
        if (exclusiveStartKey.isNotEmpty()) {
            listRequestBuilder.exclusiveStartKey(exclusiveStartKey)
        }

        val listRequest = listRequestBuilder.build()

        val (sequence, lastEvaluationKey) = queryPartialSequence(listRequest, _dynamoDBClient)

        val encodedCursor = getEncodedCursor(json, lastEvaluationKey)
        return Pair(sequence, encodedCursor)
    }

    private const val DEFAULT_PAGE_SIZE = 50

    fun count(
        _dynamoDBClient: DynamoDBClient,
        tableName: String,
        table: TableWithCapabilities,
        potentialKeys: PotentialKeys
    ): Long {

        val countRequestBuilder = QueryRequest.builder().init(tableName, table, potentialKeys, true)

        countRequestBuilder.select(Select.COUNT)
        val countRequest = countRequestBuilder.build()

        return count(countRequest, _dynamoDBClient)
    }

    /**
     * [QueryRequest.Builder] extension initializing the common stuff
     *
     * @param tableName
     * @param table
     * @param potentialKeys
     * @return a pre-initialized [QueryRequest.Builder]
     */
    private fun QueryRequest.Builder.init(
        tableName: String,
        table: TableWithCapabilities,
        potentialKeys: PotentialKeys,
        ascendingOrder: Boolean
    ): QueryRequest.Builder {
        val indexAndKeys = findIndexAndKeysFrom(table, potentialKeys)
        if (indexAndKeys == null) {
            throwCapabilityException(
                TableCapabilities.TableCapability.FILTERING_ABSENT,
                TableCapabilities.TableCapability.FILTERING_ABSENT.unsupportedMessage + FOR_TABLE_OR_DB_ERROR_TEMPLATE
            )
            // Unreachable, just to prevent compiler requesting safe call notation on indexAndKeys below
            return this
        }

        tableName(tableName)
            .indexName(indexAndKeys.index.indexName)
            .keyConditionExpression(indexAndKeys.keyConditionExpression)
            .expressionAttributeNames(indexAndKeys.expressionNameMap())
            .expressionAttributeValues(indexAndKeys.expressionValueMap())
            .scanIndexForward(ascendingOrder)

        val filterExpression = indexAndKeys.filterExpression()
        if (filterExpression.isNotBlank()) {
            filterExpression(filterExpression)
        }
        return this
    }

    private const val FOR_TABLE_OR_DB_ERROR_TEMPLATE = " either for that table or with %s"

    fun validateRequest(tableCapabilities: TableCapabilities, dialectName: String, sortingRequested: Boolean) {

        // Sorting not supported by some tables
        if (sortingRequested && tableCapabilities.unsupported.contains(TableCapabilities.TableCapability.SORTING)) {
            throwCapabilityException(
                TableCapabilities.TableCapability.SORTING,
                TableCapabilities.TableCapability.SORTING.unsupportedMessage + FOR_TABLE_OR_DB_ERROR_TEMPLATE,
                dialectName
            )
        }
    }

    private fun throwCapabilityException(
        unsupportedCapability: TableCapabilities.TableCapability,
        messageTemplate: String,
        vararg arguments: String
    ) {

        throw DataSourceCapabilityException(unsupportedCapability, String.format(messageTemplate, arguments))
    }

    /**
     * Finds the index to use based on provided keys
     *
     * @param table             providing indexes
     * @param potentialKeys     potential partition, sort and filter keys, as returned by [filterAndSortPotentialKeys]
     * @return [IndexAndKeys] helper object to be used with [QueryRequest.Builder]
     */
    private fun findIndexAndKeysFrom(
        table: TableWithCapabilities,
        potentialKeys: PotentialKeys
    ): IndexAndKeys<Any, Any, Any>? {
        var lastPotentialPartitionOnlyIndex: IndexAndKeys<Any, Any, Any>? = null
        potentialKeys.partitionKeys.forEach { potentialPartitionKey ->
            val potentialIndexes = table.queryCapabilities().indexes.asSequence()
                .filter { index ->
                    index.partitionAttribute.name == potentialPartitionKey.key.name
                }.toSet()
            potentialKeys.sortKeys.forEach { potentialSortKey ->
                val foundIndex = potentialIndexes.firstOrNull { index ->
                    index.sortAttribute?.name == potentialSortKey.key.name
                }

                if (foundIndex != null) {
                    val filterKeys = moveLeftOverKeys(potentialKeys, potentialPartitionKey, potentialSortKey)
                    return IndexAndKeys(
                        foundIndex,
                        potentialPartitionKey.toPair(),
                        potentialSortKey.toPair(),
                        filterKeys
                    )
                }
            }
            val filterKeys = moveLeftOverKeys(potentialKeys, potentialPartitionKey)
            lastPotentialPartitionOnlyIndex =
                IndexAndKeys(potentialIndexes.first(), potentialPartitionKey.toPair(), null, filterKeys)
        }
        return lastPotentialPartitionOnlyIndex
    }

    private fun moveLeftOverKeys(
        potentialKeys: PotentialKeys,
        foundPartitionKey: Map.Entry<DynamoDBAttribute<Any>, Any>,
        foundSortKey: Map.Entry<DynamoDBAttribute<Any>, Any>? = null
    ): Map<DynamoDBAttribute<Any>, Any> {
        val filterKeys = potentialKeys.filterKeys.toMutableMap()
        potentialKeys.partitionKeys.filter { it != foundPartitionKey }
            .forEach { potentialPartitionKey ->
                filterKeys += potentialPartitionKey.key to potentialPartitionKey.value
            }
        potentialKeys.sortKeys.filter { it != foundSortKey }
            .forEach { potentialSortKey ->
                filterKeys += potentialSortKey.key to potentialSortKey.value
            }
        return filterKeys
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
     * Helper object holding found index, partition and (optional) sort & filter keys,
     * along with their respective value.
     *
     * Provides methods generating parameters needed by a [QueryRequest.Builder]
     *
     * @param T1                partition key's type
     * @param T2                sort key's type
     * @property index          fitting provided keys
     * @property partitionKey   [Pair] with [DynamoDBAttribute] & value
     * @property sortKey        [Pair] with [DynamoDBAttribute] & value
     */
    private class IndexAndKeys<T1, T2, T3>(
        val index: Index,
        val partitionKey: Pair<DynamoDBAttribute<T1>, T1>,
        val sortKey: Pair<DynamoDBAttribute<T2>, T2>?,
        val filterKeys: Map<DynamoDBAttribute<T3>, T3>
    ) {
        fun expressionValueMap(): Map<String, AttributeValue> {
            val expressionValueMap = mutableMapOf(partitionKey.first.toExpressionNameValuePair(partitionKey.second))
            if (filterKeys.isNotEmpty()) {
                filterKeys.forEach { filterKey ->
                    expressionValueMap += filterKey.key.toExpressionNameValuePair(filterKey.value)
                }
            }
            return expressionValueMap
        }

        fun expressionNameMap(): Map<String?, String?> {
            val expressionNameMap =
                mutableMapOf<String?, String?>(partitionKey.first.hashName to partitionKey.first.name)
            if (filterKeys.isNotEmpty()) {
                filterKeys.forEach { filterKey ->
                    expressionNameMap += filterKey.key.hashName to filterKey.key.name
                }
            }
            return expressionNameMap
        }

        val keyConditionExpression = "${partitionKey.first.hashName} = ${partitionKey.first.colonName}"

        fun filterExpression(): String {
            var filterExpression = ""
            if (filterKeys.isNotEmpty()) {
                val filterKey = filterKeys.asSequence().first()
                filterExpression += "${filterKey.key.hashName} = ${filterKey.key.colonName}"
            }
            filterKeys.asSequence().drop(1).forEach { filterKey ->
                filterExpression += " AND ${filterKey.key.hashName} = ${filterKey.key.colonName}"

            }
            return filterExpression
        }
    }
}