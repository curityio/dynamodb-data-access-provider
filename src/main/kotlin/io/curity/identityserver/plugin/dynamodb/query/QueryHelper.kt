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

    // Used to find index & prepare DynamoDB request
    data class PotentialKeys(
        val partitionKeys: Map<DynamoDBAttribute<Any>, Any>,
        val sortKeys: Map<DynamoDBAttribute<Any>, Any> = mapOf(),
        val filterKeys: Map<DynamoDBAttribute<Any>, Any> = mapOf()
    )

    fun list(
        dynamoDBClient: DynamoDBClient,
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

        if (!pageCursor.isNullOrBlank()) {
            val exclusiveStartKey = getExclusiveStartKey(json, pageCursor)
            listRequestBuilder.exclusiveStartKey(exclusiveStartKey)
        }

        val listRequest = listRequestBuilder.build()

        val (sequence, lastEvaluationKey) = queryPartialSequence(listRequest, dynamoDBClient)

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
            ?: throw DataSourceCapabilityException(
                TableCapabilities.TableCapability.FILTERING_ABSENT,
                TableCapabilities.TableCapability.FILTERING_ABSENT.unsupportedMessage
            )

        tableName(tableName)
            .indexName(indexAndKeys.index.indexName)
            .keyConditionExpression(indexAndKeys.keyConditionExpression)
            .expressionAttributeNames(indexAndKeys.expressionNameMap())
            .expressionAttributeValues(indexAndKeys.expressionValueMap())
            .scanIndexForward(ascendingOrder)

        indexAndKeys.filterExpression()?.let { filterExpression(it) }
        return this
    }

    fun validateRequest(tableCapabilities: TableCapabilities, sortingRequested: Boolean) {

        // Sorting not supported by some tables
        if (sortingRequested && tableCapabilities.unsupported.contains(TableCapabilities.TableCapability.SORTING)) {
            throw DataSourceCapabilityException(
                TableCapabilities.TableCapability.SORTING,
                TableCapabilities.TableCapability.SORTING.unsupportedMessage
            )
        }
    }

    /**
     * Finds the index to use based on provided [potentialKeys].
     *
     * The table's indexes are first searched for those having one of the partition keys (PKs). If some are found,
     * they are then searched for those having one of the sort keys (SKs). If some are found, the first one will
     * be used to build the DynamoDB request.
     *
     * If only indexes with one of the provided PKs are found, without any of the provided SKs, then the first one
     * will be used to build the DynamoDB request.
     *
     * If no indexes are found with one of the PKs, then `null` is returned.
     *
     * @param table             providing indexes
     * @param potentialKeys     potential partition, sort and filter keys
     * @return [IndexAndKeys] helper object to be used with [QueryRequest.Builder]
     */
    private fun findIndexAndKeysFrom(
        table: TableWithCapabilities,
        potentialKeys: PotentialKeys
    ): IndexAndKeys<Any, Any, Any>? {
        var lastPotentialPartitionOnlyIndex: IndexAndKeys<Any, Any, Any>? = null
        potentialKeys.partitionKeys.forEach { potentialPartitionKey ->
            // Search table's indexes for those having it as partition key (PK)
            val potentialIndexes = table.queryCapabilities().indexes.asSequence()
                .filter { index ->
                    index.partitionAttribute.name == potentialPartitionKey.key.name
                }.toSet()
            potentialKeys.sortKeys.forEach { potentialSortKey ->
                // Search previously found indexes for those having it as sort key (SK)
                val foundIndex = potentialIndexes.firstOrNull { index ->
                    index.sortAttribute?.name == potentialSortKey.key.name
                }

                if (foundIndex != null) {
                    // Found an index with both PK & SK, so move other potential keys to filters
                    val filterKeys = moveLeftOverKeys(potentialKeys, potentialPartitionKey, potentialSortKey)
                    return IndexAndKeys(
                        foundIndex,
                        potentialPartitionKey.toPair(),
                        potentialSortKey.toPair(),
                        filterKeys
                    )
                }
            }
            // Found indexes but only with one of the PKs but without any of the provided SKs,
            // so move other potential keys to filters
            val filterKeys = moveLeftOverKeys(potentialKeys, potentialPartitionKey)
            // And take the first as the one to work with
            lastPotentialPartitionOnlyIndex =
                IndexAndKeys(potentialIndexes.first(), potentialPartitionKey.toPair(), null, filterKeys)
        }
        // Found no indexes with any of the provided PKs
        return lastPotentialPartitionOnlyIndex
    }

    /**
     * Moves partition keys and sort keys to filters if they were not found in the table's indexes
     *
     * @param potentialKeys
     * @param foundPartitionKey
     * @param foundSortKey
     * @return the filter keys
     */
    private fun moveLeftOverKeys(
        potentialKeys: PotentialKeys,
        foundPartitionKey: Map.Entry<DynamoDBAttribute<Any>, Any>,
        foundSortKey: Map.Entry<DynamoDBAttribute<Any>, Any>? = null
    ): Map<DynamoDBAttribute<Any>, Any> {
        val filterKeys = potentialKeys.filterKeys.toMutableMap()
        potentialKeys.partitionKeys.filter { it != foundPartitionKey }
            // Add unused partition keys to the filter keys
            .forEach { potentialPartitionKey ->
                filterKeys += potentialPartitionKey.key to potentialPartitionKey.value
            }
        potentialKeys.sortKeys.filter { it != foundSortKey }
            // Add unused sort keys to the filter keys
            .forEach { potentialSortKey ->
                filterKeys += potentialSortKey.key to potentialSortKey.value
            }
        return filterKeys
    }

    private fun getEncodedCursor(json: Json, cursor: Map<String, AttributeValue>?): String? {
        if (cursor?.isEmpty() != false) {
            return null
        }

        val cursorAsList = toBasicAttributeValueList(cursor)
        val serializedCursor = json.toJson(cursorAsList, true)
        return Base64.getEncoder().encodeToString(serializedCursor.toByteArray())
    }

    /**
     * Converts a map of [AttributeValue] to a simpler list for serialization
     *
     * @param cursor to be converted
     * @return simpler representation of the cursor
     */
    private fun toBasicAttributeValueList(cursor: Map<String, AttributeValue>): List<Map<String, Any?>> {
        return cursor.map { entry ->
            val (type, value) = getTypeAndValue(entry.value)
            mapOf("name" to entry.key, "type" to type, "value" to value)
        }
    }

    /**
     * @param attributeValue    to retrieve type-value from
     * @return the first type-value pair with a non-null value
     */
    private fun getTypeAndValue(attributeValue: AttributeValue): Pair<String, Any?> {
        attributeValue.s()?.let {
            return Pair("s", attributeValue.s())
        }
        attributeValue.n()?.let {
            return Pair("n", attributeValue.n())
        }
        attributeValue.b()?.let {
            return Pair("b", attributeValue.b())
        }
        if (attributeValue.hasSs()) {
            return Pair("ss", attributeValue.ss())
        }
        if (attributeValue.hasNs()) {
            return Pair("ns", attributeValue.ns())
        }
        if (attributeValue.hasBs()) {
            return Pair("bs", attributeValue.bs())
        }
        if (attributeValue.hasM()) {
            return Pair("m", attributeValue.m())
        }
        if (attributeValue.hasL()) {
            return Pair("l", attributeValue.l())
        }
        attributeValue.bool()?.let {
            return Pair("bool", attributeValue.bool())
        }
        attributeValue.nul()?.let {
            return Pair("nul", attributeValue.nul())
        }

        // Should never be reached!
        return Pair("", null)
    }

    private fun getDecodedJson(cursor: String) = String(Base64.getDecoder().decode(cursor))

    private fun getDeserializedList(jsonDeserializer: Json, decodedJson: String): List<Map<String, Any?>> =
        try {
            @Suppress("UNCHECKED_CAST")
            jsonDeserializer.fromJsonArray(decodedJson, true) as? List<Map<String, Any?>>
                ?: throw IllegalArgumentException(String.format("Couldn't deserialize JSON cursor: %s", decodedJson))
        } catch (e: Json.JsonException) {
            logger.debug("Couldn't deserialize JSON cursor, it's likely invalid")
            throw IllegalArgumentException(String.format("Couldn't deserialize JSON cursor: %s", decodedJson))
        }

    private fun getExclusiveStartKey(
        jsonDeserializer: Json,
        encodedCursor: String
    ): Map<String, AttributeValue?> {
        val cursor = getDeserializedList(jsonDeserializer, getDecodedJson(encodedCursor))
        return cursor.toExclusiveStartKey()
    }

    private fun List<Map<String, Any?>>.toExclusiveStartKey(): Map<String, AttributeValue> {
        val exclusiveStartKey: MutableMap<String, AttributeValue> = mutableMapOf()
        forEach { element ->
            val builder = AttributeValue.builder()
            @Suppress("UNCHECKED_CAST")
            when (element["type"]) {
                "s" -> builder.s(element["value"] as String)
                "n" -> builder.n(element["value"] as String)
                "b" -> builder.b(element["value"] as SdkBytes)
                "ss" -> builder.ss(element["value"] as Collection<String>)
                "ns" -> builder.ns(element["value"] as Collection<String>)
                "bs" -> builder.bs(element["value"] as Collection<SdkBytes>)
                "m" -> builder.m(element["value"] as Map<String, AttributeValue>)
                "l" -> builder.l(element["value"] as Collection<AttributeValue>)
                "bool" -> builder.bool(element["value"] as Boolean)
                "nul" -> builder.nul(element["value"] as Boolean)
            }
            exclusiveStartKey[element["name"] as String] = builder.build()
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

        fun filterExpression(): String? {
            var filterExpression = ""
            if (filterKeys.isNotEmpty()) {
                val filterKey = filterKeys.asSequence().first()
                filterExpression += "${filterKey.key.hashName} = ${filterKey.key.colonName}"
            }
            filterKeys.asSequence().drop(1).forEach { filterKey ->
                filterExpression += " AND ${filterKey.key.hashName} = ${filterKey.key.colonName}"

            }
            return filterExpression.ifEmpty {
                null
            }
        }
    }
}