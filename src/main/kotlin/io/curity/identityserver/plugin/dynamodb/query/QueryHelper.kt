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

import io.curity.identityserver.plugin.dynamodb.DEFAULT_PAGE_SIZE
import io.curity.identityserver.plugin.dynamodb.DynamoDBAttribute
import io.curity.identityserver.plugin.dynamodb.DynamoDBClient
import io.curity.identityserver.plugin.dynamodb.DynamoDBClient.Companion.logger
import io.curity.identityserver.plugin.dynamodb.TableWithCapabilities
import io.curity.identityserver.plugin.dynamodb.count
import io.curity.identityserver.plugin.dynamodb.queryWithPagination
import io.curity.identityserver.plugin.dynamodb.scanWithPagination
import se.curity.identityserver.sdk.datasource.db.TableCapabilities
import se.curity.identityserver.sdk.datasource.errors.DataSourceCapabilityException
import se.curity.identityserver.sdk.datasource.query.AttributesFiltering
import se.curity.identityserver.sdk.datasource.query.AttributesFiltering.FilterType
import se.curity.identityserver.sdk.datasource.query.AttributesSorting
import se.curity.identityserver.sdk.service.Json
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.Select
import java.util.Base64

object QueryHelper {

    // Used to find index & prepare DynamoDB request
    data class PotentialKeys(
        val partitionKeys: Map<DynamoDBAttribute<Any>, Any>,
        val sortKeys: Map<DynamoDBAttribute<Any>, Any> = mapOf(),
        val filterKeys: Map<DynamoDBAttribute<Any>, Any> = mapOf()
    )

    // Marker object acting as a value placeholder for potential sort keys
    data class NoValueForSortKeys(
        private val NOT_APPLICABLE_VALUE: Long = 0xBADBADBADBAD
    )

    fun list(
        dynamoDBClient: DynamoDBClient,
        json: Json,
        tableName: String,
        indexAndKeys: IndexAndKeys<Any, Any>,
        ascendingOrder: Boolean,
        pageCount: Int?,
        pageCursor: String?,
        toLastEvaluatedKey: (Map<String, AttributeValue>) -> Map<String, AttributeValue>
    ): Pair<List<Map<String, AttributeValue>>, String?> {

        var exclusiveStartKey = if (!pageCursor.isNullOrBlank()) {
            getExclusiveStartKey(json, pageCursor)
        } else {
            null
        }
        var expectedCount = pageCount ?: DEFAULT_PAGE_SIZE
        var more = true
        val items: MutableList<Map<String, AttributeValue>> = mutableListOf()

        // Loop to get requested page count items, as DynamoDB 'limit' is:
        // "The maximum number of items to evaluate (not necessarily the number of matching items)."
        while (more) {
            val (list, lastEvaluationKey) = if (indexAndKeys.useScan) {
                val listScanBuilder = ScanRequest.builder().init(tableName, indexAndKeys)
                // Items will be unsorted!
                scanWithPagination(
                    dynamoDBClient,
                    listScanBuilder,
                    expectedCount,
                    exclusiveStartKey,
                    toLastEvaluatedKey
                )
            } else {
                val listQueryBuilder = QueryRequest.builder().init(tableName, indexAndKeys, ascendingOrder)
                queryWithPagination(
                    listQueryBuilder,
                    expectedCount,
                    exclusiveStartKey,
                    dynamoDBClient,
                    toLastEvaluatedKey
                )
            }
            items += list
            expectedCount -= list.count()
            exclusiveStartKey = lastEvaluationKey
            more = !lastEvaluationKey.isNullOrEmpty() && (expectedCount > 0)
        }

        val encodedCursor = getEncodedCursor(json, exclusiveStartKey)
        return Pair(items, encodedCursor)
    }

    fun count(
        dynamoDBClient: DynamoDBClient,
        tableName: String,
        indexAndKeys: IndexAndKeys<Any, Any>
    ): Long {

        return if (indexAndKeys.useScan) {
            val countScanBuilder = ScanRequest.builder().init(tableName, indexAndKeys)
            countScanBuilder.select(Select.COUNT)
            // Don't enable consistentRead as with listScan & listQuery

            count(countScanBuilder.build(), dynamoDBClient)
        } else {
            val countQueryBuilder = QueryRequest.builder().init(tableName, indexAndKeys, true)
            countQueryBuilder.select(Select.COUNT)

            count(countQueryBuilder.build(), dynamoDBClient)

        }
    }

    /**
     * [QueryRequest.Builder] extension initializing the common stuff
     */
    private fun QueryRequest.Builder.init(
        tableName: String,
        indexAndKeys: IndexAndKeys<Any, Any>,
        ascendingOrder: Boolean
    ): QueryRequest.Builder {
        tableName(tableName)
            .indexName(indexAndKeys.index?.name)
            .keyConditionExpression(indexAndKeys.keyConditionExpression)
            .expressionAttributeNames(indexAndKeys.expressionNameMap())
            .expressionAttributeValues(indexAndKeys.expressionValueMap())
            .scanIndexForward(ascendingOrder)

        indexAndKeys.filterExpression()?.let { filterExpression(it) }
        return this
    }

    /**
     * [ScanRequest.Builder] extension initializing the common stuff
     */
    private fun ScanRequest.Builder.init(
        tableName: String,
        indexAndKeys: IndexAndKeys<Any, Any>
    ): ScanRequest.Builder {
        tableName(tableName)

        val filterExpression = indexAndKeys.filterExpression()
        if (!filterExpression.isNullOrBlank()) {
            expressionAttributeNames(indexAndKeys.expressionNameMap())
            expressionAttributeValues(indexAndKeys.expressionValueMap())
            filterExpression(filterExpression)
        }
        return this
    }

    fun validateRequest(
        useScan: Boolean,
        allowedScan: Boolean,
        tableCapabilities: TableCapabilities? = null,
        attributesFiltering: AttributesFiltering? = null,
        sortRequest: AttributesSorting? = null
    ) {
        if (FilterType.ENDS_WITH == attributesFiltering?.filterType
            && tableCapabilities?.isSupported(TableCapabilities.TableCapability.FILTERING_ENDS_WITH) == false
        ) {
            throw DataSourceCapabilityException(
                TableCapabilities.TableCapability.FILTERING_ENDS_WITH,
                TableCapabilities.TableCapability.FILTERING_ENDS_WITH.unsupportedMessage
            )
        }

        if (useScan && !allowedScan) {
            throw DataSourceCapabilityException(
                TableCapabilities.TableCapability.FILTERING_ABSENT,
                TableCapabilities.TableCapability.FILTERING_ABSENT.unsupportedMessage
            )
        }

        val sortingExpected = if (useScan) {
            // Ignore sorting with scan as unsupported
            false
        } else {
            when (sortRequest?.sortAttribute) {
                AttributesSorting.SortAttribute.CREATED_TIMESTAMP -> true
                AttributesSorting.SortAttribute.LAST_MODIFIED_TIMESTAMP -> true
                AttributesSorting.SortAttribute.NAME -> true
                AttributesSorting.SortAttribute.NONE -> false
                AttributesSorting.SortAttribute.DEFAULT_SORT -> false
                null -> false
            }
        }

        validateSortingRequest(tableCapabilities, sortingExpected)
    }

    private fun validateSortingRequest(tableCapabilities: TableCapabilities?, sortingExpected: Boolean) {
        if (!sortingExpected || tableCapabilities == null) {
            return
        }

        // Sorting not supported by some tables
        if (tableCapabilities.unsupported.contains(TableCapabilities.TableCapability.SORTING)) {
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
     * @return [IndexAndKeys] helper object to be passed to [list] or [count]
     */
    fun findIndexAndKeysFrom(
        table: TableWithCapabilities,
        potentialKeys: PotentialKeys
    ): IndexAndKeys<Any, Any> {
        potentialKeys.partitionKeys.forEach { potentialPartitionKey ->
            // Search table's indexes for those having it as partition key (PK)
            val potentialIndexes = table.queryCapabilities().indexes.asSequence()
                .filter { index ->
                    index.partitionAttribute.name == potentialPartitionKey.key.name
                }.toSet()
            if (potentialIndexes.isEmpty()) return@forEach

            potentialKeys.sortKeys.forEach { potentialSortKey ->
                // Search previously found indexes for those having it as sort key (SK)
                val foundIndex = potentialIndexes.firstOrNull { index ->
                    index.sortAttribute?.name == potentialSortKey.key.name
                }

                if (foundIndex != null) {
                    // Found an index with both PK & SK, so move other potential keys to filters
                    val filterKeys = moveLeftOverKeys(potentialKeys, potentialPartitionKey)
                    return IndexAndKeys(foundIndex, potentialPartitionKey.toPair(), filterKeys)
                }
            }
            // Found indexes but only with one of the PKs but without any of the provided SKs,
            // so move other potential keys to filters
            val filterKeys = moveLeftOverKeys(potentialKeys, potentialPartitionKey)
            // And take the first as the one to work with
            return IndexAndKeys(potentialIndexes.first(), potentialPartitionKey.toPair(), filterKeys)
        }
        // Found no indexes with any of the provided PKs, move all potential keys to filters
        val filterKeys = moveLeftOverKeys(potentialKeys)
        return IndexAndKeys(filterKeys)
    }

    /**
     * Moves partition keys and sort keys to filters if they were not found in the table's indexes
     *
     * @param potentialKeys
     * @param foundPartitionKey
     * @return the filter keys
     */
    private fun moveLeftOverKeys(
        potentialKeys: PotentialKeys,
        foundPartitionKey: Map.Entry<DynamoDBAttribute<Any>, Any>? = null
    ): Map<DynamoDBAttribute<Any>, Any> {
        val filterKeys = potentialKeys.filterKeys.toMutableMap()
        potentialKeys.partitionKeys.filter { it != foundPartitionKey }
            // Add unused partition keys to the filter keys
            .forEach { potentialPartitionKey ->
                filterKeys += potentialPartitionKey.key to potentialPartitionKey.value
            }
        return filterKeys
    }

    fun getEncodedCursor(json: Json, cursor: Map<String, AttributeValue>?): String? {
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

        throw IllegalArgumentException("Couldn't determine type for 'attributeValue'")
    }

    private fun getDecodedJson(cursor: String) = String(Base64.getDecoder().decode(cursor))

    private fun getDeserializedList(jsonDeserializer: Json, decodedJson: String): List<Map<String, Any?>> =
        try {
            @Suppress("UNCHECKED_CAST")
            jsonDeserializer.fromJsonArray(decodedJson, true) as? List<Map<String, Any?>>
                ?: throw IllegalArgumentException("Couldn't deserialize JSON cursor")
        } catch (e: Json.JsonException) {
            logger.debug("Couldn't deserialize JSON cursor, it's likely invalid")
            throw IllegalArgumentException("Couldn't deserialize JSON cursor")
        }

    fun getExclusiveStartKey(
        jsonDeserializer: Json,
        encodedCursor: String
    ): Map<String, AttributeValue> {
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
     * Helper object holding found index name, partition & optional filter key,
     * along with their respective value.
     *
     * Provides methods generating parameters needed by a [QueryRequest.Builder]
     *
     * @param T1                partition key's type
     * @param T2                filter keys' type
     * @property index          fitting provided keys
     * @property partitionKey   [Pair] with [DynamoDBAttribute] & value
     * @property filterKeys     [Map] of [DynamoDBAttribute] & value
     */
    class IndexAndKeys<T1, T2>(
        val index: Index?,
        private val partitionKey: Pair<DynamoDBAttribute<T1>, T1>?,
        private val filterKeys: Map<DynamoDBAttribute<T2>, T2>
    ) {

        constructor(
            filterKeys: Map<DynamoDBAttribute<T2>, T2>
        ) : this(null, null, filterKeys)

        fun expressionValueMap(): Map<String, AttributeValue> {
            val expressionValueMap = if (partitionKey != null) {
                mutableMapOf(partitionKey.first.toExpressionNameValuePair(partitionKey.second))
            } else {
                mutableMapOf()
            }
            if (filterKeys.isNotEmpty()) {
                filterKeys.forEach { filterKey ->
                    expressionValueMap += filterKey.key.toExpressionNameValuePair(filterKey.value)
                }
            }
            return expressionValueMap
        }

        fun expressionNameMap(): Map<String?, String?> {
            val expressionNameMap = if (partitionKey != null) {
                mutableMapOf<String?, String?>(partitionKey.first.hashName to partitionKey.first.name)
            } else {
                mutableMapOf()
            }
            if (filterKeys.isNotEmpty()) {
                filterKeys.forEach { filterKey ->
                    expressionNameMap += filterKey.key.hashName to filterKey.key.name
                }
            }
            return expressionNameMap
        }

        val keyConditionExpression = if (partitionKey != null) {
            "${partitionKey.first.hashName} = ${partitionKey.first.colonName}"
        } else {
            ""
        }

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

        val useScan = this.partitionKey == null
    }
}