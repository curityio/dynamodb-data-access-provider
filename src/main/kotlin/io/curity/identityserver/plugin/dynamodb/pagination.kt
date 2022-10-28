/*
 *  Copyright 2021 Curity AB
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

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

const val DEFAULT_PAGE_SIZE = 50
const val MAXIMUM_PAGE_SIZE = DEFAULT_PAGE_SIZE

// Returns a sequence with the items produced by a query, handling pagination if needed
fun querySequence(request: QueryRequest, client: DynamoDBClient) = sequence {
    var response = client.query(request)
    if (response.hasItems()) {
        response.items().forEach {
            yield(it)
        }
        while (response.hasLastEvaluatedKey()) {
            val newRequest = request.toBuilder().exclusiveStartKey(response.lastEvaluatedKey()).build()
            response = client.query(newRequest)
            if (response.hasItems()) {
                response.items().forEach {
                    yield(it)
                }
            }
        }
    }
}

data class PartialListResult(
    val items: List<Map<String, AttributeValue>>,
    val lastEvaluationKey: Map<String, AttributeValue>?
)

// Returns a pair with the next page's items as a list, along with the last evaluation key, using a query request
fun queryPartialList(
    requestBuilder: QueryRequest.Builder,
    limit: Int,
    exclusiveStartKey: Map<String, AttributeValue>?,
    client: DynamoDBClient,
    // Converts an item returned from DynamoDB into a lastEvaluatedKey which must only contain the key attributes.
    toLastEvaluatedKey: (Map<String, AttributeValue>) -> Map<String, AttributeValue>
): PartialListResult {
    requestBuilder.limit(limit)

    var lastEvaluatedKey = exclusiveStartKey
    val items: MutableList<Map<String, AttributeValue>> = mutableListOf()

    do {
        if (lastEvaluatedKey != null) {
            requestBuilder.exclusiveStartKey(lastEvaluatedKey)
        }

        val request = requestBuilder.build()
        val response = client.query(request)
        val number = limit - items.size
        items += response.items().asSequence().take(number)

        if (response.items().size > number) {
            // There were more items than needed. The end of the response was skipped.
            // The last evaluated key should be set to the last item added to the results.
            lastEvaluatedKey = toLastEvaluatedKey(items.last())
            break
        }

        lastEvaluatedKey = if (response.hasLastEvaluatedKey()) response.lastEvaluatedKey() else null

        if (items.size == limit && request.filterExpression().isNullOrEmpty()) {
            // We reach the limit, and we have no filter (to return all rows for the given key),
            // the response.lastEvaluatedKey is the good one to return.
            break
        }

        // Either we have not yet reached the limit or we reached it but there is a filterExpression.
        // In the latter case, DynamoDB may have returned a lastEvaluatedKey, but we are not sure that
        // the remaining keys will match the condition.
        // We go on, even if items.size == limit to know if it really remains more matching rows and avoid wrongly
        // returning a lastEvaluatedKey.
    } while (lastEvaluatedKey != null)

    return PartialListResult(
        items,
        lastEvaluatedKey
    )
}

// Returns a sequence with the items produced by a scan, handling pagination if needed
fun scanSequence(request: ScanRequest, client: DynamoDBClient) = sequence {
    var response = client.scan(request)
    if (response.hasItems()) {
        response.items().forEach {
            yield(it)
        }
        while (response.hasLastEvaluatedKey()) {
            val newRequest = request.toBuilder().exclusiveStartKey(response.lastEvaluatedKey()).build()
            response = client.scan(newRequest)
            if (response.hasItems()) {
                response.items().forEach {
                    yield(it)
                }
            }
        }
    }
}

fun scanPartialList(
    dynamoDBClient: DynamoDBClient,
    listScanBuilder: ScanRequest.Builder,
    limit: Int,
    exclusiveStartKey: Map<String, AttributeValue>?,
    // Converts an item returned from DynamoDB into a lastEvaluatedKey which must only contain the key attributes.
    toLastEvaluatedKey: (Map<String, AttributeValue>) -> Map<String, AttributeValue>
): PartialListResult {
    // Don't enable consistentRead: to be consistent with listQuery
    // Also: "strongly consistent reads are twice the cost of eventually consistent reads"

    // Items will be unsorted!
    listScanBuilder.limit(limit)

    var lastEvaluatedKey = exclusiveStartKey
    val items: MutableList<Map<String, AttributeValue>> = mutableListOf()

    do {
        if (lastEvaluatedKey != null) {
            listScanBuilder.exclusiveStartKey(lastEvaluatedKey)
        }

        val request = listScanBuilder.build()
        val response = dynamoDBClient.scan(request)
        val number = limit - items.size
        items += response.items().asSequence().take(number)

        if (response.items().size > number) {
            // There were more items than needed. The end of the response was skipped.
            // The last evaluated key should be set to the last item added to the results.
            lastEvaluatedKey = toLastEvaluatedKey(items.last())
            break
        }

        lastEvaluatedKey = if (response.hasLastEvaluatedKey()) response.lastEvaluatedKey() else null

        if (items.size == limit && request.filterExpression().isNullOrEmpty()) {
            // We reach the limit, and we have no filter (to return all rows),
            // the response.lastEvaluationKey is the good one to return.
            break
        }

        // Either we have not yet reached the limit or we reached it but there is a filterExpression.
        // In the latter case, DynamoDB may have returned a lastEvaluationKey, but we are not sure that
        // the remaining keys will match the condition.
        // We go on, even if items.size == limit to know if it really remains more matching rows and avoid wrongly
        // returning a lastEvaluatedKey.
    } while (lastEvaluatedKey != null)

    return PartialListResult(
        items,
        lastEvaluatedKey
    )
}

fun count(request: QueryRequest, client: DynamoDBClient): Long {
    var response = client.query(request)
    var counter = response.count().toLong()
    while (response.hasLastEvaluatedKey()) {
        val newRequest = request.toBuilder().exclusiveStartKey(response.lastEvaluatedKey()).build()
        response = client.query(newRequest)
        counter += response.count()
    }

    return counter
}

fun count(request: ScanRequest, client: DynamoDBClient): Long {
    var response = client.scan(request)
    var counter = response.count().toLong()
    while (response.hasLastEvaluatedKey()) {
        val newRequest = request.toBuilder().exclusiveStartKey(response.lastEvaluatedKey()).build()
        response = client.scan(newRequest)
        counter += response.count()
    }

    return counter
}

