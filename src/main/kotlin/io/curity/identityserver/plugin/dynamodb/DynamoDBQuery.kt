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

data class DynamoDBQuery(
    val indexName: String?,
    val keyExpression: String,
    val filterExpression: String,
    val valueMap: Map<String, AttributeValue>,
    val nameMap: Map<String, String>
)

fun QueryRequest.Builder.configureWith(query: DynamoDBQuery): QueryRequest.Builder {
    if (query.indexName != null) {
        indexName(query.indexName)
    }
    keyConditionExpression(query.keyExpression)
    if (query.filterExpression.isNotBlank()) {
        filterExpression(query.filterExpression)
    }
    if (query.nameMap.isNotEmpty()) {
        expressionAttributeNames(query.nameMap)
    }
    if (query.valueMap.isNotEmpty()) {
        expressionAttributeValues(query.valueMap)
    }
    return this
}

