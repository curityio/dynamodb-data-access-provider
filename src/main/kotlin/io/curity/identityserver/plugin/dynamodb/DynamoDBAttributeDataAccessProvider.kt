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

import se.curity.identityserver.sdk.attribute.AttributeTableView
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.datasource.AttributeDataAccessProvider
import software.amazon.awssdk.services.dynamodb.model.QueryRequest

class DynamoDBAttributeDataAccessProvider(private val dynamoDBClient: DynamoDBClient): AttributeDataAccessProvider
{
    override fun getAttributes(subject: String): AttributeTableView
    {
        // TODO should implement dynamodb paging

        val request = QueryRequest.builder()
                .tableName("curity-links")
                .indexName("list-links-index")
                .keyConditionExpression("localAccountId = :localAccountId")
                .expressionAttributeValues(mapOf(Pair(":localAccountId", subject.toAttributeValue())))
                .build()

        val response = dynamoDBClient.query(request)

        if (!response.hasItems() || response.items().isEmpty()) {
            return AttributeTableView.empty()
        }

        val result = mutableListOf<Attributes>()
        response.items().forEach {item -> result.add(Attributes.fromMap(item.mapValues { v -> v.value.s() }))}

        return AttributeTableView.ofAttributes(result)
    }
}
