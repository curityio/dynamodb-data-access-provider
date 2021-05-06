/*
 * Copyright (C) 2021 Curity AB. All rights reserved.
 *
 * The contents of this file are the property of Curity AB.
 * You may not copy or use this file, in either source code
 * or executable form, except in compliance with terms
 * set by Curity AB.
 *
 * For further information, please contact Curity AB.
 */

package io.curity.identityserver.plugin.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest

class DynamoDBQuery(
    val indexName: String,
    val keyExpression: String,
    val filterExpression: String,
    val valueMap: Map<String, AttributeValue>,
    val nameMap: Map<String, String>
)

fun QueryRequest.Builder.configureWith(query: DynamoDBQuery)
{
    indexName(query.indexName)
    keyConditionExpression(query.keyExpression)
    filterExpression(query.filterExpression)
    expressionAttributeNames(query.nameMap)
    expressionAttributeValues(query.valueMap)
}

