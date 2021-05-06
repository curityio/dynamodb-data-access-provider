package io.curity.identityserver.plugin.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

class DynamoDBScan(
    val filterExpression: String,
    val valueMap: Map<String, AttributeValue>,
    val nameMap: Map<String, String>
)

fun ScanRequest.Builder.configureWith(query: DynamoDBScan)
{
    filterExpression(query.filterExpression)
    expressionAttributeNames(query.nameMap)
    expressionAttributeValues(query.valueMap)
}
