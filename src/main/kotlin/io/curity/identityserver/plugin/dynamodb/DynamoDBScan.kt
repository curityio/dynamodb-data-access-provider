package io.curity.identityserver.plugin.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

class DynamoDBScan(
    val filterExpression: String,
    val valueMap: Map<String, AttributeValue>,
    val nameMap: Map<String, String>
)

fun ScanRequest.Builder.configureWith(query: DynamoDBScan): ScanRequest.Builder
{
    if (query.filterExpression.isNotBlank())
    {
        filterExpression(query.filterExpression)
    }
    if (query.nameMap.isNotEmpty())
    {
        expressionAttributeNames(query.nameMap)
    }
    if (query.valueMap.isNotEmpty())
    {
        expressionAttributeValues(query.valueMap)
    }
    return this
}
