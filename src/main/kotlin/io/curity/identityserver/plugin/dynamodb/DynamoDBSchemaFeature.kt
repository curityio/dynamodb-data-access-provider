package io.curity.identityserver.plugin.dynamodb

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest

interface DynamoDBSchemaFeatureCheck {
    fun featureId(): String
    fun checkFeature(dynamoDBClient: DynamoDbClient): Boolean
}

class DynamoDBGlobalSecondaryIndexFeatureCheck(
    val tableName: String,
    private val partitionAndSortIndex: PartitionAndSortIndex<*, *>
) : DynamoDBSchemaFeatureCheck {
    companion object {
        fun buildFeatureId(tableName: String, partitionAndSortIndex: PartitionAndSortIndex<*, *>) =
            tableName + ":" + partitionAndSortIndex.name
    }

    override fun featureId(): String {
        return buildFeatureId(tableName, partitionAndSortIndex)
    }

    override fun checkFeature(dynamoDBClient: DynamoDbClient): Boolean {
        val request = DescribeTableRequest.builder().tableName(tableName).build()
        val response = dynamoDBClient.describeTable(request)
        return response.table().globalSecondaryIndexes().any { it.indexName() == partitionAndSortIndex.name }
    }
}
