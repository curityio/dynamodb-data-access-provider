package io.curity.identityserver.plugin.dynamodb

import org.slf4j.Logger
import org.slf4j.LoggerFactory
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
        val logger: Logger = LoggerFactory.getLogger(DynamoDBGlobalSecondaryIndexFeatureCheck::class.java)

        fun buildFeatureId(tableName: String, partitionAndSortIndex: PartitionAndSortIndex<*, *>) =
            tableName + ":" + partitionAndSortIndex.name
    }

    override fun featureId(): String {
        return buildFeatureId(tableName, partitionAndSortIndex)
    }

    override fun checkFeature(dynamoDBClient: DynamoDbClient): Boolean {
        return try {
            val request = DescribeTableRequest.builder().tableName(tableName).build()
            val response = dynamoDBClient.describeTable(request)
            response.table().globalSecondaryIndexes().any { it.indexName() == partitionAndSortIndex.name }
        } catch (e: Exception) {
            // If DynamoDB database is not reachable during plugin initialization,
            // assumes that the feature is available by default.
            logger.warn(
                "An exception occurred while checking feature ${featureId()} into DynamoDB database. " +
                        "The feature is assumed to be available.", e
            )
            true
        }
    }
}
