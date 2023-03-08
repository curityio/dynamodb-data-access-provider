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

package io.curity.identityserver.plugin.dynamodb

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException

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
        } catch (e: ResourceNotFoundException) {
            // The table to probe was not found in the database, disable the feature.
            logger.debug(
                "The {} table targeted by the {} GSI was not found into DynamoDB database. " +
                        "The {} feature has been disabled",
                tableName, partitionAndSortIndex.name, featureId(), e
            )
            false
        } catch (e: Exception) {
            // If DynamoDB database is not reachable during plugin initialization,
            // assume that the feature is available by default.
            logger.warn(
                "An exception occurred while checking feature {} into DynamoDB database. " +
                        "The feature is assumed to be available.", featureId(), e
            )
            true
        }
    }
}
