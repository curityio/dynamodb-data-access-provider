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

import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import org.slf4j.MarkerFactory
import se.curity.identityserver.sdk.datasource.BucketDataAccessProvider
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant

class DynamoDBBucketDataAccessProvider(
    private val _configuration: DynamoDBDataAccessProviderConfiguration,
    private val _dynamoDBClient: DynamoDBClient
) : BucketDataAccessProvider {
    override fun getAttributes(subject: String, purpose: String): Map<String, Any> {
        _logger.debug(MASK_MARKER, "getAttributes with subject: {} , purpose : {}", subject, purpose)

        val request = GetItemRequest.builder()
            .tableName(BucketsTable.name(_configuration))
            .key(BucketsTable.key(subject, purpose))
            .consistentRead(true)
            .build()
        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem()) {
            return mapOf()
        }
        val attributesString = BucketsTable.attributes.optionalFrom(response.item())
            ?: throw SchemaErrorException(
                BucketsTable,
                BucketsTable.attributes
            )
        return _configuration.getJsonHandler().fromJson(attributesString)
    }

    override fun storeAttributes(subject: String, purpose: String, dataMap: Map<String, Any>): Map<String, Any> {
        _logger.debug(MASK_MARKER,
            "storeAttributes with subject: {} , purpose : {} and data : {}",
            subject,
            purpose,
            dataMap
        )

        val attributesString = _configuration.getJsonHandler().toJson(dataMap)
        val now = Instant.now().epochSecond

        val request = UpdateItemRequest.builder()
            .tableName(BucketsTable.name(_configuration))
            .key(BucketsTable.key(subject, purpose))
            .updateExpression(updateExpression(attributesString, now, now))
            .build()

        _dynamoDBClient.updateItem(request)

        return dataMap
    }

    override fun clearBucket(subject: String, purpose: String): Boolean {
        val request = DeleteItemRequest.builder()
            .tableName(BucketsTable.name(_configuration))
            .key(BucketsTable.key(subject, purpose))
            .returnValues(ReturnValue.ALL_OLD)
            .build()

        val response = _dynamoDBClient.deleteItem(request)

        return response.hasAttributes()
    }

    private object BucketsTable : Table("curity-bucket") {
        val subject = StringAttribute("subject")
        val purpose = StringAttribute("purpose")
        val attributes = StringAttribute("attributes")
        val created = NumberLongAttribute("created")
        val updated = NumberLongAttribute("updated")

        fun key(subject: String, purpose: String) = mapOf(
            this.subject.toNameValuePair(subject),
            this.purpose.toNameValuePair(purpose)
        )
    }

    private fun updateExpression(attributesString: String, created: Long, updated: Long) = object : Expression(
        _updateExpressionBuilder
    ) {
        override val values = mapOf(
            BucketsTable.attributes.toExpressionNameValuePair(attributesString),
            BucketsTable.updated.toExpressionNameValuePair(created),
            BucketsTable.created.toExpressionNameValuePair(updated)
        )
    }

    companion object {
        private val _updateExpressionBuilder = ExpressionBuilder(
            "SET #attributes = :attributes, #updated = :updated, #created = if_not_exists(#created, :created)",
            BucketsTable.attributes, BucketsTable.created, BucketsTable.updated
        )
        private val _logger: Logger =
            LoggerFactory.getLogger(DynamoDBBucketDataAccessProvider::class.java)
        private val MASK_MARKER : Marker = MarkerFactory.getMarker("MASK")
    }
}
