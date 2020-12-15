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
package io.curity.dynamoDBDataAccessProvider

import io.curity.dynamoDBDataAccessProvider.configuration.DynamoDBDataAccessProviderDataAccessProviderConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.scim.v2.Meta
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.EXPIRES_AT
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.RESOURCE_TYPE
import se.curity.identityserver.sdk.data.query.ResourceQuery
import se.curity.identityserver.sdk.data.query.ResourceQueryResult
import se.curity.identityserver.sdk.datasource.DeviceDataAccessProvider
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.QueryResponse
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant
import java.util.UUID

class DynamoDBDataAccessProviderDeviceDataAccessProvider(private val dynamoDBClient: DynamoDBClient, private val configuration: DynamoDBDataAccessProviderDataAccessProviderConfig): DeviceDataAccessProvider
{
    private val jsonHandler = configuration.getJsonHandler()

    override fun getBy(deviceId: String, accountId: String): DeviceAttributes?
    {
        _logger.debug("Received request to get device by deviceId :{} and accountId: {}", deviceId, accountId)

        val request = QueryRequest.builder()
                .tableName(tableName)
                .indexName("accountId-deviceId-index")
                .keyConditionExpression("deviceId = :deviceId AND accountId = :accountId")
                .expressionAttributeValues(getKey(deviceId, accountId))
                .limit(1)
                .build()

        val response = dynamoDBClient.query(request)

        if (!response.hasItems() || response.items().isEmpty()) {
            return null
        }

        return toDeviceAttributes(response.items().first())
    }

    override fun getBy(deviceId: String, accountId: String,
                                        attributesEnumeration: ResourceQuery.AttributesEnumeration): ResourceAttributes<*>?
    {
        _logger.debug("Received request to get device by deviceId: {} and accountId: {}", deviceId, accountId)

        val requestBuilder = QueryRequest.builder()
                .tableName(tableName)
                .indexName("accountId-deviceId-index")
                .keyConditionExpression("deviceId = :deviceId AND accountId = :accountId")
                .expressionAttributeValues(getKey(deviceId, accountId))
                .limit(1)

        if (attributesEnumeration.isNeutral) {
            requestBuilder.attributesToGet(attributesEnumeration.attributes)
        }

        val response = dynamoDBClient.query(requestBuilder.build())

        if (!response.hasItems() || response.items().isEmpty()) {
            return null
        }

        return toDeviceAttributes(response.items().first())
    }

    override fun getById(id: String): DeviceAttributes?
    {
        _logger.debug("Received request to get device by id: {}", id)

        val request = GetItemRequest.builder()
                .tableName(tableName)
                .key(id.toKey("id"))
                .build()

        val response = dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        return toDeviceAttributes(response.item())
    }

    override fun getById(id: String, attributesEnumeration: ResourceQuery.AttributesEnumeration): ResourceAttributes<*>?
    {
        _logger.debug("Received request to get device by id: {}", id)
        val requestBuilder = GetItemRequest.builder()
                .tableName(tableName)
                .key(id.toKey("id"))

        if (attributesEnumeration.isNeutral) {
            requestBuilder.attributesToGet(attributesEnumeration.attributes)
        }

        val response = dynamoDBClient.getItem(requestBuilder.build())

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        return toDeviceAttributes(response.item())
    }

    override fun getByAccountId(accountId: String): List<DeviceAttributes>
    {
        _logger.debug("Received request to get devices by accountId: {}", accountId)

        val result = mutableListOf<DeviceAttributes>()
        var lastEvaluatedKey: Map<String, AttributeValue>? = null

        do {
            val response = queryForGetByAccountId(accountId, lastEvaluatedKey, null)
            response.items().forEach{ item -> result.add(toDeviceAttributes(item)) }
            lastEvaluatedKey = response.lastEvaluatedKey()
        } while (response.hasLastEvaluatedKey() && !lastEvaluatedKey.isNullOrEmpty())

        return result
    }

    private fun queryForGetByAccountId(accountId: String, startKey: Map<String, AttributeValue>?, attributesEnumeration: ResourceQuery.AttributesEnumeration?): QueryResponse {
        val requestBuilder = QueryRequest.builder()
                .tableName(tableName)
                .indexName("accountId-deviceId-index")
                .keyConditionExpression("accountId = :accountId")
                .expressionAttributeValues(mapOf(Pair(":accountId", accountId.toAttributeValue())))

        if (startKey != null) {
            requestBuilder.exclusiveStartKey(startKey)
        }

        if (attributesEnumeration != null && !attributesEnumeration.isNeutral) {
            requestBuilder.attributesToGet(attributesEnumeration.attributes)
        }

        return dynamoDBClient.query(requestBuilder.build())
    }

    override fun getByAccountId(accountId: String, attributesEnumeration: ResourceQuery.AttributesEnumeration): List<ResourceAttributes<*>>
    {
        _logger.debug("Received request to get devices by accountId: {}", accountId)

        val result = mutableListOf<DeviceAttributes>()
        var lastEvaluatedKey: Map<String, AttributeValue>? = null

        do {
            val response = queryForGetByAccountId(accountId, lastEvaluatedKey, attributesEnumeration)
            response.items().forEach{ item -> result.add(toDeviceAttributes(item)) }
            lastEvaluatedKey = response.lastEvaluatedKey()
        } while (response.hasLastEvaluatedKey() && !lastEvaluatedKey.isNullOrEmpty())

        return result
    }

    override fun create(deviceAttributes: DeviceAttributes)
    {
        _logger.debug("Received request to create device by deviceId: {}", deviceAttributes.deviceId)

        val item = mutableMapOf<String, AttributeValue>()
        val deviceAttributesMap = deviceAttributes.asMap()

        deviceFields.forEach { deviceField ->
            if (deviceAttributes[deviceField] != null) {
                item[deviceField] = deviceAttributes[deviceField].value.toString().toAttributeValue()
                deviceAttributesMap.remove(deviceField)
            }
        }

        if (deviceAttributes.expiresAt != null) {
            item[EXPIRES_AT] = deviceAttributes.expiresAt.epochSecond.toAttributeValue()
            deviceAttributesMap.remove(EXPIRES_AT)
        }

        if (deviceAttributes.meta != null) {
            item[Meta.CREATED] = if (deviceAttributes.meta.created != null) {
                deviceAttributes.meta.created.epochSecond.toAttributeValue()
            } else {
                Instant.now().epochSecond.toAttributeValue()
            }
            deviceAttributesMap.remove(Meta.CREATED)

            if (deviceAttributes.meta.lastModified != null) {
                item[Meta.LAST_MODIFIED] = deviceAttributes.meta.lastModified.epochSecond.toAttributeValue()
                deviceAttributesMap.remove(Meta.LAST_MODIFIED)
            }
        }
        deviceAttributesMap.remove(DeviceAttributes.META)

        if (item["id"] == null) {
            item["id"] = UUID.randomUUID().toString().toAttributeValue()
        }

        if (deviceAttributesMap.isNotEmpty()) {
            item[DeviceAttributes.ATTRIBUTES] = jsonHandler.fromAttributes(Attributes.fromMap(deviceAttributesMap)).toAttributeValue()
        }

        val request = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build()

        dynamoDBClient.putItem(request)
    }

    override fun update(deviceAttributes: DeviceAttributes)
    {
        _logger.debug("Received request to update device by deviceId: {}", deviceAttributes.deviceId)

        // TODO for now only update alias
        val request = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(deviceAttributes.id.toKey("id"))
                .updateExpression("SET ${DeviceAttributes.ALIAS} = :alias")
                .expressionAttributeValues(mapOf(Pair(":alias", deviceAttributes.alias.toAttributeValue())))
                .build()

        dynamoDBClient.updateItem(request)
    }

    override fun delete(id: String)
    {
        _logger.debug("Received request to update device by id: {}", id)

        val request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(id.toKey("id"))
                .build()

        dynamoDBClient.deleteItem(request)
    }

    override fun delete(deviceId: String, accountId: String)
    {
        _logger.debug("Received request to delete device by deviceId: {} and accountId: {}", deviceId, accountId)

        val item = getBy(deviceId, accountId)
        if (item != null) {
            delete(item.id)
        }
    }

    override fun getAll(startIndex: Long, count: Long): ResourceQueryResult
    {
        //TODO implement paging and proper handling of DynamoDB paging
        _logger.debug("Received request to get all devices with startIndex: {} and count: {}", startIndex, count)

        val request = ScanRequest.builder()
                .tableName(tableName)
                .build()

        val response = dynamoDBClient.scan(request)

        return ResourceQueryResult(
                response.items().map { item -> toDeviceAttributes(item) },
                response.count().toLong(),
                0,
                response.count().toLong()
        )
    }

    private fun getKey(deviceId: String, accountId: String): Map<String, AttributeValue> = mapOf(
            Pair("deviceId", deviceId.toAttributeValue()),
            Pair("accountId", accountId.toAttributeValue())
    )

    private fun toDeviceAttributes(item: Map<String, AttributeValue>): DeviceAttributes = DeviceAttributes.createDevice(
                item["id"]?.s(),
                item["deviceId"]?.s(),
                item["accountId"]?.s(),
                item["externalId"]?.s(),
                item["alias"]?.s(),
                item["formFactor"]?.s(),
                item["deviceType"]?.s(),
                item["owner"]?.s(),
                Instant.ofEpochSecond(item["expiresAt"]?.s()?.toLong() ?: -1L),
                Meta.of(RESOURCE_TYPE)
                        .withCreated(Instant.ofEpochSecond(item["created"]?.s()?.toLong() ?: -1L))
                        .withLastModified(Instant.ofEpochSecond(item["lastModified"]?.s()?.toLong() ?: -1L))
        )


    companion object
    {
        private val _logger: Logger = LoggerFactory.getLogger(DynamoDBDataAccessProviderCredentialDataAccessProvider::class.java)
        private const val tableName = "curity-devices"
        private val deviceFields = setOf(
                ResourceAttributes.ID,
                DeviceAttributes.DEVICE_ID,
                DeviceAttributes.ACCOUNT_ID,
                ResourceAttributes.EXTERNAL_ID,
                DeviceAttributes.ALIAS,
                DeviceAttributes.FORM_FACTOR,
                DeviceAttributes.DEVICE_TYPE,
                DeviceAttributes.OWNER,
                DeviceAttributes.ATTRIBUTES
        )
    }
}
