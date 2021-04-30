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
import se.curity.identityserver.sdk.attribute.Attribute
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.scim.v2.Meta
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.ATTRIBUTES
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.EXPIRES_AT
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.META
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.RESOURCE_TYPE
import se.curity.identityserver.sdk.data.query.ResourceQuery
import se.curity.identityserver.sdk.data.query.ResourceQueryResult
import se.curity.identityserver.sdk.datasource.DeviceDataAccessProvider
import se.curity.identityserver.sdk.errors.ConflictException
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant
import java.util.UUID

class DynamoDBDeviceDataAccessProvider(
    private val _dynamoDBClient: DynamoDBClient,
    configuration: DynamoDBDataAccessProviderConfiguration
) : DeviceDataAccessProvider
{
    private val _jsonHandler = configuration.getJsonHandler()

    object DeviceTable : Table("curity-devices")
    {
        val pk = KeyStringAttribute("pk")
        val id = StringAttribute("id")
        val accountId = StringAttribute("accountId")
        val deviceId = StringAttribute("deviceId")
        val externalId = StringAttribute("externalId")
        val alias = StringAttribute("alias")
        val formFactor = StringAttribute("formFactor")
        val deviceType = StringAttribute("deviceType")
        val owner = StringAttribute("owner")
        val attributes = StringAttribute("attributes")
        val expires = NumberLongAttribute("expires")
        val created = NumberLongAttribute("created")
        val updated = NumberLongAttribute("updated")

        val accountIdIndex = Index("accountId-deviceId-index", accountId)
        val accountIdDeviceIdIndex = Index2("accountId-deviceId-index", accountId, deviceId)
    }

    private fun DeviceAttributes.toItem(): MutableDynamoDBItem
    {
        val now = Instant.now()
        val created = now

        val item = mutableMapOf<String, AttributeValue>()
        val deviceAttributesAsMap = this.asMap()
        mappings.forEach { it.addToItem(item, this, deviceAttributesAsMap) }

        val id = deviceAttributesAsMap.remove(ResourceAttributes.ID) as? String ?: UUID.randomUUID().toString()
        DeviceTable.id.addTo(item, id)

        if (deviceAttributesAsMap.isNotEmpty())
        {
            deviceAttributesAsMap.remove(META)
            DeviceTable.attributes.addTo(item, _jsonHandler.fromAttributes(Attributes.fromMap(deviceAttributesAsMap)))
        }

        DeviceTable.created.addTo(item, created.epochSecond)
        DeviceTable.updated.addTo(item, now.epochSecond)

        return item
    }

    private fun DynamoDBItem.toDeviceAttributes(attributesEnumeration: ResourceQuery.AttributesEnumeration? = null)
            : DeviceAttributes
    {
        val item = this
        val attributeList = mutableListOf<Attribute>()
        attributeList.add(Attribute.of(ResourceAttributes.ID, DeviceTable.id.from(item)))
        if (attributesEnumeration == null || attributesEnumeration.keepAttribute(ResourceAttributes.META))
        {
            val created = DeviceTable.created.from(item)
            val modified = DeviceTable.updated.from(item)
            attributeList.add(
                Attribute.of(
                    META, Meta.of(RESOURCE_TYPE)
                        .withCreated(Instant.ofEpochSecond(created))
                        .withLastModified(Instant.ofEpochSecond(modified))
                )
            )
        }

        mappings.forEach {
            it.toAttribute(this)?.let { attribute -> attributeList.add(attribute) }
        }

        DeviceTable.attributes.fromOpt(item)?.let {
            attributeList.addAll(_jsonHandler.toAttributes(it))
        }

        return DeviceAttributes.of(attributeList)
    }

    override fun create(deviceAttributes: DeviceAttributes)
    {
        _logger.debug("Received request to create device by deviceId: {}", deviceAttributes.deviceId)

        val transactionItems = mutableListOf<TransactWriteItem>()

        // Add main item
        val mainItem = deviceAttributes.toItem()
        val id = DeviceTable.id.from(mainItem)
        DeviceTable.pk.addTo(mainItem, idKey(id))

        transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(DeviceTable.name)
                    it.conditionExpression("attribute_not_exists(${DeviceTable.pk})")
                    it.item(mainItem)
                }
                .build()
        )

        // Add secondary item
        val secondaryItem = mutableMapOf(
            DeviceTable.id.toNameValuePair(id)
        )
        DeviceTable.pk.addTo(secondaryItem, accountIdDeviceIdKey(deviceAttributes))
        transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(DeviceTable.name)
                    it.conditionExpression("attribute_not_exists(${DeviceTable.pk})")
                    it.item(secondaryItem)
                }
                .build()
        )

        val request = TransactWriteItemsRequest.builder()
            .transactItems(transactionItems)
            .build()

        try
        {
            _dynamoDBClient.transactionWriteItems(request)
        }catch (ex: Exception)
        {
            if (ex.isTransactionCancelledDueToConditionFailure())
            {
                throw ConflictException(
                    "Unable to create device as uniqueness check failed"
                )
            }
            throw ex
        }
    }

    private fun delete(id: String, accountId: String?, deviceId: String)
    {
        val transactionItems = mutableListOf<TransactWriteItem>()

        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(DeviceTable.name)
                    it.key(mapOf(DeviceTable.pk.toNameValuePair(idKey(id))))
                }
                .build()
        )

        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(DeviceTable.name)
                    it.key(mapOf(DeviceTable.pk.toNameValuePair(accountIdDeviceIdKey(accountId, deviceId))))
                    it.conditionExpression("${DeviceTable.id.hashName} = ${DeviceTable.id.colonName}")
                    it.expressionAttributeValues(mapOf(DeviceTable.id.toExpressionNameValuePair(id)))
                    it.expressionAttributeNames(mapOf(DeviceTable.id.toNamePair()))
                }
                .build()
        )

        val request = TransactWriteItemsRequest.builder()
            .transactItems(transactionItems)
            .build()

        _dynamoDBClient.transactionWriteItems(request)
    }

    override fun delete(id: String)
    {
        _logger.debug("Received request to update device by id: {}", id)

        val deviceAttributes = getById(id) ?: return
        val accountId = deviceAttributes.accountId
        val deviceId = deviceAttributes.deviceId

        delete(id, accountId, deviceId)
    }

    override fun delete(deviceId: String, accountId: String)
    {
        _logger.debug("Received request to delete device by deviceId: {} and accountId: {}", deviceId, accountId)

        val item = getBy(deviceId, accountId) ?: return
        delete(item.id, accountId, deviceId)
    }

    override fun update(deviceAttributes: DeviceAttributes)
    {
        val now = Instant.now()

        val deviceAttributesAsMap = deviceAttributes.asMap()
        val updateBuilder = UpdateExpressionsBuilder()
        mappings.forEach { it.addToUpdateBuilder(deviceAttributes, deviceAttributesAsMap, updateBuilder) }

        deviceAttributesAsMap.remove(ResourceAttributes.ID)

        if (deviceAttributesAsMap.isNotEmpty())
        {
            deviceAttributesAsMap.remove(META)
            updateBuilder.update(
                DeviceTable.attributes,
                _jsonHandler.fromAttributes(Attributes.fromMap(deviceAttributesAsMap))
            )
        } else
        {
            updateBuilder.update(DeviceTable.attributes, null)
        }

        // updateBuilder.update(DeviceTable.updated, now.epochSecond)

        updateBuilder.onlyIf(DeviceTable.deviceId, deviceAttributes.deviceId)
        deviceAttributes.accountId?.let {
            updateBuilder.onlyIf(DeviceTable.accountId, it)
        }

        val updateRequest = UpdateItemRequest.builder()
            .tableName(DeviceTable.name)
            .apply {
                updateBuilder.applyTo(this)
            }
            .key(mapOf(DeviceTable.pk.toNameValuePair(idKey(deviceAttributes.id))))
            .build()

        _dynamoDBClient.updateItem(updateRequest)
    }

    override fun getBy(
        deviceId: String, accountId: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): DeviceAttributes?
    {
        _logger.debug("Received request to get device by deviceId: {} and accountId: {}", deviceId, accountId)

        val index = DeviceTable.accountIdDeviceIdIndex
        val requestBuilder = QueryRequest.builder()
            .tableName(DeviceTable.name)
            .indexName(index.name)
            .keyConditionExpression(index.keyConditionExpression)
            .expressionAttributeValues(index.expressionValueMap(accountId, deviceId))
            .expressionAttributeNames(index.expressionNameMap)
            .limit(1)

        if (!attributesEnumeration.isNeutral && attributesEnumeration is ResourceQuery.Inclusions)
        {
            requestBuilder.projectionExpression(attributesEnumeration.attributes.joinToString(","))
        }

        val response = _dynamoDBClient.query(requestBuilder.build())

        if (!response.hasItems() || response.items().isEmpty())
        {
            return null
        }

        return response.items().first().toDeviceAttributes(attributesEnumeration)
    }

    override fun getByAccountId(accountId: String?): List<DeviceAttributes>
    {
        _logger.debug("Received request to get device by accountId: {}", accountId)

        if (accountId == null)
        {
            return listOf()
        }

        val index = DeviceTable.accountIdIndex
        val request = QueryRequest.builder()
            .tableName(DeviceTable.name)
            .indexName(index.name)
            .keyConditionExpression(index.expression)
            .expressionAttributeValues(index.expressionValueMap(accountId))
            .expressionAttributeNames(index.expressionNameMap)
            .build()

        return querySequence(request, _dynamoDBClient)
            .map {
                it.toDeviceAttributes()
            }
            .toList()
    }

    override fun getBy(deviceId: String, accountId: String) =
        getBy(deviceId, accountId, ResourceQuery.Exclusions.none())

    override fun getById(id: String): DeviceAttributes?
    {
        _logger.debug("Received request to get device by id: {}", id)

        val request = GetItemRequest.builder()
            .tableName(DeviceTable.name)
            .key(mapOf(DeviceTable.pk.toNameValuePair(idKey(id))))
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty())
        {
            return null
        }

        return response.item().toDeviceAttributes()
    }

    override fun getById(id: String, attributesEnumeration: ResourceQuery.AttributesEnumeration): ResourceAttributes<*>?
    {
        _logger.debug("Received request to get device by id: {}", id)
        val requestBuilder = GetItemRequest.builder()
            .tableName(DeviceTable.name)
            .key(mapOf(DeviceTable.pk.toNameValuePair(idKey(id))))

        if (!attributesEnumeration.isNeutral && attributesEnumeration is ResourceQuery.Inclusions)
        {
            requestBuilder.attributesToGet(attributesEnumeration.attributes)
        }

        val response = _dynamoDBClient.getItem(requestBuilder.build())

        if (!response.hasItem() || response.item().isEmpty())
        {
            return null
        }

        return response.item().toDeviceAttributes()
    }

    override fun getByAccountId(accountId: String, attributesEnumeration: ResourceQuery.AttributesEnumeration):
            List<DeviceAttributes>
    {
        _logger.debug("Received request to get devices by accountId: {}", accountId)

        val index = DeviceTable.accountIdIndex
        val requestBuilder = QueryRequest.builder()
            .tableName(DeviceTable.name)
            .indexName(index.name)
            .keyConditionExpression(index.expression)
            .expressionAttributeValues(index.expressionValueMap(accountId))
            .expressionAttributeNames(index.expressionNameMap)

        if (!attributesEnumeration.isNeutral && attributesEnumeration is ResourceQuery.Inclusions)
        {
            requestBuilder.attributesToGet(attributesEnumeration.attributes)
        }

        return querySequence(requestBuilder.build(), _dynamoDBClient)
            .map {
                it.toDeviceAttributes(attributesEnumeration)
            }
            .toList()
    }

    override fun getAll(startIndex: Long, count: Long): ResourceQueryResult
    {
        _logger.debug("Received request to get all devices with startIndex: {} and count: {}", startIndex, count)

        val validatedStartIndex = startIndex.intOrThrow("startIndex")
        val validatedCount = count.intOrThrow("count")

        val request = ScanRequest.builder()
            .tableName(DeviceTable.name)
            .indexName(DeviceTable.accountIdDeviceIdIndex.name)
            .build()

        val all = scanSequence(request, _dynamoDBClient).toList()

        val page = all.drop(validatedStartIndex).take(validatedCount)

        return ResourceQueryResult(
            page.map { item -> item.toDeviceAttributes() },
            all.size.toLong(),
            startIndex,
            count
        )
    }

    companion object
    {
        private val _logger: Logger = LoggerFactory.getLogger(DynamoDBCredentialDataAccessProvider::class.java)

        private fun accountIdDeviceIdKey(accountId: String?, deviceId: String) =
            "accountIdDeviceId#${accountId}_${deviceId}"

        private fun accountIdDeviceIdKey(deviceAttributes: DeviceAttributes) =
            accountIdDeviceIdKey(deviceAttributes.accountId, deviceAttributes.deviceId)

        private fun idKey(id: String) = "id#$id"

        private val deviceFields = setOf(
            ResourceAttributes.ID,
            DeviceAttributes.DEVICE_ID,
            DeviceAttributes.ACCOUNT_ID,
            ResourceAttributes.EXTERNAL_ID,
            DeviceAttributes.ALIAS,
            DeviceAttributes.FORM_FACTOR,
            DeviceAttributes.DEVICE_TYPE,
            DeviceAttributes.OWNER,
            ATTRIBUTES
        )

        // TODO
        private val mappings = listOf(
            AttributeMapping(DeviceTable.deviceId, DeviceAttributes.DEVICE_ID, false),
            AttributeMapping(DeviceTable.accountId, DeviceAttributes.ACCOUNT_ID, true),
            AttributeMapping(DeviceTable.externalId, ResourceAttributes.EXTERNAL_ID, true),
            AttributeMapping(DeviceTable.alias, DeviceAttributes.ALIAS, true),
            AttributeMapping(DeviceTable.formFactor, DeviceAttributes.FORM_FACTOR, true),
            AttributeMapping(DeviceTable.deviceType, DeviceAttributes.DEVICE_TYPE, true),
            AttributeMapping(DeviceTable.owner, DeviceAttributes.OWNER, true),
            AttributeMapping(DeviceTable.expires, EXPIRES_AT, true,
                { it.expiresAt?.epochSecond },
                {
                    se.curity.identityserver.sdk.attribute.AttributeValue.formatAsStringAttributeValue(
                        Instant.ofEpochSecond(
                            it
                        )
                    )
                })
        )
    }
}

private data class AttributeMapping<T>(
    val dynamoAttribute: DynamoDBAttribute<T>,
    val deviceAttributeName: String,
    val optional: Boolean,
    val retriever: ((DeviceAttributes) -> T?)? = null,
    val toAttributeValue: ((T) -> Any)? = null
)
{
    fun addToUpdateBuilder(
        deviceAttributes: DeviceAttributes,
        deviceAttributesMap: MutableMap<String, Any>,
        updateBuilder: UpdateExpressionsBuilder
    )
    {
        if (retriever != null)
        {
            val value = retriever.invoke(deviceAttributes)
            deviceAttributesMap.remove(deviceAttributeName)
            if (value == null && !optional)
            {
                throw Exception("TODO")
            } else
            {
                updateBuilder.update(dynamoAttribute, value)
            }
        } else
        {
            val value = deviceAttributesMap.remove(deviceAttributeName)
            if (value == null && !optional)
            {
                throw Exception("TODO")
            } else
            {
                updateBuilder.update(dynamoAttribute, value?.let { dynamoAttribute.cast(it) })
            }
        }
    }

    fun addToItem(
        item: MutableDynamoDBItem,
        deviceAttributes: DeviceAttributes,
        deviceAttributesMap: MutableMap<String, Any>
    )
    {
        if (retriever != null)
        {
            val value = retriever.invoke(deviceAttributes)
            deviceAttributesMap.remove(deviceAttributeName)
            if (value == null)
            {
                if (!optional)
                {
                    throw Exception("TODO")
                }
            } else
            {
                dynamoAttribute.addTo(item, value)
            }
        } else
        {
            val value = deviceAttributesMap.remove(deviceAttributeName)
            if (value == null)
            {
                if (!optional)
                {
                    throw Exception("TODO")
                }
            } else
            {
                dynamoAttribute.addToAny(item, value)
            }
        }
    }

    fun toAttribute(item: DynamoDBItem): Attribute? =
        if (optional)
        {
            dynamoAttribute.fromOpt(item)?.let {
                Attribute.of(
                    deviceAttributeName,
                    se.curity.identityserver.sdk.attribute.AttributeValue.of(toAttributeValue?.invoke(it) ?: it)
                )
            }
        } else
        {
            dynamoAttribute.from(item).let {
                Attribute.of(
                    deviceAttributeName,
                    se.curity.identityserver.sdk.attribute.AttributeValue.of(toAttributeValue?.invoke(it) ?: it)
                )
            }
        }
}


