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

import io.curity.identityserver.plugin.dynamodb.DynamoDBDeviceDataAccessProvider.Companion.computePkFromAccountIdAndDeviceId
import io.curity.identityserver.plugin.dynamodb.DynamoDBDeviceDataAccessProvider.Companion.computePkFromId
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.attribute.Attribute
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.scim.v2.Meta
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.EXPIRES_AT
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.META
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.RESOURCE_TYPE
import se.curity.identityserver.sdk.data.query.ResourceQuery
import se.curity.identityserver.sdk.data.query.ResourceQueryResult
import se.curity.identityserver.sdk.datasource.DeviceDataAccessProvider
import se.curity.identityserver.sdk.errors.ConflictException
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID

/**
 * The devices table has two uniqueness restrictions:
 * - The `id` must be unique.
 * - The `(accountId, deviceId)` pair must also be unique.
 * Due to this, this table uses the design documented on
 * [https://aws.amazon.com/blogs/database/simulating-amazon-dynamodb-unique-constraints-using-transactions/]
 *
 * The `pk` column has the "primary key", which is unique and can be:
 * - `id#{id}` (see [computePkFromId]) - main item, with all the attributes.
 * - `accountIdDeviceId#{accountId}&{deviceId}` (see [computePkFromAccountIdAndDeviceId]) - secondary item,
 * just to ensure uniqueness, and containing only the the device `id`.
 *
 * Note that the `accountId` and `deviceId` properties can never change for a device, so the secondary item
 * doesn't need to be changed on update operations.
 */
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

        // These two fields refer to the same index, which can be used in two different ways...
        // - Just with the partition key, to obtain all the devices associated to an `accountId`.
        val accountIdIndex = Index("accountId-deviceId-index", accountId)

        // - With both partition key and sort key, to obtain a specific device.
        val accountIdDeviceIdIndex = Index2("accountId-deviceId-index", accountId, deviceId)
    }

    /**
     * Produces a [MutableDynamoDBItem] (i.e. a table row) from a [DeviceAttributes].
     */
    private fun DeviceAttributes.toItem(): MutableDynamoDBItem
    {
        val now = Instant.now()
        val created = now

        val item = mutableMapOf<String, AttributeValue>()
        val deviceAttributesAsMap = this.asMap()
        _deviceAttributesToDynamoAttributes.forEach { it.addToItem(item, this, deviceAttributesAsMap) }

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

    /**
     * Produces a [DeviceAttributes] from a [DynamoDBItem] (i.e. a table row).
     */
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

        _deviceAttributesToDynamoAttributes.forEach {
            it.toAttribute(this)?.let { attribute -> attributeList.add(attribute) }
        }

        DeviceTable.attributes.optionalFrom(item)?.let {
            attributeList.addAll(_jsonHandler.toAttributes(it))
        }

        return DeviceAttributes.of(attributeList)
    }

    override fun create(deviceAttributes: DeviceAttributes)
    {
        _logger.debug("Received request to create device by deviceId: {}", deviceAttributes.deviceId)

        val transactionItems = mutableListOf<TransactWriteItem>()

        // Add main item with all the columns/attributes
        val mainItem = deviceAttributes.toItem()
        val id = DeviceTable.id.from(mainItem)
        DeviceTable.pk.addTo(mainItem, computePkFromId(id))

        transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(DeviceTable.name)
                    it.conditionExpression("attribute_not_exists(${DeviceTable.pk})")
                    it.item(mainItem)
                }
                .build()
        )

        // Add secondary item, with just the device ID
        val secondaryItem = mutableMapOf(
            DeviceTable.id.toNameValuePair(id)
        )
        DeviceTable.pk.addTo(secondaryItem, computePkFromAccountIdAndDeviceId(deviceAttributes))
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
        } catch (ex: Exception)
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

    /**
     * Deletes both the primary and secondary items for a given device.
     */
    private fun delete(id: String, accountId: String?, deviceId: String)
    {
        if (accountId == null)
        {
            throw requiredDeviceAttributeIsNotPresent("accountId")
        }

        val transactionItems = mutableListOf<TransactWriteItem>()

        // Delete primary item (the one having the id as the PK)
        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(DeviceTable.name)
                    it.key(mapOf(DeviceTable.pk.toNameValuePair(computePkFromId(id))))
                }
                .build()
        )

        // Delete secondary item (the one having (accountId, deviceId) as the PK)
        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(DeviceTable.name)
                    it.key(
                        mapOf(
                            DeviceTable.pk.toNameValuePair(
                                computePkFromAccountIdAndDeviceId(
                                    accountId,
                                    deviceId
                                )
                            )
                        )
                    )
                    // To double check we are removing the right secondary item.
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

        // Get the (accountId, deviceId) pair in order to remove both main and secondary items.
        val deviceAttributes = getById(id) ?: return
        val accountId = deviceAttributes.accountId
        val deviceId = deviceAttributes.deviceId

        delete(id, accountId, deviceId)
    }

    override fun delete(deviceId: String, accountId: String)
    {
        _logger.debug("Received request to delete device by deviceId: {} and accountId: {}", deviceId, accountId)

        // Get the id in order to remove both main and secondary items.
        val item = getBy(deviceId, accountId) ?: return
        delete(item.id, accountId, deviceId)
    }

    override fun update(deviceAttributes: DeviceAttributes)
    {
        val now = Instant.now()

        val deviceAttributesAsMap = deviceAttributes.asMap()
        val updateBuilder = UpdateExpressionsBuilder()
        _deviceAttributesToDynamoAttributes.forEach {
            it.addToUpdateBuilder(
                deviceAttributes,
                deviceAttributesAsMap,
                updateBuilder
            )
        }

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

        updateBuilder.update(DeviceTable.updated, now.epochSecond)

        updateBuilder.onlyIf(DeviceTable.deviceId, deviceAttributes.deviceId)
        deviceAttributes.accountId?.let {
            updateBuilder.onlyIf(DeviceTable.accountId, it)
        }

        /*
         * Only updates the main item:
         * - The secondary item doesn't need to change because an update cannot change `accountId nor `deviceId`.
         * - The update is conditioned to `accountId` and `deviceId` being the same as the ones in device attributes.
         */
        val updateRequest = UpdateItemRequest.builder()
            .tableName(DeviceTable.name)
            .apply {
                updateBuilder.applyTo(this)
            }
            .key(mapOf(DeviceTable.pk.toNameValuePair(computePkFromId(deviceAttributes.id))))
            .build()

        try
        {
            _dynamoDBClient.updateItem(updateRequest)
        } catch (e: ConditionalCheckFailedException)
        {
            _logger.trace(
                "conditional request was not applied, " +
                        "meaning that a device with the update key (accountId, deviceId) does not exist"
            )
            // Ignoring the failed update is by design
        }
    }

    override fun getBy(
        deviceId: String, accountId: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>?
    {
        _logger.debug("Received request to get device by deviceId: {} and accountId: {}", deviceId, accountId)

        // Uses the secondary index
        val index = DeviceTable.accountIdDeviceIdIndex
        val requestBuilder = QueryRequest.builder()
            .tableName(DeviceTable.name)
            .indexName(index.name)
            .keyConditionExpression(index.keyConditionExpression)
            .expressionAttributeValues(index.expressionValueMap(accountId, deviceId))
            .expressionAttributeNames(index.expressionNameMap)
            .limit(1)

        val response = _dynamoDBClient.query(requestBuilder.build())

        if (!response.hasItems() || response.items().isEmpty())
        {
            return null
        }

        return response.items().first().toDeviceAttributes(attributesEnumeration).filter(attributesEnumeration)
    }

    override fun getByAccountId(accountId: String?): List<DeviceAttributes>
    {
        _logger.debug("Received request to get devices by accountId: {}", accountId)

        if (accountId == null)
        {
            return listOf()
        }

        // Uses the secondary index
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

    override fun getBy(deviceId: String, accountId: String): DeviceAttributes?
    {
        _logger.debug("Received request to get device by deviceId: {} and accountId: {}", deviceId, accountId)

        // Uses the secondary index
        val index = DeviceTable.accountIdDeviceIdIndex
        val requestBuilder = QueryRequest.builder()
            .tableName(DeviceTable.name)
            .indexName(index.name)
            .keyConditionExpression(index.keyConditionExpression)
            .expressionAttributeValues(index.expressionValueMap(accountId, deviceId))
            .expressionAttributeNames(index.expressionNameMap)
            .limit(1)

        val response = _dynamoDBClient.query(requestBuilder.build())

        if (!response.hasItems() || response.items().isEmpty())
        {
            return null
        }

        return response.items().first().toDeviceAttributes()
    }

    override fun getById(id: String): DeviceAttributes?
    {
        _logger.debug("Received request to get device by id: {}", id)

        val request = GetItemRequest.builder()
            .tableName(DeviceTable.name)
            .key(mapOf(DeviceTable.pk.toNameValuePair(computePkFromId(id))))
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
            .key(mapOf(DeviceTable.pk.toNameValuePair(computePkFromId(id))))

        val response = _dynamoDBClient.getItem(requestBuilder.build())

        if (!response.hasItem() || response.item().isEmpty())
        {
            return null
        }

        return response.item().toDeviceAttributes(attributesEnumeration).filter(attributesEnumeration)
    }

    override fun getByAccountId(accountId: String, attributesEnumeration: ResourceQuery.AttributesEnumeration):
            List<ResourceAttributes<*>>
    {
        _logger.debug("Received request to get devices by accountId: {}", accountId)

        // Uses the secondary index
        val index = DeviceTable.accountIdIndex
        val requestBuilder = QueryRequest.builder()
            .tableName(DeviceTable.name)
            .indexName(index.name)
            .keyConditionExpression(index.expression)
            .expressionAttributeValues(index.expressionValueMap(accountId))
            .expressionAttributeNames(index.expressionNameMap)

        return querySequence(requestBuilder.build(), _dynamoDBClient)
            .map {
                it.toDeviceAttributes(attributesEnumeration).filter(attributesEnumeration)
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

        private fun computePkFromAccountIdAndDeviceId(accountId: String, deviceId: String) =
            "accountIdDeviceId#${URLEncoder.encode(accountId, StandardCharsets.UTF_8.name())}" +
                    "&${URLEncoder.encode(deviceId, StandardCharsets.UTF_8.name())}"

        private fun computePkFromAccountIdAndDeviceId(deviceAttributes: DeviceAttributes) =
            computePkFromAccountIdAndDeviceId(
                deviceAttributes.accountId ?: throw requiredDeviceAttributeIsNotPresent("accountId"),
                deviceAttributes.deviceId
            )

        private fun computePkFromId(id: String) = "id#$id"

        private val _deviceAttributesToDynamoAttributes = listOf(
            // Note than on DynamoDB, both deviceId and accountId are required.
            AttributeMapping(DeviceTable.deviceId, DeviceAttributes.DEVICE_ID, false),
            AttributeMapping(DeviceTable.accountId, DeviceAttributes.ACCOUNT_ID, false),
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

/**
 * Associates a SDK [DeviceAttributes] attribute to a [DynamoDBAttribute].
 */
private data class AttributeMapping<T>(
    val dynamoAttribute: DynamoDBAttribute<T>,
    val deviceAttributeName: String,
    val optional: Boolean,
    val retrieveFromDeviceAttributes: ((DeviceAttributes) -> T?)? = null,
    val toAttributeValue: ((T) -> Any)? = null
)
{
    // Gets the SDK device attribute and adds it to the DynamoDB item (i.e. row) being constructed.
    fun addToItem(
        item: MutableDynamoDBItem,
        deviceAttributes: DeviceAttributes,
        deviceAttributesMap: MutableMap<String, Any>
    )
    {
        if (retrieveFromDeviceAttributes != null)
        {
            val value = retrieveFromDeviceAttributes.invoke(deviceAttributes)
            deviceAttributesMap.remove(deviceAttributeName)
            if (value == null)
            {
                if (!optional)
                {
                    throw requiredDeviceAttributeIsNotPresent(deviceAttributeName)
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
                    throw requiredDeviceAttributeIsNotPresent(deviceAttributeName)
                }
            } else
            {
                dynamoAttribute.addToAny(item, value)
            }
        }
    }

    // Gets the SDK device attribute and adds it to the DynamoDB update being built.
    fun addToUpdateBuilder(
        deviceAttributes: DeviceAttributes,
        deviceAttributesMap: MutableMap<String, Any>,
        updateBuilder: UpdateExpressionsBuilder
    )
    {
        if (retrieveFromDeviceAttributes != null)
        {
            val value = retrieveFromDeviceAttributes.invoke(deviceAttributes)
            deviceAttributesMap.remove(deviceAttributeName)
            if (value == null && !optional)
            {
                throw requiredDeviceAttributeIsNotPresent(deviceAttributeName)
            } else
            {
                updateBuilder.update(dynamoAttribute, value)
            }
        } else
        {
            val value = deviceAttributesMap.remove(deviceAttributeName)
            if (value == null && !optional)
            {
                throw requiredDeviceAttributeIsNotPresent(deviceAttributeName)
            } else
            {
                updateBuilder.update(dynamoAttribute, value?.let { dynamoAttribute.cast(it) })
            }
        }
    }

    // Gets the attribute from the DynamoDB item and creates a SDKAttribute out of it.
    fun toAttribute(item: DynamoDBItem): Attribute? =
        if (optional)
        {
            dynamoAttribute.optionalFrom(item)?.let {
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

private fun requiredDeviceAttributeIsNotPresent(name: String) =
    NullPointerException("Required device attribute is not present: '$name'")
