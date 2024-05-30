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
import se.curity.identityserver.sdk.attribute.Attribute
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.scim.v2.Meta
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.ACCOUNT_ID
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.ALIAS
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.DEVICE_ID
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.DEVICE_TYPE
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.EXPIRES_AT
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.FORM_FACTOR
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.META
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.OWNER
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DeviceAttributes.RESOURCE_TYPE
import se.curity.identityserver.sdk.data.query.ResourceQuery
import se.curity.identityserver.sdk.data.query.ResourceQueryResult
import se.curity.identityserver.sdk.datasource.DeviceDataAccessProvider
import se.curity.identityserver.sdk.errors.ConflictException
import se.curity.identityserver.sdk.service.authentication.TenantId
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import java.time.Instant
import java.util.UUID

/**
 * The devices table has two uniqueness restrictions:
 * - The `id` must be unique.
 * - The `(accountId, deviceId)` pair must also be unique for a given tenant.
 * Due to this, this table uses the design documented on
 * [https://aws.amazon.com/blogs/database/simulating-amazon-dynamodb-unique-constraints-using-transactions/]
 *
 * The `pk` column has the "primary key", which is unique and can be:
 * - `id#{id}` (for default tenant) or `id{tenantId}#{id}` for custom tenant: main item, with all the attributes.
 * - `accountId#{accountId}` (for default tenant) or `accountId{tenantId}#{accountId}` for custom tenant:
 *    secondary item, both to ensure uniqueness
 * and to allow strong consistency reads by accountId and deviceId.
 */
class DynamoDBDeviceDataAccessProvider(
    private val _dynamoDBClient: DynamoDBClient,
    private val _configuration: DynamoDBDataAccessProviderConfiguration
) : DeviceDataAccessProvider {
    private val _tenantId = _configuration.getTenantId()
    private val _jsonHandler = _configuration.getJsonHandler()

    object DeviceTable : Table("curity-devices") {
        // Each device has two items in the table, with the following primary key structure
        // (pk - partition key, sk - sort key):
        // - pk=id#{id} or id({tenantId})#id, sk="sk" (sk is constant, since id must be unique)
        // - pk=accountId#{accountId} or accountId({tenantId})#id, sk={deviceId}
        //   (the pair (accountId, deviceId) must be unique)
        // This structure allows reads with strong consistency by id or by (accountId, deviceId)

        // the partition key
        val pk = KeyStringAttribute("pk")

        // the sort key
        val sk = KeyStringAttribute("sk")

        val id = UniqueStringAttribute("id", "id", "#")
        val tenantId = StringAttribute("tenantId")
        val accountId = UniqueStringAttribute("accountId", "accountId", "#")
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
        val deletableAt = NumberLongAttribute("deletableAt")

        // index
        val deviceIdIndex = PartitionOnlyIndex("deviceId-index", deviceId)
    }

    /**
     * Produces a [MutableDynamoDBItem] (i.e. a table row) from a [DeviceAttributes].
     */
    private fun DeviceAttributes.toItem(tenantId: TenantId): MutableDynamoDBItem {
        val now = Instant.now()

        val item = mutableMapOf<String, AttributeValue>()
        val deviceAttributesAsMap = this.asMap()
        _deviceAttributesToDynamoAttributes.forEach { it.addToItem(item, this, deviceAttributesAsMap) }

        val id = deviceAttributesAsMap.remove(ResourceAttributes.ID) as? String ?: UUID.randomUUID().toString()
        DeviceTable.id.addTo(item, id)

        tenantId.tenantId?.let {
            DeviceTable.tenantId.addTo(item, tenantId.tenantId)
        }

        if (deviceAttributesAsMap.isNotEmpty()) {
            deviceAttributesAsMap.remove(META)
            DeviceTable.attributes.addTo(item, _jsonHandler.fromAttributes(Attributes.fromMap(deviceAttributesAsMap)))
        }

        DeviceTable.created.addTo(item, now.epochSecond)
        DeviceTable.updated.addTo(item, now.epochSecond)

        return item
    }

    /**
     * Produces a [DeviceAttributes] from a [DynamoDBItem] (i.e. a table row).
     */
    private fun DynamoDBItem.toDeviceAttributes(attributesEnumeration: ResourceQuery.AttributesEnumeration? = null)
            : DeviceAttributes {
        val item = this
        val attributeList = mutableListOf<Attribute>()
        attributeList.add(Attribute.of(ResourceAttributes.ID, DeviceTable.id.from(item)))
        if (attributesEnumeration == null || attributesEnumeration.keepAttribute(ResourceAttributes.META)) {
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

    override fun create(deviceAttributes: DeviceAttributes) {
        _logger.debug("Received request to create device by deviceId: {}", deviceAttributes.deviceId)

        val transactionItems = mutableListOf<TransactWriteItem>()

        // Add main item with all the columns/attributes
        val commonItem = deviceAttributes.toItem(_tenantId)

        val mainItem = commonItem.toMutableMap()
        DeviceTable.pk.addTo(mainItem, DeviceTable.id.uniquenessValueFrom(_tenantId, deviceAttributes.id))
        DeviceTable.sk.addTo(mainItem, SK_FOR_ID_ITEM)

        val secondaryItem = commonItem.toMutableMap()
        DeviceTable.pk.addTo(secondaryItem, DeviceTable.accountId.uniquenessValueFrom(_tenantId, deviceAttributes.accountId))
        DeviceTable.sk.addTo(secondaryItem, deviceAttributes.deviceId)

        // Conditionally add the `deletableAt` to *both* main and secondary items
        DeviceTable.expires.optionalFrom(mainItem)?.let { expires ->
            val deletableAt = expires + _configuration.getDevicesTtlRetainDuration()
            DeviceTable.deletableAt.addTo(mainItem, deletableAt)
            DeviceTable.deletableAt.addTo(secondaryItem, deletableAt)
        }

        transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(DeviceTable.name(_configuration))
                    it.conditionExpression("attribute_not_exists(${DeviceTable.pk})")
                    it.item(mainItem)
                }
                .build()
        )

        transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(DeviceTable.name(_configuration))
                    it.conditionExpression("attribute_not_exists(${DeviceTable.pk})")
                    it.item(secondaryItem)
                }
                .build()
        )

        val request = TransactWriteItemsRequest.builder()
            .transactItems(transactionItems)
            .build()

        try {
            _dynamoDBClient.transactionWriteItems(request)
        } catch (ex: Exception) {
            if (ex.isTransactionCancelledDueToConditionFailure()) {
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
    private fun delete(id: String, accountId: String?, deviceId: String) {
        if (accountId == null) {
            throw requiredDeviceAttributeIsNotPresent("accountId")
        }

        val transactionItems = mutableListOf<TransactWriteItem>()

        // Delete primary item (the one having the id as the PK)
        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(DeviceTable.name(_configuration))
                    it.key(
                        mapOf(
                            DeviceTable.pk.uniqueKeyEntryFor(DeviceTable.id, _tenantId, id),
                            DeviceTable.sk.toNameValuePair(SK_FOR_ID_ITEM)
                        )
                    )
                }
                .build()
        )

        // Delete secondary item (the one having (accountId, deviceId) as the PK)
        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(DeviceTable.name(_configuration))
                    it.key(
                        mapOf(
                            DeviceTable.pk.uniqueKeyEntryFor(DeviceTable.accountId, _tenantId, accountId),
                            DeviceTable.sk.toNameValuePair(deviceId)
                        )
                    )
                    // To double-check we are removing the right secondary item.
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

    override fun delete(id: String) {
        _logger.debug("Received request to update device by id: {}", id)

        // Get the (accountId, deviceId) pair in order to remove both main and secondary items.
        val deviceAttributes = getById(id) ?: return
        val accountId = deviceAttributes.accountId
        val deviceId = deviceAttributes.deviceId

        delete(id, accountId, deviceId)
    }

    override fun delete(deviceId: String, accountId: String) {
        _logger.debug(MASK_MARKER, "Received request to delete device by deviceId: {} and accountId: {}", deviceId, accountId)

        // Get the id in order to remove both main and secondary items.
        val item = getBy(deviceId, accountId) ?: return
        delete(item.id, accountId, deviceId)
    }

    override fun update(deviceAttributes: DeviceAttributes) {

        val updateBuilder = getUpdateExpressionBuilder(deviceAttributes, null)

        // updates the main item (i.e. the one with pk = “id#{id}”)
        val transactionItems = mutableListOf<TransactWriteItem>()
        transactionItems.add(
            TransactWriteItem.builder()
                .update {
                    it.tableName(DeviceTable.name(_configuration))
                    updateBuilder.applyTo(it)
                    it.key(
                        mapOf(
                            DeviceTable.pk.uniqueKeyEntryFor(DeviceTable.id, _tenantId, deviceAttributes.id),
                            DeviceTable.sk.toNameValuePair(SK_FOR_ID_ITEM)

                        )
                    )
                }
                .build()
        )

        // updates the secondary item (i.e. the with pk = “accountId#{accountId}”
        transactionItems.add(
            TransactWriteItem.builder()
                .update {
                    it.tableName(DeviceTable.name(_configuration))
                    updateBuilder.applyTo(it)
                    it.key(
                        mapOf(
                            DeviceTable.pk.uniqueKeyEntryFor(DeviceTable.accountId, _tenantId, deviceAttributes.accountId),
                            DeviceTable.sk.toNameValuePair(deviceAttributes.deviceId)

                        )
                    )
                }
                .build()
        )

        val request = TransactWriteItemsRequest.builder()
            .transactItems(transactionItems)
            .build()

        try {
            _dynamoDBClient.transactionWriteItems(request)
        } catch (ex: Exception) {
            if (ex.isTransactionCancelledDueToConditionFailure()) {
                _logger.trace("No device matches the update condition")
            } else {
                throw ex
            }
        }
    }

    /**
     * Creates UpdateExpressionsBuilder with required mutation and condition expressions to an update TransactWriteItem
     *
     * @param  deviceAttributes updated attributes od a device
     * @param  currentDeviceId  currently used device id
     * @return UpdateExpressionsBuilder
     */
    private fun getUpdateExpressionBuilder(
        deviceAttributes: DeviceAttributes,
        currentDeviceId: String?
    ): UpdateExpressionsBuilder {

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

        if (deviceAttributesAsMap.isNotEmpty()) {
            deviceAttributesAsMap.remove(META)
            updateBuilder.update(
                DeviceTable.attributes,
                _jsonHandler.fromAttributes(Attributes.fromMap(deviceAttributesAsMap))
            )
        } else {
            updateBuilder.update(DeviceTable.attributes, null)
        }

        updateBuilder.update(DeviceTable.updated, now.epochSecond)

        // to make the update conditional on the item having a given deviceId.
        updateBuilder.onlyIf(
            DeviceTable.deviceId, if (currentDeviceId.isNullOrEmpty()) deviceAttributes.deviceId else currentDeviceId
        )
        deviceAttributes.accountId?.let {
            updateBuilder.onlyIf(DeviceTable.accountId, it)
        }

        val deletableAt: Long? = deviceAttributes.expiresAt?.let {
            it.epochSecond + _configuration.getDevicesTtlRetainDuration()
        }

        updateBuilder.update(DeviceTable.deletableAt, deletableAt)
        return updateBuilder
    }

    override fun update(attributes: DeviceAttributes, currentDeviceId: String) {

        val transactionItems = mutableListOf<TransactWriteItem>()

        // Delete the record (the one having (accountId, deviceId) as the PK) - sk can not be updated
        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(DeviceTable.name(_configuration))
                    it.key(
                        mapOf(
                            DeviceTable.pk.uniqueKeyEntryFor(DeviceTable.accountId, _tenantId, attributes.accountId),
                            DeviceTable.sk.toNameValuePair(currentDeviceId)
                        )
                    )
                    it.conditionExpression("${DeviceTable.id.hashName} = ${DeviceTable.id.colonName}")
                    it.expressionAttributeValues(mapOf(DeviceTable.id.toExpressionNameValuePair(attributes.id)))
                    it.expressionAttributeNames(mapOf(DeviceTable.id.toNamePair()))
                }
                .build()
        )

        // Update the device attributes (including device id) on the primary record (the one having the id as the PK)
        val updateBuilder = getUpdateExpressionBuilder(attributes, currentDeviceId)

        transactionItems.add(
            TransactWriteItem.builder()
                .update {
                    it.tableName(DeviceTable.name(_configuration))
                    updateBuilder.applyTo(it)
                    it.key(
                        mapOf(
                            DeviceTable.pk.uniqueKeyEntryFor(DeviceTable.id, _tenantId, attributes.id),
                            DeviceTable.sk.toNameValuePair(SK_FOR_ID_ITEM)
                        )
                    )
                }
                .build()
        )

        // Create a new secondary record (accountId, deviceId as the PK) to replace the deleted one
        val secondaryRecords = attributes.toItem(_tenantId).toMutableMap()
        DeviceTable.pk.addTo(secondaryRecords, DeviceTable.accountId.uniquenessValueFrom(_tenantId, attributes.accountId))
        DeviceTable.sk.addTo(secondaryRecords, attributes.deviceId)

        DeviceTable.expires.optionalFrom(secondaryRecords)?.let { expires ->
            val deletableAt = expires + _configuration.getDevicesTtlRetainDuration()
            DeviceTable.deletableAt.addTo(secondaryRecords, deletableAt)
        }

        transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(DeviceTable.name(_configuration))
                    it.conditionExpression("attribute_not_exists(${DeviceTable.pk})")
                    it.item(secondaryRecords)
                }
                .build()
        )

        val request = TransactWriteItemsRequest.builder()
            .transactItems(transactionItems)
            .build()

        try {
            _dynamoDBClient.transactionWriteItems(request)
        } catch (ex: Exception) {
            if (ex.isTransactionCancelledDueToConditionFailure()) {
                _logger.trace("Unable to update the device details")
            } else {
                throw ex
            }
        }
    }

    override fun getBy(
        deviceId: String,
        accountId: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>? = doGetBy(deviceId, accountId, attributesEnumeration)?.filter(attributesEnumeration)

    override fun getByAccountId(accountId: String?): List<DeviceAttributes> {

        _logger.debug(MASK_MARKER, "Received request to get devices by accountId: {}", accountId)

        if (accountId == null) {
            return listOf()
        }

        val request = QueryRequest.builder()
            .tableName(DeviceTable.name(_configuration))
            .keyConditionExpression("${DeviceTable.pk.hashName} = ${DeviceTable.pk.colonName}")
            .expressionAttributeValues(
                mapOf(
                    DeviceTable.pk.toExpressionNameValuePair(DeviceTable.accountId.uniquenessValueFrom(_tenantId, accountId))
                )
            )
            .expressionAttributeNames(
                mapOf(
                    DeviceTable.pk.toNamePair()
                )
            )
            .consistentRead(true)
            .build()

        return querySequence(request, _dynamoDBClient)
            .map {
                it.toDeviceAttributes()
            }
            .toList()
    }

    override fun getByAccountId(accountId: String, attributesEnumeration: ResourceQuery.AttributesEnumeration):
            List<ResourceAttributes<*>> = getByAccountId(accountId).map { it.filter(attributesEnumeration) }

    override fun getByDeviceId(deviceId: String): List<DeviceAttributes> {
        _logger.debug(MASK_MARKER, "Received request to get devices by deviceId: {}", deviceId)

        val requestBuilder = QueryRequest.builder()
            .tableName(DeviceTable.name(_configuration))
            .indexName(DeviceTable.deviceIdIndex.name)
            .keyConditionExpression(DeviceTable.deviceIdIndex.expression)
            .filterExpression("begins_with(${DeviceTable.pk}, ${DeviceTable.pk.colonName})")
            .expressionAttributeValues(mapOf(
                DeviceTable.deviceId.toExpressionNameValuePair(deviceId),
                DeviceTable.pk.toExpressionNameValuePair(DeviceTable.id.toValuePrefix(_tenantId))
            ))
            .expressionAttributeNames(DeviceTable.deviceIdIndex.expressionNameMap)

        val response = _dynamoDBClient.query(requestBuilder.build())

        if (response.items().isEmpty()) {
            return emptyList()
        }

        return response.items().map { it.toDeviceAttributes() }.toList()
    }

    override fun getByDeviceId(
        deviceId: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): List<ResourceAttributes<*>> = getByDeviceId(deviceId).map { it.filter(attributesEnumeration) }

    override fun getBy(deviceId: String, accountId: String): DeviceAttributes? = doGetBy(deviceId, accountId, null)

    private fun doGetBy(
        deviceId: String,
        accountId: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration?
    ): DeviceAttributes? {
        _logger.debug(MASK_MARKER, "Received request to get device by deviceId: {} and accountId: {}", deviceId, accountId)

        val requestBuilder = GetItemRequest.builder()
            .tableName(DeviceTable.name(_configuration))
            .key(
                mapOf(
                    DeviceTable.pk.uniqueKeyEntryFor(DeviceTable.accountId, _tenantId, accountId),
                    DeviceTable.sk.toNameValuePair(deviceId)
                )
            )
            .consistentRead(true)

        val response = _dynamoDBClient.getItem(requestBuilder.build())

        if (!response.hasItem()) {
            return null
        }

        return response.item().toDeviceAttributes(attributesEnumeration)
    }

    override fun getById(id: String): DeviceAttributes? {
        _logger.debug("Received request to get device by id: {}", id)

        val request = GetItemRequest.builder()
            .tableName(DeviceTable.name(_configuration))
            .key(
                mapOf(
                    DeviceTable.pk.uniqueKeyEntryFor(DeviceTable.id, _tenantId, id),
                    DeviceTable.sk.toNameValuePair(SK_FOR_ID_ITEM)
                )
            )
            .consistentRead(true)
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        return response.item().toDeviceAttributes()
    }

    override fun getById(
        id: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>? {
        return getById(id)?.filter(attributesEnumeration)
    }

    override fun getAll(startIndex: Long, count: Long): ResourceQueryResult {
        _logger.debug("Received request to get all devices with startIndex: {} and count: {}", startIndex, count)

        val validatedStartIndex = startIndex.toIntOrThrow("startIndex")
        val validatedCount = count.toIntOrThrow("count")

        val request = ScanRequest.builder()
            .tableName(DeviceTable.name(_configuration))
            .filterExpression("begins_with(${DeviceTable.pk}, ${DeviceTable.pk.colonName})")
            .expressionAttributeValues(
                mapOf(DeviceTable.pk.toExpressionNameValuePair(DeviceTable.id.toValuePrefix(_tenantId)))
            )
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

    companion object {
        private val _logger: Logger = LoggerFactory.getLogger(DynamoDBDeviceDataAccessProvider::class.java)
        private val MASK_MARKER : Marker = MarkerFactory.getMarker("MASK")

        private const val SK_FOR_ID_ITEM = "sk"

        private val _deviceAttributesToDynamoAttributes = listOf(
            // Note than on DynamoDB, both deviceId and accountId are required.
            AttributeMapping(DeviceTable.deviceId, DEVICE_ID, false),
            AttributeMapping(DeviceTable.accountId, ACCOUNT_ID, false),
            AttributeMapping(DeviceTable.externalId, ResourceAttributes.EXTERNAL_ID, true),
            AttributeMapping(DeviceTable.alias, ALIAS, true),
            AttributeMapping(DeviceTable.formFactor, FORM_FACTOR, true),
            AttributeMapping(DeviceTable.deviceType, DEVICE_TYPE, true),
            AttributeMapping(DeviceTable.owner, OWNER, true),
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
 * Associates an SDK [DeviceAttributes] attribute to a [DynamoDBAttribute].
 */
private data class AttributeMapping<T>(
    val dynamoAttribute: DynamoDBAttribute<T>,
    val deviceAttributeName: String,
    val optional: Boolean,
    val retrieveFromDeviceAttributes: ((DeviceAttributes) -> T?)? = null,
    val toAttributeValue: ((T) -> Any)? = null
) {
    // Gets the SDK device attribute and adds it to the DynamoDB item (i.e. row) being constructed.
    fun addToItem(
        item: MutableDynamoDBItem,
        deviceAttributes: DeviceAttributes,
        deviceAttributesMap: MutableMap<String, Any>
    ) {
        if (retrieveFromDeviceAttributes != null) {
            val value = retrieveFromDeviceAttributes.invoke(deviceAttributes)
            deviceAttributesMap.remove(deviceAttributeName)
            if (value == null) {
                if (!optional) {
                    throw requiredDeviceAttributeIsNotPresent(deviceAttributeName)
                }
            } else {
                dynamoAttribute.addTo(item, value)
            }
        } else {
            val value = deviceAttributesMap.remove(deviceAttributeName)
            if (value == null) {
                if (!optional) {
                    throw requiredDeviceAttributeIsNotPresent(deviceAttributeName)
                }
            } else {
                dynamoAttribute.addToAny(item, value)
            }
        }
    }

    // Gets the SDK device attribute and adds it to the DynamoDB update being built.
    fun addToUpdateBuilder(
        deviceAttributes: DeviceAttributes,
        deviceAttributesMap: MutableMap<String, Any>,
        updateBuilder: UpdateExpressionsBuilder
    ) {
        if (retrieveFromDeviceAttributes != null) {
            val value = retrieveFromDeviceAttributes.invoke(deviceAttributes)
            deviceAttributesMap.remove(deviceAttributeName)
            if (value == null && !optional) {
                throw requiredDeviceAttributeIsNotPresent(deviceAttributeName)
            } else {
                updateBuilder.update(dynamoAttribute, value)
            }
        } else {
            val value = deviceAttributesMap.remove(deviceAttributeName)
            if (value == null && !optional) {
                throw requiredDeviceAttributeIsNotPresent(deviceAttributeName)
            } else {
                updateBuilder.update(dynamoAttribute, value?.let { dynamoAttribute.cast(it) })
            }
        }
    }

    // Gets the attribute from the DynamoDB item and creates a SDKAttribute out of it.
    fun toAttribute(item: DynamoDBItem): Attribute? =
        if (optional) {
            dynamoAttribute.optionalFrom(item)?.let {
                Attribute.of(
                    deviceAttributeName,
                    se.curity.identityserver.sdk.attribute.AttributeValue.of(toAttributeValue?.invoke(it) ?: it)
                )
            }
        } else {
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