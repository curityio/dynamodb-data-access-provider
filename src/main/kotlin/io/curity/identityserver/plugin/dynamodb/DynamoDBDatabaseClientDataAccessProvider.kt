/*
 * Copyright (C) 2023 Curity AB. All rights reserved.
 *
 * The contents of this file are the property of Curity AB.
 * You may not copy or use this file, in either source code
 * or executable form, except in compliance with terms
 * set by Curity AB.
 *
 * For further information, please contact Curity AB.
 */
package io.curity.identityserver.plugin.dynamodb

import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.configurationReferencesToJson
import io.curity.identityserver.plugin.dynamodb.query.Index
import io.curity.identityserver.plugin.dynamodb.query.TableQueryCapabilities
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.attribute.Attribute
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientStatus
import se.curity.identityserver.sdk.attribute.scim.v2.Meta
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes
import se.curity.identityserver.sdk.data.query.ResourceQuery
import se.curity.identityserver.sdk.datasource.DatabaseClientDataAccessProvider
import se.curity.identityserver.sdk.datasource.pagination.PaginatedDataAccessResult
import se.curity.identityserver.sdk.datasource.pagination.PaginationRequest
import se.curity.identityserver.sdk.datasource.query.DatabaseClientAttributesFiltering
import se.curity.identityserver.sdk.datasource.query.DatabaseClientAttributesSorting
import se.curity.identityserver.sdk.errors.ConflictException
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException
import java.time.Instant


// TODO IS-7807 add javadoc documenting different records and DDB-specific attributes
class DynamoDBDatabaseClientDataAccessProvider(
    private val _configuration: DynamoDBDataAccessProviderConfiguration,
    private val _dynamoDBClient: DynamoDBClient
) : DatabaseClientDataAccessProvider {
    private val _json = _configuration.getJsonHandler()

    object DatabaseClientsTable : TableWithCapabilities("curity-database-clients") {
        private const val CLIENT_NAME_KEY = "client_name_key"
        private const val TAG_KEY = "tag_key"

        // Table Partition Key (PK)
        val profileId = StringAttribute(DatabaseClientAttributesHelper.PROFILE_ID)
        // DynamoDB-specific, composite string made up of clientId and tag, or clientId only
        // Table Sort Key (SK)
        val clientIdKey = object : UniqueStringAttribute("client_id_key", "") {
            override fun uniquenessValueFrom(value: String) = value
        }

        // DynamoDB-specific, composite string made up of profileId and clientName
        // PK for clientName-based GSIs
        val clientNameKey = object : UniqueStringAttribute(CLIENT_NAME_KEY, "") {
            override fun uniquenessValueFrom(value: String) = value
        }

        // DynamoDB-specific, composite string made up of profileId and an individual item from tags
        // PK for tag-based GSIs
        val tagKey = object : UniqueStringAttribute(TAG_KEY, "") {
            override fun uniquenessValueFrom(value: String) = value
        }

        // DynamoDB-specific, attribute version
        val version = NumberLongAttribute("version")

        // SKs for GSIs & LSIs
        val clientName = StringAttribute(DatabaseClientAttributesHelper.CLIENT_NAME_COLUMN)
        val created = NumberLongAttribute(Meta.CREATED)
        val updated = NumberLongAttribute(Meta.LAST_MODIFIED)

        // Non-key attributes
        val tags = ListStringAttribute(DatabaseClientAttributeKeys.TAGS)
        val status = StringAttribute(DatabaseClientAttributeKeys.STATUS)

        val attributes = StringAttribute(DatabaseClientAttributesHelper.ATTRIBUTES)
        val configurationReferences = StringAttribute(DatabaseClientAttributesHelper.CONFIGURATION_REFERENCES)

        // Base table primary key
        val compositePrimaryKey = CompositePrimaryKey(profileId, clientIdKey)

        // Composite string helpers
        fun tagKeyFor(profileId: String, tag: String) = "$profileId#$tag"
        fun clientIdKeyFor(clientId: String, tag: String) = "$clientId#$tag"
        fun clientNameKeyFor(profileId: String, clientName: String) = "$profileId#${clientName}"

        // GSIs
        private val clientNameCreatedIndex =
            PartitionAndSortIndex("client_name-created-index", clientNameKey, created)
        private val clientNameUpdatedIndex =
            PartitionAndSortIndex("client_name-lastModified-index", clientNameKey, updated)
        private val clientNameClientNameIndex =
            PartitionAndSortIndex("client_name-client_name-index", clientNameKey, clientName)
        private val tagCreatedIndex =
            PartitionAndSortIndex("tag-created-index", tagKey, created)
        private val tagUpdatedIndex =
            PartitionAndSortIndex("tag-lastModified-index", tagKey, updated)
        private val tagClientNameIndex =
            PartitionAndSortIndex("tag-client_name-index", tagKey, clientName)

        // LSIs
        private val lsiCreatedIndex =
            PartitionAndSortIndex("lsi-created-index", profileId, created)
        private val lsiUpdatedIndex =
            PartitionAndSortIndex("lsi-lastModified-index", profileId, updated)
        private val lsiClientNameIndex =
            PartitionAndSortIndex("lsi-client_name-index", profileId, clientName)

        override fun queryCapabilities(): TableQueryCapabilities = object : TableQueryCapabilities(
            indexes = listOf(
                Index.from(compositePrimaryKey),
                Index.from(clientNameCreatedIndex),
                Index.from(clientNameUpdatedIndex),
                Index.from(clientNameClientNameIndex),
                Index.from(tagCreatedIndex),
                Index.from(tagUpdatedIndex),
                Index.from(tagClientNameIndex),
                Index.from(lsiCreatedIndex),
                Index.from(lsiUpdatedIndex),
                Index.from(lsiClientNameIndex),
            ),
            attributeMap = mapOf(
                DatabaseClientAttributesHelper.PROFILE_ID to profileId,
                DatabaseClientAttributeKeys.CLIENT_ID to clientIdKey,
                CLIENT_NAME_KEY to clientNameKey,
                DatabaseClientAttributeKeys.NAME to clientName,
                TAG_KEY to tagKey,
                DatabaseClientAttributeKeys.TAGS to tags,
                Meta.CREATED to created,
                Meta.LAST_MODIFIED to updated,
                DatabaseClientAttributeKeys.STATUS to status,
                DatabaseClientAttributesHelper.ATTRIBUTES to attributes,
                DatabaseClientAttributesHelper.CONFIGURATION_REFERENCES to configurationReferences,
            )
        ) {
            override fun getGsiCount() = 6
            override fun getLsiCount() = 3
        }

        // Return primary key for provided values
        fun primaryKey(pkValue: String, skValue: String) =
            mapOf(profileId.toNameValuePair(pkValue), clientIdKey.toNameValuePair(skValue))
    }

    override fun getClientById(clientId: String, profileId: String): DatabaseClientAttributes? {
        logger.debug("Getting database client with id: '$clientId' in profile: '$profileId'")

        return getItemById(clientId, profileId)?.toAttributes()
    }

    private fun getItemById(clientId: String, profileId: String): DynamoDBItem? {
        val request = GetItemRequest.builder()
            .tableName(DatabaseClientsTable.name(_configuration))
            .key(DatabaseClientsTable.primaryKey(profileId, clientId))
            .consistentRead(true)
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        return response.item()
    }

    override fun create(attributes: DatabaseClientAttributes, profileId: String): DatabaseClientAttributes {
        logger.debug("Creating database client with id: '${attributes.clientId}' in profile: '$profileId'")

        // the commonItem contains the attributes that will be on both the primary and secondary items
        val now = Instant.now().epochSecond
        val commonItem = attributes.toItem(profileId, now, now)
        commonItem.addAttr(DatabaseClientsTable.version, 0)

        // Item must not already exist
        val writeConditionExpression = "attribute_not_exists(${DatabaseClientsTable.clientIdKey.name})"

        val transactionItems = mutableListOf<TransactWriteItem>()
        // Create operation for the main item
        addTransactionItem(
            commonItem,
            // Main item's specific attributes
            mapOf(
                // Add clientIdKey as SK for base table
                // For the main item, it is not composite and holds clientId only
                Pair(
                    DatabaseClientsTable.clientIdKey.name,
                    DatabaseClientsTable.clientIdKey.toAttrValue(attributes.clientId)
                ),
                // Add composite clientNameKey as PK for clientName-based GSIs
                Pair(
                    DatabaseClientsTable.clientNameKey.name,
                    DatabaseClientsTable.clientNameKey.toAttrValue(
                        DatabaseClientsTable.clientNameKeyFor(profileId, attributes.name)
                    )
                ),
            ),
            transactionItems,
            writeConditionExpression
        )

        // One create operation per secondary item, i.e. per tag
        attributes.tags?.forEach { tag ->
            addTransactionItem(
                commonItem,
                // Secondary item's specific attributes
                mapOf(
                    // Add composite clientIdKey as SK for base table
                    Pair(
                        DatabaseClientsTable.clientIdKey.name,
                        DatabaseClientsTable.clientIdKey.toAttrValue(
                            DatabaseClientsTable.clientIdKeyFor(attributes.clientId, tag)
                        )
                    ),
                    // Add composite tagKey as PK for tag-based GSIs
                    Pair(
                        DatabaseClientsTable.tagKey.name,
                        DatabaseClientsTable.tagKey.toAttrValue(
                            DatabaseClientsTable.tagKeyFor(profileId, tag)
                        )
                    ),
                ),
                transactionItems,
                writeConditionExpression
            )
        }

        val request = TransactWriteItemsRequest.builder()
            .transactItems(transactionItems)
            .build()

        try {
            _dynamoDBClient.transactionWriteItems(request)

            return attributes
        } catch (e: Exception) {
            val message = "Unable to create client with id: '${attributes.clientId}' in profile '$profileId'"
            logger.trace(message, e)

            if (e.isTransactionCancelledDueToConditionFailure()) {
                val exceptionCause = e.cause
                if (exceptionCause is TransactionCanceledException) {
                    e.validateKnownUniqueConstraintsForAccountMutations(
                        exceptionCause.cancellationReasons(),
                        transactionItems
                    )
                } else {
                    throw ConflictException("$message as uniqueness check failed")
                }
            }
            throw e
        }
    }

    override fun update(attributes: DatabaseClientAttributes, profileId: String): DatabaseClientAttributes? {
        logger.debug("Updating database client with id: '${attributes.clientId}' in profile '$profileId'")

        val result = retry("update", N_OF_ATTEMPTS) {
            val currentMainItem =
                getItemById(attributes.clientId, profileId) ?: return@retry TransactionAttemptResult.Success(null)
            tryUpdate(attributes, profileId, currentMainItem)
        }
        return if (result != null) {
            getClientById(attributes.clientId, profileId)
        } else {
            null
        }
    }

    private fun tryUpdate(
        attributes: DatabaseClientAttributes,
        profileId: String,
        currentMainItem: DynamoDBItem
    ): TransactionAttemptResult<Unit> {
        val currentVersion = currentMainItem.version()
        val newVersion = currentVersion + 1

        // Preserve created attribute
        val created = DatabaseClientsTable.created.from(currentMainItem)
        val commonItem = attributes.toItem(profileId, created, updated = Instant.now().epochSecond)
        commonItem.addAttr(DatabaseClientsTable.version, newVersion)

        // Current item's clientIdKey based on clientId only, must be same as new clientId!
        val currentClientIdKey = DatabaseClientsTable.clientIdKey.from(currentMainItem)

        val updateBuilder = UpdateBuilderWithMultipleUniquenessConstraints(
            _configuration,
            DatabaseClientsTable,
            commonItem,
            _pkAttribute = StringAttribute(DatabaseClientsTable.profileId.name),
            versionAndClientIdKeyConditionExpression(currentVersion, currentClientIdKey),
            _pkValue = profileId,
            _skAttribute = StringAttribute(DatabaseClientsTable.clientIdKey.name),
        )

        // Update main item
        updateBuilder.handleUniqueAttribute(
            DatabaseClientsTable.clientIdKey,
            before = currentClientIdKey,
            // New clientIdKey based on clientId only
            after = attributes.clientId,
            additionalAttributes = mapOf(
                // Add clientIdKey as SK for base table. For the main item, it is not composite and holds clientId only
                Pair(
                    DatabaseClientsTable.clientIdKey.name,
                    DatabaseClientsTable.clientIdKey.toAttrValue(attributes.clientId)
                ),
                // Add composite clientNameKey as PK for clientName-based GSIs
                Pair(
                    DatabaseClientsTable.clientNameKey.name,
                    DatabaseClientsTable.clientNameKey.toAttrValue(
                        DatabaseClientsTable.clientNameKeyFor(profileId, attributes.name)
                    )
                )
            )
        )

        val currentTags = DatabaseClientsTable.tags.optionalFrom(currentMainItem) as List<String>
        val newTags = attributes.tags
        val commonTagCount = Integer.min(currentTags.size, newTags?.size!!)

        // 1. Update secondary items for the first commonTagCount tags
        var index = 0
        newTags.subList(0, commonTagCount).forEach { newTag ->
            // Secondary item's clientIdKey based on clientId and tag with same index as the new one
            val secondaryClientIdKey = DatabaseClientsTable.clientIdKeyFor(
                DatabaseClientsTable.clientIdKey.from(currentMainItem),
                currentTags.elementAt(index++)
            )

            // Update secondary item for newTag
            updateBuilder.handleUniqueAttribute(
                DatabaseClientsTable.clientIdKey,
                before = secondaryClientIdKey,
                // New clientIdKey based on clientId and new tag
                after = DatabaseClientsTable.clientIdKeyFor(attributes.clientId, newTag),
                additionalAttributes = mapOf(
                    // Add composite tagKey as PK for tag-based GSIs
                    Pair(
                        DatabaseClientsTable.tagKey.name,
                        DatabaseClientsTable.tagKey.toAttrValue(
                            DatabaseClientsTable.tagKeyFor(profileId, newTag)
                        )
                    )
                ),
                versionAndClientIdKeyConditionExpression(currentVersion, secondaryClientIdKey),
            )
        }

        // 2. Delete secondary items if additional tags in current item
        currentTags.subList(commonTagCount, currentTags.size).forEach { currentTag ->
            // Secondary item's clientIdKey based on clientId and current tag
            val secondaryClientIdKey = DatabaseClientsTable.clientIdKeyFor(
                DatabaseClientsTable.clientIdKey.from(currentMainItem),
                currentTag
            )

            // Delete secondary item for currentTag
            updateBuilder.handleUniqueAttribute(
                DatabaseClientsTable.clientIdKey,
                before = secondaryClientIdKey,
                after = null,
                additionalAttributes = mapOf(),
                versionAndClientIdKeyConditionExpression(currentVersion, secondaryClientIdKey),
            )
        }

        // 3. Create secondary items if additional new tags
        newTags.subList(commonTagCount, newTags.size).forEach { newTag ->
            // Secondary item's clientIdKey based on clientId and new tag
            val secondaryClientIdKey = DatabaseClientsTable.clientIdKeyFor(
                DatabaseClientsTable.clientIdKey.from(currentMainItem),
                newTag
            )

            // Create secondary item for newTag
            updateBuilder.handleUniqueAttribute(
                DatabaseClientsTable.clientIdKey,
                before = null,
                after = secondaryClientIdKey,
                additionalAttributes = mapOf(
                    // Add composite tagKey as PK for tag-based GSIs
                    Pair(
                        DatabaseClientsTable.tagKey.name,
                        DatabaseClientsTable.tagKey.toAttrValue(
                            DatabaseClientsTable.tagKeyFor(profileId, newTag)
                        )
                    )
                )
            )
        }

        try {
            _dynamoDBClient.transactionWriteItems(updateBuilder.build())

            return TransactionAttemptResult.Success(Unit)
        } catch (e: Exception) {
            val message = "Unable to delete client with id: '${attributes.clientId}' from profile '$profileId'"
            logger.trace(message, e)
            if (e.isTransactionCancelledDueToConditionFailure()) {
                return TransactionAttemptResult.Failure(
                    ConflictException("$message as version check failed")
                )
            }
            throw e
        }
    }

    override fun delete(clientId: String, profileId: String): Boolean {
        logger.debug("Deleting database client with id: '$clientId' from profile '$profileId'")

        retry("delete", N_OF_ATTEMPTS) {
            tryDelete(clientId, profileId)
        }

        return true
    }

    private fun tryDelete(clientId: String, profileId: String): TransactionAttemptResult<Unit> {
        logger.debug("Deleting database client with id: '$clientId' from profile '$profileId'")

        // Deleting a database client requires the deletion of the main item and all the secondary items.
        // A `getItem` is needed to obtain the `tags` required to compute the secondary item keys
        val getItemResponse = _dynamoDBClient.getItem(
            GetItemRequest.builder()
                .tableName(tableName())
                .key(DatabaseClientsTable.primaryKey(profileId, clientId))
                .build()
        )

        if (!getItemResponse.hasItem() || getItemResponse.item().isEmpty()) {
            return TransactionAttemptResult.Success(Unit)
        }

        val item = getItemResponse.item()
        val version = DatabaseClientsTable.version.optionalFrom(item)
            ?: throw SchemaErrorException(DatabaseClientsTable, DatabaseClientsTable.version)

        // Create a transaction with all the items (main and secondary) deletions,
        // conditioned to the version not having changed - optimistic concurrency
        val transactionItems = mutableListOf<TransactWriteItem>()

        // Delete operation for the main item
        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(tableName())
                    it.key(DatabaseClientsTable.primaryKey(profileId, clientId))
                    it.conditionExpression(
                        versionAndClientIdKeyConditionExpression(version, clientId)
                    )
                }
                .build()
        )

        // One delete operation per secondary item, i.e. per tag
        DatabaseClientsTable.tags.optionalFrom(item)?.forEach { tag ->
            transactionItems.add(
                TransactWriteItem.builder()
                    .delete {
                        it.tableName(tableName())
                        it.key(
                            DatabaseClientsTable.primaryKey(
                                profileId,
                                DatabaseClientsTable.clientIdKeyFor(clientId, tag)
                            )
                        )
                        it.conditionExpression(
                            versionAndClientIdKeyConditionExpression(
                                version,
                                DatabaseClientsTable.clientIdKeyFor(clientId, tag)
                            )
                        )
                    }
                    .build()
            )
        }

        val request = TransactWriteItemsRequest.builder()
            .transactItems(transactionItems)
            .build()

        try {
            _dynamoDBClient.transactionWriteItems(request)

            return TransactionAttemptResult.Success(Unit)
        } catch (e: Exception) {
            val message = "Unable to delete client with id: '$clientId' from profile '$profileId'"
            logger.trace(message, e)
            if (e.isTransactionCancelledDueToConditionFailure()) {
                return TransactionAttemptResult.Failure(
                    ConflictException("$message as version check failed")
                )
            }
            throw e
        }
    }

    override fun getAllClientsBy(
        profileId: String,
        filters: DatabaseClientAttributesFiltering?,
        paginationRequest: PaginationRequest?,
        sortRequest: DatabaseClientAttributesSorting?,
        activeClientsOnly: Boolean
    ): PaginatedDataAccessResult<DatabaseClientAttributes> {
        TODO("Not yet implemented")
    }

    override fun getClientCountBy(
        profileId: String,
        filters: DatabaseClientAttributesFiltering?,
        activeClientsOnly: Boolean
    ): Long {
        TODO("Not yet implemented")
    }

    companion object {
        private val logger: Logger =
            LoggerFactory.getLogger(DynamoDBDatabaseClientDataAccessProvider::class.java)

        // Retry count upon delete and update transactions
        private const val N_OF_ATTEMPTS = 3

        private val _conditionExpressionBuilder = ExpressionBuilder(
            "#${DatabaseClientsTable.version} = :oldVersion AND #${DatabaseClientsTable.clientIdKey.name} = :${DatabaseClientsTable.clientIdKey.name}",
            DatabaseClientsTable.version,
            DatabaseClientsTable.clientIdKey
        )

        private fun DynamoDBItem.version(): Long =
            DatabaseClientsTable.version.optionalFrom(this)
                ?: throw SchemaErrorException(DatabaseClientsTable, DatabaseClientsTable.version)
    }

    private fun DatabaseClientAttributes.toItem(
        profileId: String,
        created: Long?,
        updated: Long
    ): MutableMap<String, AttributeValue> {
        val item = mutableMapOf<String, AttributeValue>()

        // Non-nullable attributes
        DatabaseClientsTable.clientName.addTo(item, name)
        // Persist the whole DatabaseClientAttributes, but non persistable attributes, in the "attributes" attribute
        DatabaseClientsTable.attributes.addTo(
            item,
            _json.fromAttributes(
                removeAttributes(DatabaseClientAttributesHelper.DATABASE_CLIENT_SEEDING_ATTRIBUTES)
            )
        )
        // References also stored as JSON
        DatabaseClientsTable.configurationReferences.addTo(
            item,
            configurationReferencesToJson(this, _json)
        )

        // Nullable attributes
        DatabaseClientsTable.profileId.addToNullable(item, profileId)
        created?.let {
            DatabaseClientsTable.created.addToNullable(item, created)
        }
        DatabaseClientsTable.updated.addToNullable(item, updated)
        DatabaseClientsTable.status.addToNullable(item, status.name)
        DatabaseClientsTable.tags.addToNullable(item, tags)

        return item
    }

    private fun tableName(): String = DatabaseClientsTable.name(_configuration)

    private fun MutableList<Attribute>.add(name: String, value: String?) = value?.let {
        this.add(Attribute.of(name, it))
    }

    private fun MutableList<Attribute>.add(name: String, value: Collection<String>?) = value?.let {
        this.add(Attribute.of(name, it))
    }

    private fun DynamoDBItem.toAttributes(): DatabaseClientAttributes {

        val result = mutableListOf<Attribute>()
        val item = this

        result.apply {
            // DDB-specific attributes ignored: PROFILE_ID, CLIENT_NAME_KEY, TAG_KEY,
            // as not part of DatabaseClientAttributes

            // Non-nullable attributes
            add(DatabaseClientAttributeKeys.NAME, DatabaseClientsTable.clientName.from(item))
            add(DatabaseClientAttributesHelper.ATTRIBUTES, DatabaseClientsTable.attributes.from(item))
            add(
                DatabaseClientAttributesHelper.CONFIGURATION_REFERENCES,
                DatabaseClientsTable.configurationReferences.from(item)
            )

            // Nullable attributes
            add(DatabaseClientAttributeKeys.CLIENT_ID, DatabaseClientsTable.clientIdKey.optionalFrom(item))
            add(Attribute.of(
                ResourceAttributes.META,
                Meta.of(DatabaseClientAttributes.RESOURCE_TYPE)
                    .withCreated(
                        DatabaseClientsTable.created.optionalFrom(
                            item
                        )?.let {
                            Instant.ofEpochSecond(
                                it
                            )
                        }
                    )
                    .withLastModified(
                        DatabaseClientsTable.updated.optionalFrom(
                            item
                        )?.let {
                            Instant.ofEpochSecond(
                                it
                            )
                        }
                    )
            ))
            add(
                Attribute.of(
                    DatabaseClientAttributeKeys.STATUS, DatabaseClientStatus.valueOf(
                        DatabaseClientsTable.status.from(item)
                    )
                )
            )
            add(DatabaseClientAttributeKeys.TAGS, DatabaseClientsTable.tags.optionalFrom(item))
        }

        val rawAttributes = DatabaseClientAttributes.of(Attributes.of(result))
        // Parse ATTRIBUTES and CONFIGURATION_REFERENCES
        return DatabaseClientAttributesHelper.toResource(rawAttributes, ResourceQuery.Exclusions.none(), _json)
    }

    private fun addTransactionItem(
        commonItem: MutableMap<String, AttributeValue>,
        itemAttributes: Map<String, AttributeValue>,
        transactionItems: MutableList<TransactWriteItem>,
        writeConditionExpression: String
    ) {
        val item = commonItem + itemAttributes
        transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(tableName())
                    it.conditionExpression(writeConditionExpression)
                    it.item(item)
                }
                .build()
        )
    }

    private fun versionAndClientIdKeyConditionExpression(version: Long, clientIdKey: String) = object : Expression(
        _conditionExpressionBuilder
    ) {
        override val values = mapOf(
            ":oldVersion" to DatabaseClientsTable.version.toAttrValue(version),
            DatabaseClientsTable.clientIdKey.toExpressionNameValuePair(clientIdKey)
        )
    }
}
