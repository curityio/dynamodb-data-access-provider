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
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
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
        val clientIdKey = StringAttribute("client_id_key")

        // DynamoDB-specific, composite string made up of profileId and clientName
        // PK for clientName-based GSIs
        val clientNameKey = StringAttribute(CLIENT_NAME_KEY)

        // DynamoDB-specific, composite string made up of profileId and an individual item from tags
        // PK for tag-based GSIs
        val tagKey = StringAttribute(TAG_KEY)

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

        // GSIs
        private val clientNameCreatedIndex =
            PartitionAndSortIndex("clientName-created-index", clientNameKey, created)
        private val clientNameUpdatedIndex =
            PartitionAndSortIndex("clientName-updated-index", clientNameKey, updated)
        private val clientNameClientNameIndex =
            PartitionAndSortIndex("clientName-clientName-index", clientNameKey, clientName)
        private val tagCreatedIndex =
            PartitionAndSortIndex("tag-created-index", tagKey, created)
        private val tagUpdatedIndex =
            PartitionAndSortIndex("tag-updated-index", tagKey, updated)
        private val tagClientNameIndex =
            PartitionAndSortIndex("tag-clientName-index", tagKey, clientName)

        // LSIs
        private val lsiCreatedIndex =
            PartitionAndSortIndex("lsi-created-index", profileId, created)
        private val lsiUpdatedIndex =
            PartitionAndSortIndex("lsi-updated-index", profileId, updated)
        private val lsiClientNameIndex =
            PartitionAndSortIndex("lsi-clientName-index", profileId, clientName)

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

        val request = GetItemRequest.builder()
            .tableName(DatabaseClientsTable.name(_configuration))
            .key(DatabaseClientsTable.primaryKey(profileId, clientId))
            .consistentRead(true)
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        return response.item().toAttributes()
    }

    override fun create(attributes: DatabaseClientAttributes, profileId: String): DatabaseClientAttributes {
        logger.debug("Creating database client with id: '${attributes.clientId}' in profile: '$profileId'")

        // the commonItem contains the attributes that will be on both the primary and secondary items
        val commonItem = attributes.toItem(profileId, Instant.now())
        commonItem.addAttr(DatabaseClientsTable.version, 0)

        // Item must not already exist
        val writeConditionExpression = "attribute_not_exists(${DatabaseClientsTable.clientIdKey.name})"

        val transactionItems = mutableListOf<TransactWriteItem>()
        // Add main item
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
                    DatabaseClientsTable.clientNameKey.toAttrValue("$profileId#${attributes.name}")
                ),
            ),
            transactionItems,
            writeConditionExpression
        )

        // Add one secondary item per tag, with tagKey as used PK for tag-based GSIs
        attributes.tags?.forEach { tag ->
            addTransactionItem(
                commonItem,
                // Secondary item's specific attributes
                mapOf(
                    // Add composite clientIdKey as SK for base table
                    Pair(
                        DatabaseClientsTable.clientIdKey.name,
                        DatabaseClientsTable.clientIdKey.toAttrValue("${attributes.clientId}#$tag")
                    ),
                    // Add composite tagKey as PK for tag-based GSIs
                    Pair(
                        DatabaseClientsTable.tagKey.name,
                        DatabaseClientsTable.tagKey.toAttrValue("$profileId#$tag")
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
            if (e.isTransactionCancelledDueToConditionFailure()) {
                val exceptionCause = e.cause
                if (exceptionCause is TransactionCanceledException) {
                    e.validateKnownUniqueConstraintsForAccountMutations(
                        exceptionCause.cancellationReasons(),
                        transactionItems
                    )
                } else {
                    throw ConflictException(
                        "Unable to create client with id: '${attributes.clientId}' in profile '$profileId' as uniqueness check failed"
                    )
                }
            }
            throw e
        }
    }

    override fun update(attributes: DatabaseClientAttributes, profileId: String): DatabaseClientAttributes {
        logger.debug("Updating database client with id: '${attributes.clientId}' in profile '$profileId'")

        // TODO IS-7807 only main record for now => update also as many secondary records as tags

        // Pass empty tag for main record
        val builder = attributes.toUpdateExpressionBuilder(profileId, "")

        val requestBuilder = UpdateItemRequest.builder()
            .tableName(tableName())
            .key(DatabaseClientsTable.primaryKey(profileId, attributes.clientId))
            .returnValues(ReturnValue.ALL_NEW)
            .apply { builder.applyTo(this) }

        try {
            val response = _dynamoDBClient.updateItem(requestBuilder.build())

            if (!response.hasAttributes() || response.attributes().isEmpty()) {
                throw RuntimeException("No updated attributes returned, unable to update client with id: '${attributes.clientId}' in profile '$profileId'")
            }

            return response.attributes().toAttributes()
        } catch (e: ConditionalCheckFailedException) {
            // this exception means the entry does not exist
            logger.trace("Unable to update client with id: '${attributes.clientId}' in profile '$profileId'")

            throw e
        }
    }

    override fun delete(clientId: String, profileId: String): Boolean {
        logger.debug("Deleting database client with id: '$clientId' from profile '$profileId'")

        val request = DeleteItemRequest.builder()
            .tableName(tableName())
            .key(DatabaseClientsTable.primaryKey(profileId, clientId))
            .build()

        return try {
            _dynamoDBClient.deleteItem(request)
            true
        } catch (exception: SdkException) {
            logger.trace("Unable to delete client '$clientId' from profile '$profileId'.", exception)
            false
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

    }

    private fun DatabaseClientAttributes.toItem(profileId: String, now: Instant): MutableMap<String, AttributeValue> {
        val created = meta?.created ?: now

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
        DatabaseClientsTable.created.addToNullable(item, created.epochSecond)
        DatabaseClientsTable.updated.addToNullable(item, now.epochSecond)
        DatabaseClientsTable.status.addToNullable(item, status.name)
        DatabaseClientsTable.tags.addToNullable(item, tags)

        return item
    }

    private fun DatabaseClientAttributes.toUpdateExpressionBuilder(
        profileId: String,
        tag: String
    ): UpdateExpressionsBuilder {
        val builder = UpdateExpressionsBuilder()
        val mainRecord = tag.isEmpty()

        // Non-nullable attributes
        builder.update(DatabaseClientsTable.clientName, name)
        // Only main record has a 'clientNameKey'
        if (mainRecord) {
            builder.update(DatabaseClientsTable.clientNameKey, "$profileId#${name}")
        }
        // Main record has no 'tagKey'
        if (!mainRecord) {
            builder.update(DatabaseClientsTable.tagKey, "$profileId#$tag")
        }
        // Persist the whole DatabaseClientAttributes, but non persistable attributes, in the "attributes" attribute
        builder.update(
            DatabaseClientsTable.attributes,
            _json.fromAttributes(removeAttributes(DatabaseClientAttributesHelper.DATABASE_CLIENT_SEEDING_ATTRIBUTES))
        )
        // References also stored as JSON
        builder.update(DatabaseClientsTable.configurationReferences, configurationReferencesToJson(this, _json))

        // Nullable attributes
        builder.update(DatabaseClientsTable.updated, Instant.now().epochSecond)
        builder.update(DatabaseClientsTable.status, status.name)
        builder.update(DatabaseClientsTable.tags, tags)

        // TODO IS-7807 restore once update turned into a transaction
        // builder.onlyIfExists(DatabaseClientsTable.clientIdKey)

        return builder
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
}
