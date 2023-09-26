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
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnValue
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

        // SKs for GSIs & LSIs
        val clientName = StringAttribute(DatabaseClientAttributesHelper.CLIENT_NAME_COLUMN)
        val created = NumberLongAttribute(Meta.CREATED)
        val updated = NumberLongAttribute(Meta.LAST_MODIFIED)

        // Non-key attributes
        val tags = ListStringAttribute(DatabaseClientAttributeKeys.TAGS)
        val status = StringAttribute(DatabaseClientAttributeKeys.STATUS)

        // TODO IS-7807 both as JSON?
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

        val rawAttributes = response.item().toAttributes()
        // Parse ATTRIBUTES and CONFIGURATION_REFERENCES
        return DatabaseClientAttributesHelper.toResource(rawAttributes, ResourceQuery.Exclusions.none(), _json)
    }

    override fun create(attributes: DatabaseClientAttributes, profileId: String): DatabaseClientAttributes {
        logger.debug("Creating database client with id: '${attributes.clientId}' in profile: '$profileId'")

        // TODO IS-7807 only main record for now => create also as many secondary records as tags

        val request = PutItemRequest.builder()
            .tableName(tableName())
            .conditionExpression("attribute_not_exists(${DatabaseClientsTable.clientIdKey.name})")
            // Pass empty tag for main record
            .item(attributes.toItem(profileId, ""))
            .build()

        try {
            _dynamoDBClient.putItem(request)

            // PutItem doesn't support returning newly set attributes
            /** @see PutItemRequest.returnValues */
            // TODO IS-7807  should we call getClientById to return actually set attributes? i.e. consistency vs. performance

            return attributes
        } catch (exception: ConditionalCheckFailedException) {
            val newException =
                ConflictException("Client '${attributes.clientId}' is already registered")
            logger.trace(
                "Client with id: '${attributes.clientId}' is already registered in profile '$profileId'",
                newException
            )
            throw newException
        }
    }

    override fun update(attributes: DatabaseClientAttributes, profileId: String): DatabaseClientAttributes {
        logger.debug("Updating database client with id: '${attributes.clientId}' in profile '$profileId'")

        val builder = UpdateExpressionsBuilder()
        attributes.apply {
            builder.update(DatabaseClientsTable.updated, Instant.now().epochSecond)
            // TODO IS-7807 generate an error if clientName is null or empty,
            //  as it is not allowed as an SK for 1 LSI and 2 GSIs!
            builder.update(DatabaseClientsTable.clientName, name)
            // TODO IS-7807 if name is changed ⇒ update clientNameKey

            builder.update(DatabaseClientsTable.status, status.name)
            builder.update(DatabaseClientsTable.tags, tags)
            // TODO IS-7807 if tags is changed ⇒ update tagKey's

            // TODO IS-7807 add all other attributes

            // TODO IS-7807 secrets will have to be updated only if not null
        }

        val requestBuilder = UpdateItemRequest.builder()
            .tableName(tableName())
            .key(DatabaseClientsTable.primaryKey(profileId, attributes.clientId))
            .returnValues(ReturnValue.ALL_NEW)
            .apply { builder.applyTo(this) }

        try {
            val response = _dynamoDBClient.updateItem(requestBuilder.build())

            if (!response.hasAttributes() || response.attributes().isEmpty()) {
                // TODO IS-7807 exception? null?
            }

            return response.attributes().toAttributes()
        } catch (_: ConditionalCheckFailedException) {
            // this exception means the entry does not exist, which should be signaled with an exception
            throw RuntimeException("Client with id: '${attributes.clientId}' could not be updated in profile '$profileId'")
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
            logger.trace(
                "Client '$clientId' from profile '$profileId' couldn't be deleted.", exception
            )
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

    private fun DatabaseClientAttributes.toItem(profileId: String, tag: String): DynamoDBItem {
        val now = Instant.now()
        val created = meta?.created ?: now

        val item = mutableMapOf<String, AttributeValue>()
        val mainRecord = tag.isEmpty()

        // Non-nullable attributes
        // TODO IS-7807 generate an error if 'clientName' is null or empty,
        //  as it is not allowed as an SK for 1 LSI and 2 GSIs!
        DatabaseClientsTable.clientName.addTo(item, name)
        // Only main record has a 'clientNameKey'
        if (mainRecord) {
            DatabaseClientsTable.clientNameKey.addTo(item, "$profileId#${name}")
        }
        // Main record has no 'tagKey'
        if (!mainRecord) {
            DatabaseClientsTable.tagKey.addTo(item, "$profileId#$tag")
        }
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
        DatabaseClientsTable.clientIdKey.addToNullable(item, clientId)
        DatabaseClientsTable.created.addToNullable(item, created.epochSecond)
        DatabaseClientsTable.updated.addToNullable(item, now.epochSecond)
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

        return DatabaseClientAttributes.of(Attributes.of(result))
    }
}
