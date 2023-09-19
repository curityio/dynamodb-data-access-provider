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
import se.curity.identityserver.sdk.datasource.DatabaseClientDataAccessProvider
import se.curity.identityserver.sdk.datasource.pagination.PaginatedDataAccessResult
import se.curity.identityserver.sdk.datasource.pagination.PaginationRequest
import se.curity.identityserver.sdk.datasource.query.DatabaseClientAttributesFiltering
import se.curity.identityserver.sdk.datasource.query.DatabaseClientAttributesSorting
import se.curity.identityserver.sdk.errors.ConflictException
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant

class DynamoDBDatabaseClientDataAccessProvider(
    private val _configuration: DynamoDBDataAccessProviderConfiguration,
    private val _dynamoDBClient: DynamoDBClient
) : DatabaseClientDataAccessProvider {
    private val _jsonHandler = _configuration.getJsonHandler()

    object DatabaseClientsTable : TableWithCapabilities("curity-database-clients") {
        // Table Partition Key (PK)
        const val PROFILE_ID = "profileId"

        // Table Sort Key (SK)
        const val CLIENT_ID_KEY = "clientIdKey"

        // PK for clientName-based GSIs
        const val CLIENT_NAME_KEY = "clientNameKey"

        // PK for tag-based GSIs
        const val TAG_KEY = "tagKey"

        val profileId = StringAttribute(PROFILE_ID)
        // DynamoDB-specific, composite string made up of clientId and tag, or clientId only
        val clientIdKey = StringAttribute(CLIENT_ID_KEY)

        // DynamoDB-specific, composite string made up of profileId and clientName
        val clientNameKey = StringAttribute(CLIENT_NAME_KEY)

        // DynamoDB-specific, composite string made up of profileId and an individual item from tags
        val tagKey = StringAttribute(CLIENT_NAME_KEY)
        val clientName = StringAttribute("clientName")
        val created = NumberLongAttribute("created")
        val updated = NumberLongAttribute("updated")
        val status = StringAttribute("status")
        val tags = ListStringAttribute("tags")
        val redirectUris = ListStringAttribute("redirectUris")

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
                "profile_id" to profileId,
                DatabaseClientAttributeKeys.CLIENT_ID to clientIdKey,
                "clientNameKey" to clientNameKey,
                DatabaseClientAttributeKeys.NAME to clientName,
                "tagKey" to tagKey,
                DatabaseClientAttributeKeys.TAGS to tags,
                Meta.CREATED to created,
                Meta.LAST_MODIFIED to updated,
                DatabaseClientAttributeKeys.STATUS to status,
            )
        ) {
            override fun getGsiCount() = 6
            override fun getLsiCount() = 3
        }

        // Table key schema
        fun primaryKey(pkValue: String, skValue: String) =
            mapOf(profileId.toNameValuePair(pkValue), clientIdKey.toNameValuePair(skValue))
    }

    override fun getClientById(clientId: String, profileId: String): DatabaseClientAttributes? {
        TODO("Not yet implemented")
    }

    override fun create(attributes: DatabaseClientAttributes, profileId: String): DatabaseClientAttributes {
        logger.debug("Creating database client with id: ${attributes.clientId} in profile $profileId")

        val request = PutItemRequest.builder()
            .tableName(tableName())
            .conditionExpression("attribute_not_exists(${DatabaseClientsTable.profileId.name})")
            .item(attributes.toItem())
            // TODO or ALL_NEW?
            .returnValues(ReturnValue.ALL_OLD)
            .build()

        try {
            val response = _dynamoDBClient.putItem(request)

            if (!response.hasAttributes() || response.attributes().isEmpty()) {
                // TODO exception?
            }

            return response.attributes().toAttributes()
        } catch (exception: ConditionalCheckFailedException) {
            val newException =
                ConflictException("Client ${attributes.clientId} is already registered")
            logger.trace(
                "Client with id: ${attributes.clientId} is already registered in profile $profileId",
                newException
            )
            throw newException
        }
    }

    override fun update(attributes: DatabaseClientAttributes, profileId: String): DatabaseClientAttributes {
        logger.debug("Updating database client with id: ${attributes.clientId} in profile $profileId")

        val builder = UpdateExpressionsBuilder()
        attributes.apply {
            // TODO secrets will have to be updated only if not null

            builder.update(DatabaseClientsTable.updated, Instant.now().epochSecond)
            builder.update(DatabaseClientsTable.status, status.name)
            builder.update(DatabaseClientsTable.redirectUris, redirectUris)
            // TODO OK?
            builder.onlyIfExists(DatabaseClientsTable.clientIdKey)
        }

        val requestBuilder = UpdateItemRequest.builder()
            .tableName(tableName())
            .key(DatabaseClientsTable.primaryKey(profileId, attributes.clientId))
            // TODO or ALL_NEW?
            .returnValues(ReturnValue.ALL_OLD)
            .apply { builder.applyTo(this) }

        try {
            val response = _dynamoDBClient.updateItem(requestBuilder.build())

            if (!response.hasAttributes() || response.attributes().isEmpty()) {
                // TODO exception? null?
            }

            return response.attributes().toAttributes()
        } catch (_: ConditionalCheckFailedException) {
            // this exception means the entry does not exist, which should be signaled with an exception
            throw RuntimeException("Client with id: ${attributes.clientId} could not be updated in profile $profileId")
        }
    }

    override fun delete(clientId: String, profileId: String): Boolean {
        logger.debug("Deleting database client with id: $clientId from profile $profileId")

        val request = DeleteItemRequest.builder()
            .tableName(tableName())
            .key(DatabaseClientsTable.primaryKey(profileId, clientId))
            .build()

        val response = _dynamoDBClient.deleteItem(request)

        if (!response.hasAttributes() || response.attributes().isEmpty()) {
            // TODO exception?
        }

        // TODO how to know if it was actually deleted?
        return true
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

    private fun DatabaseClientAttributes.toItem(): DynamoDBItem {
        val now = Instant.now()
        val created = meta?.created ?: now

        val item = mutableMapOf<String, AttributeValue>()

        // Non-nullable
        // TODO to be possibly reworked for IS-7931
        DatabaseClientsTable.clientName.addTo(item, name)

        // Nullable
        DatabaseClientsTable.clientIdKey.addToNullable(item, clientId)
        DatabaseClientsTable.created.addToNullable(item, created.epochSecond)
        DatabaseClientsTable.updated.addToNullable(item, now.epochSecond)
        DatabaseClientsTable.status.addToNullable(item, status.name)
        // TODO add all other attributes or use 'attributes'?
        DatabaseClientsTable.redirectUris.addToNullable(item, redirectUris)
        // TODO: attributes? See JdbcDCDAP
        /*DatabaseClientsTable.attributes.addToNullable(
            item, _jsonHandler.fromAttributes(
                Attributes.of(attributes)
            )
        )*/

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
            // Non-nullable
            // TODO to be possibly reworked for IS-7931
            add(DatabaseClientAttributeKeys.NAME, DatabaseClientsTable.clientName.from(item))

            // Nullable
            add(DatabaseClientAttributeKeys.CLIENT_ID, DatabaseClientsTable.clientIdKey.optionalFrom(item))
            add(
                Attribute.of(
                    ResourceAttributes.META,
                    Meta.of(DatabaseClientAttributes.RESOURCE_TYPE)
                        .withCreated(
                            Instant.ofEpochSecond(
                                DatabaseClientsTable.created.from(
                                    item
                                )
                            )
                        )
                        .withLastModified(
                            Instant.ofEpochSecond(
                                DatabaseClientsTable.updated.from(
                                    item
                                )
                            )
                        )
                )
            )
            add(
                Attribute.of(
                    DatabaseClientAttributeKeys.STATUS, DatabaseClientStatus.valueOf(
                        DatabaseClientsTable.status.from(item)
                    )
                )
            )
            // TODO add all other attributes or use 'attributes'?
            add(DatabaseClientAttributeKeys.REDIRECT_URIS, DatabaseClientsTable.redirectUris.optionalFrom(item))
        }

        return DatabaseClientAttributes.of(Attributes.of(result))
    }
}
