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
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys
import se.curity.identityserver.sdk.attribute.scim.v2.Meta
import se.curity.identityserver.sdk.datasource.DatabaseClientDataAccessProvider
import se.curity.identityserver.sdk.datasource.pagination.PaginatedDataAccessResult
import se.curity.identityserver.sdk.datasource.pagination.PaginationRequest
import se.curity.identityserver.sdk.datasource.query.DatabaseClientAttributesFiltering
import se.curity.identityserver.sdk.datasource.query.DatabaseClientAttributesSorting

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

        val primaryKeys = PrimaryKeys(profileId, clientIdKey)

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
                Index.from(primaryKeys),
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

        override fun keyAttribute(): StringAttribute = clientIdKey

        fun key(value: String) = mapOf(keyAttribute().toNameValuePair(value))
    }

    override fun getClientById(clientId: String?, profileId: String?): DatabaseClientAttributes {
        TODO("Not yet implemented")
    }

    override fun create(attributes: DatabaseClientAttributes?, profileId: String?): DatabaseClientAttributes {
        TODO("Not yet implemented")
    }

    override fun update(attributes: DatabaseClientAttributes?, profileId: String?): DatabaseClientAttributes {
        TODO("Not yet implemented")
    }

    override fun delete(clientId: String?, profileId: String?): Boolean {
        TODO("Not yet implemented")
    }

    override fun getAllClientsBy(
        profileId: String?,
        filters: DatabaseClientAttributesFiltering?,
        paginationRequest: PaginationRequest?,
        sortRequest: DatabaseClientAttributesSorting?,
        activeClientsOnly: Boolean
    ): PaginatedDataAccessResult<DatabaseClientAttributes> {
        TODO("Not yet implemented")
    }

    override fun getClientCountBy(
        profileId: String?,
        filters: DatabaseClientAttributesFiltering?,
        activeClientsOnly: Boolean
    ): Long {
        TODO("Not yet implemented")
    }

    companion object {
        private val logger: Logger =
            LoggerFactory.getLogger(DynamoDBDatabaseClientDataAccessProvider::class.java)

    }
}
