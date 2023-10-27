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
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.BACKCHANNEL_LOGOUT_HTTP_CLIENT_ID
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.HAAPI_POLICY_ID
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.ID_TOKEN_JWE_ENCRYPTION_KEY_ID
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.JWT_ASSERTION_ASYMMETRIC_KEY_ID
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.JWT_ASSERTION_JWKS_URI_CLIENT_ID
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.PRIMARY_ASYMMETRIC_KEY_ID
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.PRIMARY_CLIENT_AUTHENTICATION
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.PRIMARY_CREDENTIAL_MANAGER_ID
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.PRIMARY_MUTUAL_TLS_CLIENT_CERTIFICATE_ID
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.REQUEST_OBJECT_BY_REFERENCE_HTTP_CLIENT_ID
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.ROPC_CREDENTIAL_MANAGER_ID
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.SECONDARY_ASYMMETRIC_KEY_ID
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.SECONDARY_CLIENT_AUTHENTICATION
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.SECONDARY_CREDENTIAL_MANAGER_ID
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseClientAttributesHelper.SECONDARY_MUTUAL_TLS_CLIENT_CERTIFICATE_ID
import io.curity.identityserver.plugin.dynamodb.query.Index
import io.curity.identityserver.plugin.dynamodb.query.QueryHelper
import io.curity.identityserver.plugin.dynamodb.query.TableQueryCapabilities
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.attribute.Attribute
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.UserAuthenticationConfigAttributes
import se.curity.identityserver.sdk.attribute.UserAuthenticationConfigAttributes.AUTHENTICATOR_FILTERS
import se.curity.identityserver.sdk.attribute.UserAuthenticationConfigAttributes.REQUIRED_CLAIMS
import se.curity.identityserver.sdk.attribute.UserAuthenticationConfigAttributes.TEMPLATE_AREA
import se.curity.identityserver.sdk.attribute.client.database.ClientAttestationAttributes
import se.curity.identityserver.sdk.attribute.client.database.ClientAuthenticationVerifier
import se.curity.identityserver.sdk.attribute.client.database.ClientCapabilitiesAttributes
import se.curity.identityserver.sdk.attribute.client.database.ClientCapabilitiesAttributes.BackchannelCapability.AUTHENTICATORS
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.AUDIENCES
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.CLAIM_MAPPER_ID
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.REDIRECT_URI_VALIDATION_POLICY_ID
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.SCOPES
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.SUBJECT_TYPE
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.USERINFO_SIGNED_ISSUER_ID
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientStatus
import se.curity.identityserver.sdk.attribute.client.database.JwksUri
import se.curity.identityserver.sdk.attribute.client.database.JwtAssertionAttributes
import se.curity.identityserver.sdk.attribute.client.database.JwtSigningAttributes
import se.curity.identityserver.sdk.attribute.client.database.UserConsentAttributes
import se.curity.identityserver.sdk.attribute.client.database.UserConsentAttributes.CONSENTORS
import se.curity.identityserver.sdk.attribute.scim.v2.Meta
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes
import se.curity.identityserver.sdk.authentication.mutualtls.MutualTlsAttributes
import se.curity.identityserver.sdk.data.query.ResourceQuery
import se.curity.identityserver.sdk.datasource.DatabaseClientDataAccessProvider
import se.curity.identityserver.sdk.datasource.pagination.PaginatedDataAccessResult
import se.curity.identityserver.sdk.datasource.pagination.PaginationRequest
import se.curity.identityserver.sdk.datasource.query.AttributesFiltering
import se.curity.identityserver.sdk.datasource.query.DatabaseClientAttributesFiltering
import se.curity.identityserver.sdk.datasource.query.DatabaseClientAttributesSorting
import se.curity.identityserver.sdk.errors.ConflictException
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.ProjectionType
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import java.time.Instant
import java.util.Optional


// TODO IS-7807 add javadoc documenting different records and DDB-specific attributes
class DynamoDBDatabaseClientDataAccessProvider(
    private val _configuration: DynamoDBDataAccessProviderConfiguration,
    private val _dynamoDBClient: DynamoDBClient
) : DatabaseClientDataAccessProvider {
    private val _json = _configuration.getJsonHandler()

    object DatabaseClientsTable : TableWithCapabilities("curity-database-clients") {
        const val CLIENT_NAME_KEY = "client_name_key"
        const val TAG_KEY = "tag_key"
        private const val VERSION = "version"
        private const val KEY_VALUE_SEPARATOR = "#"
        private const val KEY_ESCAPE_CHARACTER = "\\"
        private const val KEY_ESCAPED_SEPARATOR = KEY_ESCAPE_CHARACTER + KEY_VALUE_SEPARATOR
        val INTERNAL_ATTRIBUTES = arrayOf(DatabaseClientAttributesHelper.PROFILE_ID, CLIENT_NAME_KEY, TAG_KEY, VERSION)

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
        val profileWithTagKey = object : UniqueStringAttribute(TAG_KEY, "") {
            override fun uniquenessValueFrom(value: String) = value
        }

        // DynamoDB-specific, attribute version
        val version = NumberLongAttribute(VERSION)

        // SKs for GSIs & LSIs
        val clientName = StringAttribute(DatabaseClientAttributesHelper.CLIENT_NAME_COLUMN)
        val created = NumberLongAttribute(Meta.CREATED)
        val updated = NumberLongAttribute(Meta.LAST_MODIFIED)

        // Non-key attributes
        val tags = ListStringAttribute(DatabaseClientAttributeKeys.TAGS)
        val status = StringAttribute(DatabaseClientAttributeKeys.STATUS)

        val attributes = StringAttribute(DatabaseClientAttributesHelper.ATTRIBUTES)

        // Base table primary key
        val compositePrimaryKey = CompositePrimaryKey(profileId, clientIdKey)

        // Composite string helpers
        fun tagKeyFor(profileId: String, tag: String) = compositeKeyFromComponents(profileId, tag)
        fun clientIdKeyFor(clientId: String, tag: String) = compositeKeyFromComponents(clientId, tag)
        fun clientNameKeyFor(profileId: String, clientName: String) = compositeKeyFromComponents(profileId, clientName)
        fun clientIdFrom(clientIdKey: String) = clientIdKey.splitKeyComponents().first

        private fun compositeKeyFromComponents(key: String, subKey: String): String =
            key.replace(KEY_VALUE_SEPARATOR, KEY_ESCAPED_SEPARATOR) + KEY_VALUE_SEPARATOR + subKey

        internal fun String.splitKeyComponents(): Pair<String, String> {
            var previousChar: Char? = null
            var idx = 0
            while (idx < length) {
                if (this[idx] == KEY_VALUE_SEPARATOR.first() && previousChar != KEY_ESCAPE_CHARACTER.first()) {
                    // We found a non escaped separator to split the composed key on.
                    break
                }

                previousChar = this[idx]
                idx++
            }

            return this.substring(0, idx).replace(KEY_ESCAPED_SEPARATOR, KEY_VALUE_SEPARATOR) to
                    if (idx < this.length - 1) this.substring(idx + 1) else ""
        }

        // GSIs
        private val clientNameCreatedIndex =
            PartitionAndSortIndex("client_name-created-index", clientNameKey, created)
        private val clientNameUpdatedIndex =
            PartitionAndSortIndex("client_name-lastModified-index", clientNameKey, updated)
        private val clientNameClientNameIndex =
            PartitionAndSortIndex("client_name-client_name-index", clientNameKey, clientName)
        private val tagCreatedIndex =
            PartitionAndSortIndex("tag-created-index", profileWithTagKey, created)
        private val tagUpdatedIndex =
            PartitionAndSortIndex("tag-lastModified-index", profileWithTagKey, updated)
        private val tagClientNameIndex =
            PartitionAndSortIndex("tag-client_name-index", profileWithTagKey, clientName)

        // LSIs
        private val lsiCreatedIndex =
            PartitionAndSortIndex("lsi-created-index", profileId, created)
        private val lsiUpdatedIndex =
            PartitionAndSortIndex("lsi-lastModified-index", profileId, updated)
        private val lsiClientNameIndex =
            PartitionAndSortIndex("lsi-client_name-index", profileId, clientName)

        // Projected attributes to GSIs/LSIs
        private val commonProjectedAttributes = listOf(
            DatabaseClientAttributeKeys.STATUS,
            DatabaseClientAttributeKeys.TAGS,
            DatabaseClientAttributesHelper.ATTRIBUTES,
        )
        private val projectedAttributesForCreatedSortKey =
            mutableListOf(DatabaseClientAttributesHelper.CLIENT_NAME_COLUMN, Meta.LAST_MODIFIED)
                .apply { addAll(commonProjectedAttributes) }.toList()
        private val projectedAttributesForUpdatedSortKey =
            mutableListOf(DatabaseClientAttributesHelper.CLIENT_NAME_COLUMN, Meta.CREATED)
                .apply { addAll(commonProjectedAttributes) }.toList()
        private val projectedAttributesForClientNameSortKey =
            mutableListOf(Meta.CREATED, Meta.LAST_MODIFIED)
                .apply { addAll(commonProjectedAttributes) }.toList()

        override fun queryCapabilities(): TableQueryCapabilities = object : TableQueryCapabilities(
            indexes = listOf(
                Index.from(compositePrimaryKey),
                Index.from(clientNameCreatedIndex, ProjectionType.INCLUDE, projectedAttributesForCreatedSortKey),
                Index.from(clientNameUpdatedIndex, ProjectionType.INCLUDE, projectedAttributesForUpdatedSortKey),
                Index.from(clientNameClientNameIndex, ProjectionType.INCLUDE, projectedAttributesForClientNameSortKey),
                Index.from(tagCreatedIndex, ProjectionType.INCLUDE, projectedAttributesForCreatedSortKey),
                Index.from(tagUpdatedIndex, ProjectionType.INCLUDE, projectedAttributesForUpdatedSortKey),
                Index.from(tagClientNameIndex, ProjectionType.INCLUDE, projectedAttributesForClientNameSortKey),
                Index.from(lsiCreatedIndex, ProjectionType.INCLUDE, projectedAttributesForCreatedSortKey),
                Index.from(lsiUpdatedIndex, ProjectionType.INCLUDE, projectedAttributesForUpdatedSortKey),
                Index.from(lsiClientNameIndex, ProjectionType.INCLUDE, projectedAttributesForClientNameSortKey),
            ),
            attributeMap = mapOf(
                DatabaseClientAttributesHelper.PROFILE_ID to profileId,
                DatabaseClientAttributeKeys.CLIENT_ID to clientIdKey,
                CLIENT_NAME_KEY to clientNameKey,
                DatabaseClientAttributeKeys.NAME to clientName,
                TAG_KEY to profileWithTagKey,
                DatabaseClientAttributeKeys.TAGS to tags,
                Meta.CREATED to created,
                Meta.LAST_MODIFIED to updated,
                DatabaseClientAttributeKeys.STATUS to status,
                DatabaseClientAttributesHelper.ATTRIBUTES to attributes,
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
            attributes.addConfigurationReferencesTo(commonItem),
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
                        DatabaseClientsTable.profileWithTagKey.name,
                        DatabaseClientsTable.profileWithTagKey.toAttrValue(
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
                throw ConflictException("$message as uniqueness check failed")
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
            // Common item, i.e. without configuration references
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
            ),
            null,
            // Override commonItem by adding configuration references for the main item only
            attributes.addConfigurationReferencesTo(commonItem)
        )

        val currentTags = DatabaseClientsTable.tags.optionalFrom(currentMainItem) as List<String>?
        val newTags: List<String>? = attributes.tags
        val commonTagCount = Integer.min(
            currentTags?.size ?: 0,
            newTags?.size ?: 0
        )

        // 1. Update secondary items for the first commonTagCount tags
        var index = 0
        newTags?.subList(0, commonTagCount)?.forEach { newTag ->
            // Secondary item's clientIdKey based on clientId and tag with same index as the new one
            val secondaryClientIdKey = DatabaseClientsTable.clientIdKeyFor(
                DatabaseClientsTable.clientIdKey.from(currentMainItem),
                currentTags!!.elementAt(index++)
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
                        DatabaseClientsTable.profileWithTagKey.name,
                        DatabaseClientsTable.profileWithTagKey.toAttrValue(
                            DatabaseClientsTable.tagKeyFor(profileId, newTag)
                        )
                    )
                ),
                // Override condition now based on secondaryClientIdKey
                versionAndClientIdKeyConditionExpression(currentVersion, secondaryClientIdKey),
            )
        }

        // 2. Delete secondary items if additional tags in current item
        currentTags?.subList(commonTagCount, currentTags.size)?.forEach { currentTag ->
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
        newTags?.subList(commonTagCount, newTags.size)?.forEach { newTag ->
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
                        DatabaseClientsTable.profileWithTagKey.name,
                        DatabaseClientsTable.profileWithTagKey.toAttrValue(
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
            val message = "Unable to update client with id: '${attributes.clientId}' from profile '$profileId'"
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
        val potentialKeys = createPotentialKeys(profileId, filters, activeClientsOnly, sortRequest)
        val indexAndKeys = QueryHelper.findIndexAndKeysFrom(DatabaseClientsTable, potentialKeys)

        QueryHelper.validateRequest(
            indexAndKeys.useScan,
            _configuration.getAllowTableScans(),
            DatabaseClientsTable.queryCapabilities(),
            null,
            sortRequest
        )

        val (values, encodedCursor) = QueryHelper.list(
            _dynamoDBClient,
            _json,
            tableName(),
            indexAndKeys,
            sortRequest?.sortOrder?.let { it == ResourceQuery.Sorting.SortOrder.ASCENDING } ?: true,
            paginationRequest?.count,
            paginationRequest?.cursor,
            ::toLastEvaluatedKey
        )

        val items = values
            .map { it.toAttributes() }
            .toList()

        return PaginatedDataAccessResult(items, encodedCursor)
    }

    override fun getClientCountBy(
        profileId: String,
        filters: DatabaseClientAttributesFiltering?,
        activeClientsOnly: Boolean
    ): Long {
        TODO("Not yet implemented")
    }

    private fun createPotentialKeys(
        profileId: String,
        filters: DatabaseClientAttributesFiltering?,
        activeClientsOnly: Boolean,
        sortRequest: DatabaseClientAttributesSorting? = null
    ): QueryHelper.PotentialKeys {
        val potentialPartitionKeys: MutableMap<DynamoDBAttribute<*>, Any> = mutableMapOf()

        filters?.clientNameFilter?.let { clientName ->
            potentialPartitionKeys[DatabaseClientsTable.clientNameKey] = clientName
        }
        filters?.tagsFilter?.let { tags: Set<String> ->
            // TODO support multiple tags - for the first release, only a single tag may be provided
            tags.firstOrNull()?.let { onlyTag ->
                potentialPartitionKeys[DatabaseClientsTable.profileWithTagKey] =
                    DatabaseClientsTable.profileWithTagKey.toAttrValue(
                        DatabaseClientsTable.tagKeyFor(profileId, onlyTag)
                    ).s()
            }
        }

        val potentialSortKeys: MutableMap<DynamoDBAttribute<*>, Any> = mutableMapOf()

        val sortBy = DatabaseClientsTable.queryCapabilities().attributeMap[sortRequest?.sortBy]
        if (sortBy != null) {
            potentialSortKeys[sortBy] = QueryHelper.NoValueForSortKeys()
        }

        val potentialFilterKeys: MutableMap<DynamoDBAttribute<*>, Any> = mutableMapOf()
        if (activeClientsOnly) {
            potentialFilterKeys[DatabaseClientsTable.status] = DatabaseClientStatus.ACTIVE.name
        }

        @Suppress("UNCHECKED_CAST")
        return QueryHelper.PotentialKeys(
            potentialPartitionKeys as Map<DynamoDBAttribute<Any>, Any>,
            potentialSortKeys as Map<DynamoDBAttribute<Any>, Any>,
            potentialFilterKeys as Map<DynamoDBAttribute<Any>, Any>
        )
    }

    companion object {
        private val logger: Logger =
            LoggerFactory.getLogger(DynamoDBDatabaseClientDataAccessProvider::class.java)

        // Retry count upon delete and update transactions
        private const val N_OF_ATTEMPTS = 3

        private val oldVersion = StringAttribute("oldVersion")

        private val _conditionExpressionBuilder = ExpressionBuilder(
            "${DatabaseClientsTable.version.hashName} = ${oldVersion.colonName} AND " +
                    "${DatabaseClientsTable.clientIdKey.hashName} = ${DatabaseClientsTable.clientIdKey.colonName}",
            DatabaseClientsTable.version,
            DatabaseClientsTable.clientIdKey
        )

        private fun DynamoDBItem.version(): Long =
            DatabaseClientsTable.version.optionalFrom(this)
                ?: throw SchemaErrorException(DatabaseClientsTable, DatabaseClientsTable.version)
    }

    private fun toLastEvaluatedKey(item: Map<String, AttributeValue>): Map<String, AttributeValue> =
        mapOf(DatabaseClientsTable.clientIdKey.name to DatabaseClientsTable.clientIdKey.attributeValueFrom(item))

    private fun DatabaseClientAttributes.toItem(
        profileId: String,
        created: Long,
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
        DatabaseClientsTable.profileId.addTo(item, profileId)
        DatabaseClientsTable.created.addTo(item, created)
        DatabaseClientsTable.updated.addTo(item, updated)

        // Nullable attributes
        DatabaseClientsTable.status.addToNullable(item, status?.name)
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
            // DDB-specific attributes ignored: PROFILE_ID, CLIENT_NAME_KEY, TAG_KEY, VERSION,
            // as not part of DatabaseClientAttributes

            // Non-nullable attributes
            add(DatabaseClientAttributeKeys.NAME, DatabaseClientsTable.clientName.from(item))
            add(DatabaseClientAttributesHelper.ATTRIBUTES, DatabaseClientsTable.attributes.optionalFrom(item) ?: "{}")
            val clientId = DatabaseClientsTable.clientIdFrom(DatabaseClientsTable.clientIdKey.from(item))
            add(DatabaseClientAttributeKeys.CLIENT_ID, clientId)

            // Nullable attributes
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

        val rawAttributes = Attributes.of(result)
        // Parse ATTRIBUTES attribute
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
            oldVersion.colonName to DatabaseClientsTable.version.toAttrValue(version),
            DatabaseClientsTable.clientIdKey.toExpressionNameValuePair(clientIdKey)
        )
    }

    private fun DatabaseClientAttributes.addConfigurationReferencesTo(commonItem: MutableMap<String, AttributeValue>): MutableMap<String, AttributeValue> {
        val item = commonItem.toMutableMap()
        ListStringAttribute(SCOPES).addToNullable(item, scopes)
        ListStringAttribute(AUDIENCES).addToNullable(item, audiences)
        StringAttribute(SUBJECT_TYPE).addToNullable(item, subjectType)
        StringAttribute(CLAIM_MAPPER_ID).addToNullable(item, claimMapperId)
        StringAttribute(USERINFO_SIGNED_ISSUER_ID).addToNullable(item, userInfoSignedIssuerId)
        StringAttribute(REDIRECT_URI_VALIDATION_POLICY_ID).addToNullable(item, redirectUriValidationPolicyId)

        val userAuthenticationConfigAttributes: UserAuthenticationConfigAttributes? = userAuthentication
        ListStringAttribute(REQUIRED_CLAIMS).addToNullable(
            item,
            userAuthenticationConfigAttributes?.requiredClaims
        )
        StringAttribute(TEMPLATE_AREA).addToNullable(
            item,
            userAuthenticationConfigAttributes?.templateArea?.orElse(null)
        )
        ListStringAttribute(AUTHENTICATOR_FILTERS).addToNullable(
            item,
            userAuthenticationConfigAttributes?.authenticatorFilters
        )
        StringAttribute(BACKCHANNEL_LOGOUT_HTTP_CLIENT_ID).addToNullable(
            item,
            userAuthenticationConfigAttributes?.httpClient?.orElse(null)
        )
        ListStringAttribute(CONSENTORS).addToNullable(
            item, Optional.ofNullable(userAuthenticationConfigAttributes)
                .flatMap(UserAuthenticationConfigAttributes::getConsent)
                .map(UserConsentAttributes::getConsentors)
                .orElse(null)
        )

        // Database client has been previously validated and capabilities cannot be null here
        val capabilities: ClientCapabilitiesAttributes = capabilities!!
        StringAttribute(ROPC_CREDENTIAL_MANAGER_ID).addToNullable(
            item,
            capabilities.ropcCapability?.credentialManagerId
        )
        ListStringAttribute(AUTHENTICATORS).addToNullable(
            item,
            capabilities.backchannelCapability?.allowedAuthenticators
        )

        val jwtAssertionAttributes: JwtAssertionAttributes? = capabilities.assertionCapability?.jwtAssertion
        val jwtSigningAttributes: JwtSigningAttributes? = jwtAssertionAttributes?.jwtSigning
        jwtSigningAttributes?.let {
            it.matchAll(
                {
                    // The symmetric key is not a reference but the encrypted actual encryption key.
                },
                { asymmetricKey: String? ->
                    StringAttribute(JWT_ASSERTION_ASYMMETRIC_KEY_ID).addToNullable(item, asymmetricKey)
                }
            ) { jwksUri: JwksUri? ->
                StringAttribute(JWT_ASSERTION_JWKS_URI_CLIENT_ID).addToNullable(item, jwksUri?.httpClientId)
            }
        }
        StringAttribute(HAAPI_POLICY_ID).addToNullable(
            item,
            capabilities.haapiCapability?.attestation?.getOptionalValue(ClientAttestationAttributes.POLICY_ID) as String?
        )

        // Primary client authentication
        addClientAuthentication(item, true)
        // Secondary client authentication
        addClientAuthentication(item, false)

        StringAttribute(REQUEST_OBJECT_BY_REFERENCE_HTTP_CLIENT_ID).addToNullable(
            item, requestObjectConfig?.byRefRequestObjectConfig?.httpClientId
        )
        StringAttribute(ID_TOKEN_JWE_ENCRYPTION_KEY_ID).addToNullable(
            item, idTokenConfig?.jweEncryptionConfig?.encryptionKeyId
        )

        return item
    }

    private fun DatabaseClientAttributes.addClientAuthentication(
        item: MutableMap<String, AttributeValue>,
        isPrimary: Boolean,
    ) {
        val method = if (isPrimary) {
            clientAuthentication?.primaryVerifier?.clientAuthenticationMethod
        } else {
            clientAuthentication?.secondaryVerifier?.clientAuthenticationMethod
        }

        StringAttribute(
            if (isPrimary) {
                PRIMARY_CLIENT_AUTHENTICATION
            } else {
                SECONDARY_CLIENT_AUTHENTICATION
            }
        ).addToNullable(item, method?.clientAuthenticationType?.name)

        StringAttribute(
            if (isPrimary) {
                PRIMARY_MUTUAL_TLS_CLIENT_CERTIFICATE_ID
            } else {
                SECONDARY_MUTUAL_TLS_CLIENT_CERTIFICATE_ID
            }
        ).addToNullable(
            item,
            ((method as? ClientAuthenticationVerifier.MutualTlsVerifier)?.mutualTls
                ?.mutualTls as? MutualTlsAttributes.PinnedCertificate)?.clientCertificate
        )

        StringAttribute(
            if (isPrimary) {
                PRIMARY_ASYMMETRIC_KEY_ID
            } else {
                SECONDARY_ASYMMETRIC_KEY_ID
            }
        ).addToNullable(
            item,
            (method as? ClientAuthenticationVerifier.AsymmetricKey)?.keyId
        )

        StringAttribute(
            if (isPrimary) {
                PRIMARY_CREDENTIAL_MANAGER_ID
            } else {
                SECONDARY_CREDENTIAL_MANAGER_ID
            }
        ).addToNullable(
            item,
            (method as? ClientAuthenticationVerifier.CredentialManager)?.credentialManagerId
        )
    }
}
