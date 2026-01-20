/*
 *  Copyright 2025 Curity AB
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
import io.curity.identityserver.plugin.dynamodb.helpers.DatabaseServiceProviderAttributesHelper
import io.curity.identityserver.plugin.dynamodb.query.BinaryAttributeExpression
import io.curity.identityserver.plugin.dynamodb.query.BinaryAttributeOperator.Co
import io.curity.identityserver.plugin.dynamodb.query.BinaryAttributeOperator.Eq
import io.curity.identityserver.plugin.dynamodb.query.DynamoDBQueryBuilder
import io.curity.identityserver.plugin.dynamodb.query.Index
import io.curity.identityserver.plugin.dynamodb.query.LogicalExpression
import io.curity.identityserver.plugin.dynamodb.query.LogicalOperator.And
import io.curity.identityserver.plugin.dynamodb.query.LogicalOperator.Or
import io.curity.identityserver.plugin.dynamodb.query.QueryHelper
import io.curity.identityserver.plugin.dynamodb.query.QueryPlan
import io.curity.identityserver.plugin.dynamodb.query.TableQueryCapabilities
import io.curity.identityserver.plugin.dynamodb.query.and
import io.curity.identityserver.plugin.dynamodb.query.normalize
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.attribute.Attribute
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.UserAuthenticationConfigAttributes
import se.curity.identityserver.sdk.attribute.UserAuthenticationConfigAttributes.AUTHENTICATOR_FILTERS
import se.curity.identityserver.sdk.attribute.UserAuthenticationConfigAttributes.TEMPLATE_AREA
import se.curity.identityserver.sdk.attribute.scim.v2.Meta
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes
import se.curity.identityserver.sdk.attribute.serviceprovider.database.DatabaseServiceProviderAttributes
import se.curity.identityserver.sdk.attribute.serviceprovider.database.DatabaseServiceProviderAttributes.DatabaseServiceProviderAttributeKeys
import se.curity.identityserver.sdk.attribute.serviceprovider.database.DatabaseServiceProviderAttributes.DatabaseServiceProviderStatus
import se.curity.identityserver.sdk.data.query.ResourceQuery
import se.curity.identityserver.sdk.datasource.DatabaseServiceProviderDataAccessProvider
import se.curity.identityserver.sdk.datasource.pagination.PaginatedDataAccessResult
import se.curity.identityserver.sdk.datasource.pagination.PaginationRequest
import se.curity.identityserver.sdk.datasource.query.DatabaseServiceProviderAttributesFiltering
import se.curity.identityserver.sdk.datasource.query.DatabaseServiceProviderAttributesSorting
import se.curity.identityserver.sdk.errors.ConflictException
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.ProjectionType
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.Select
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import java.time.Instant
import io.curity.identityserver.plugin.dynamodb.query.Expression as QueryExpression


class DynamoDBDatabaseServiceProviderDataAccessProvider(
    private val _configuration: DynamoDBDataAccessProviderConfiguration,
    private val _dynamoDBClient: DynamoDBClient
) : DatabaseServiceProviderDataAccessProvider {
    private val _json = _configuration.getJsonHandler()

    object DatabaseServiceProvidersTable : TableWithCapabilities("curity-database-service-providers") {
        const val SERVICE_PROVIDER_ID_KEY = "service_provider_id_key"
        const val SERVICE_PROVIDER_NAME_KEY = "service_provider_name_key"
        const val TAG_KEY = "tag_key"
        const val VERSION = "version"
        private const val KEY_VALUE_SEPARATOR = "#"
        private const val KEY_ESCAPE_CHARACTER = "\\"
        private const val KEY_ESCAPED_SEPARATOR = KEY_ESCAPE_CHARACTER + KEY_VALUE_SEPARATOR

        // Table Partition Key (PK)
        val profileId = StringAttribute(DatabaseServiceProviderAttributesHelper.PROFILE_ID)

        // DynamoDB-specific, composite string made up of service provider's id and tag, or service provider's id only
        // Table Sort Key (SK)
        val serviceProviderIdKey = object : UniqueStringAttribute("service_provider_id_key", "") {
            override fun uniquenessValueFrom(value: String) = value
        }

        // DynamoDB-specific, composite string made up of profileId and serviceProviderName
        // PK for serviceProviderName-based GSIs
        val serviceProviderNameKey = object : UniqueStringAttribute(SERVICE_PROVIDER_NAME_KEY, "") {
            override fun uniquenessValueFrom(value: String) = value
        }

        // DynamoDB-specific, composite string made up of profileId and an individual item from tags
        // PK for tag-based GSIs
        val tagKey = object : UniqueStringAttribute(TAG_KEY, "") {
            override fun uniquenessValueFrom(value: String) = value
        }

        // DynamoDB-specific, attribute version
        val version = NumberLongAttribute(VERSION)

        // SKs for GSIs & LSIs
        val serviceProviderName = StringAttribute(DatabaseServiceProviderAttributesHelper.SERVICE_PROVIDER_NAME_COLUMN)
        val created = NumberLongAttribute(Meta.CREATED)
        val updated = NumberLongAttribute(Meta.LAST_MODIFIED)

        // Non-key attributes
        val tags = ListStringAttribute(DatabaseServiceProviderAttributeKeys.TAGS)
        val enabled = StringAttribute(DatabaseServiceProviderAttributeKeys.ENABLED)

        // Duplicates serviceProviderIdKey to be used in filter expressions, as not a key (vs. serviceProviderIdKey)
        val serviceProviderId = StringAttribute(DatabaseServiceProviderAttributeKeys.ID)

        val attributes = StringAttribute(DatabaseServiceProviderAttributesHelper.ATTRIBUTES)

        // Base table primary key
        val compositePrimaryKey = CompositePrimaryKey(profileId, serviceProviderIdKey)

        // Composite string helpers
        fun tagKeyFor(profileId: String, tag: String) = compositeKeyFromComponents(profileId, tag)
        fun serviceProviderIdKeyFor(serviceProviderId: String, tag: String) = compositeKeyFromComponents(serviceProviderId, tag)
        fun serviceProviderNameKeyFor(profileId: String, serviceProviderName: String) = compositeKeyFromComponents(profileId, serviceProviderName)
        fun serviceProviderIdFrom(serviceProviderIdKey: String) = serviceProviderIdKey.splitKeyComponents().first

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
        private val serviceProviderNameCreatedIndex =
            PartitionAndSortIndex("service_provider_name-created-index", serviceProviderNameKey, created)
        private val serviceProviderNameUpdatedIndex =
            PartitionAndSortIndex("service_provider_name-lastModified-index", serviceProviderNameKey, updated)
        private val serviceProviderNameIndex =
            PartitionAndSortIndex("service_provider_name-index", serviceProviderNameKey, serviceProviderName)
        private val tagCreatedIndex =
            PartitionAndSortIndex("tag-created-index", tagKey, created)
        private val tagUpdatedIndex =
            PartitionAndSortIndex("tag-last_modified-index", tagKey, updated)
        private val tagServiceProviderNameIndex =
            PartitionAndSortIndex("tag-service_provider_name-index", tagKey, serviceProviderName)

        // LSIs
        private val lsiCreatedIndex =
            PartitionAndSortIndex("lsi-created-index", profileId, created)
        private val lsiUpdatedIndex =
            PartitionAndSortIndex("lsi-last_modified-index", profileId, updated)
        private val lsiServiceProviderNameIndex =
            PartitionAndSortIndex("lsi-service_provider_name-index", profileId, serviceProviderNameKey)

        // Projected attributes to GSIs/LSIs
        private val commonProjectedAttributes = listOf(
            DatabaseServiceProviderAttributeKeys.ENABLED,
            DatabaseServiceProviderAttributeKeys.TAGS,
            DatabaseServiceProviderAttributesHelper.ATTRIBUTES,
        )
        private val projectedAttributesForCreatedSortKey = mutableListOf(
            DatabaseServiceProviderAttributesHelper.SERVICE_PROVIDER_NAME_COLUMN,
            DatabaseServiceProviderAttributeKeys.ID,
            Meta.LAST_MODIFIED,
        ).apply { addAll(commonProjectedAttributes) }.toList()
        private val projectedAttributesForUpdatedSortKey = mutableListOf(
            DatabaseServiceProviderAttributesHelper.SERVICE_PROVIDER_NAME_COLUMN,
            DatabaseServiceProviderAttributeKeys.ID,
            Meta.CREATED,
        ).apply { addAll(commonProjectedAttributes) }.toList()
        private val projectedAttributesForServiceProviderNameSortKey = mutableListOf(
            DatabaseServiceProviderAttributeKeys.ID,
            Meta.CREATED,
            Meta.LAST_MODIFIED,
        ).apply { addAll(commonProjectedAttributes) }.toList()
        private val projectedAttributesForServiceProviderNameKeySortKey = mutableListOf(
            DatabaseServiceProviderAttributesHelper.SERVICE_PROVIDER_NAME_COLUMN
        ).apply { addAll(projectedAttributesForServiceProviderNameSortKey) }.toList()

        override fun queryCapabilities(): TableQueryCapabilities = object : TableQueryCapabilities(
            indexes = listOf(
                Index.from(compositePrimaryKey),
                Index.from(serviceProviderNameCreatedIndex, ProjectionType.INCLUDE, projectedAttributesForCreatedSortKey),
                Index.from(serviceProviderNameUpdatedIndex, ProjectionType.INCLUDE, projectedAttributesForUpdatedSortKey),
                Index.from(serviceProviderNameIndex, ProjectionType.INCLUDE, projectedAttributesForServiceProviderNameSortKey),
                Index.from(tagCreatedIndex, ProjectionType.INCLUDE, projectedAttributesForCreatedSortKey),
                Index.from(tagUpdatedIndex, ProjectionType.INCLUDE, projectedAttributesForUpdatedSortKey),
                Index.from(tagServiceProviderNameIndex, ProjectionType.INCLUDE, projectedAttributesForServiceProviderNameSortKey),
                Index.from(lsiCreatedIndex, ProjectionType.INCLUDE, projectedAttributesForCreatedSortKey),
                Index.from(lsiUpdatedIndex, ProjectionType.INCLUDE, projectedAttributesForUpdatedSortKey),
                Index.from(lsiServiceProviderNameIndex, ProjectionType.INCLUDE, projectedAttributesForServiceProviderNameKeySortKey),
            ),
            attributeMap = mapOf(
                DatabaseServiceProviderAttributesHelper.PROFILE_ID to profileId,
                SERVICE_PROVIDER_ID_KEY to serviceProviderIdKey,
                DatabaseServiceProviderAttributeKeys.ID to serviceProviderId,
                SERVICE_PROVIDER_NAME_KEY to serviceProviderNameKey,
                DatabaseServiceProviderAttributeKeys.NAME to serviceProviderName,
                TAG_KEY to tagKey,
                DatabaseServiceProviderAttributeKeys.TAGS to tags,
                Meta.CREATED to created,
                Meta.LAST_MODIFIED to updated,
                DatabaseServiceProviderAttributeKeys.ENABLED to enabled,
                DatabaseServiceProviderAttributesHelper.ATTRIBUTES to attributes,
            )
        ) {
            override fun getGsiCount() = 6
            override fun getLsiCount() = 3
        }

        // Return primary key for provided values
        fun primaryKey(pkValue: String, skValue: String) =
            mapOf(profileId.toNameValuePair(pkValue), serviceProviderIdKey.toNameValuePair(skValue))
    }

    override fun getServiceProviderById(serviceProviderId: String, profileId: String): DatabaseServiceProviderAttributes? {
        logger.debug("Getting database service provider with id: '$serviceProviderId' in profile: '$profileId'")

        return getItemById(serviceProviderId, profileId)?.toAttributes()
    }

    private fun getItemById(serviceProviderId: String, profileId: String): DynamoDBItem? {
        val request = GetItemRequest.builder()
            .tableName(tableName())
            .key(DatabaseServiceProvidersTable.primaryKey(profileId, serviceProviderId))
            .consistentRead(true)
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        return response.item()
    }

    override fun create(attributes: DatabaseServiceProviderAttributes, profileId: String): DatabaseServiceProviderAttributes {
        logger.debug("Creating database service provider with id: '${attributes.id}' in profile: '$profileId'")

        // the commonItem contains the attributes that will be on both the primary and secondary items
        val now = Instant.now().epochSecond
        val commonItem = attributes.toItem(profileId, now, now)
        commonItem.addAttr(DatabaseServiceProvidersTable.version, 0)

        // Item must not already exist
        val writeConditionExpression = "attribute_not_exists(${DatabaseServiceProvidersTable.serviceProviderIdKey.name})"

        val transactionItems = mutableListOf<TransactWriteItem>()
        // Create operation for the main item
        addTransactionItem(
            attributes.addConfigurationReferencesTo(commonItem),
            // Main item's specific attributes
            listOfNotNull(
                // Add serviceProviderIdKey as SK for base table
                // For the main item, it is not composite and holds serviceProviderId only
                Pair(
                    DatabaseServiceProvidersTable.serviceProviderIdKey.name,
                    DatabaseServiceProvidersTable.serviceProviderIdKey.toAttrValue(attributes.id)
                ),
                // Add composite serviceProviderNameKey as PK for serviceProviderName-based GSIs,
                // if serviceProviderName is not present add just key, e.g. {profile}#
                // => we need to have all serviceProviders present in the Indexes (even without name)
                Pair(
                    DatabaseServiceProvidersTable.serviceProviderNameKey.name,
                    DatabaseServiceProvidersTable.serviceProviderNameKey.toAttrValue(
                        DatabaseServiceProvidersTable.serviceProviderNameKeyFor(profileId, attributes.name)
                    )
                ),
                // If the service provider has not tags create an empty tagKey attribute value, e.g.: `{profileId}#`
                // It's used in case when filtering service providers with no tags
                // For the list of use-cases consult the ticket: #IS-8010
                Pair(
                    DatabaseServiceProvidersTable.tagKey.name,
                    DatabaseServiceProvidersTable.tagKey.toAttrValue(
                        DatabaseServiceProvidersTable.tagKeyFor(profileId, ""))
                ).takeIf { attributes.tags.isNullOrEmpty() },
            ).toMap(),
            transactionItems,
            writeConditionExpression
        )

        // One create operation per secondary item, i.e. per tag
        attributes.tags?.forEach { tag ->
            addTransactionItem(
                commonItem,
                // Secondary item's specific attributes
                mapOf(
                    // Add composite serviceProviderIdKey as SK for base table
                    Pair(
                        DatabaseServiceProvidersTable.serviceProviderIdKey.name,
                        DatabaseServiceProvidersTable.serviceProviderIdKey.toAttrValue(
                            DatabaseServiceProvidersTable.serviceProviderIdKeyFor(attributes.id, tag)
                        )
                    ),
                    // Add composite tagKey as PK for tag-based GSIs
                    Pair(
                        DatabaseServiceProvidersTable.tagKey.name,
                        DatabaseServiceProvidersTable.tagKey.toAttrValue(
                            DatabaseServiceProvidersTable.tagKeyFor(profileId, tag)
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
            val message = "Unable to create service provider with id: '${attributes.id}' in profile '$profileId'"
            logger.trace(message, e)

            if (e.isTransactionCancelledDueToConditionFailure()) {
                throw ConflictException("$message as uniqueness check failed")
            }
            throw e
        }
    }

    override fun update(attributes: DatabaseServiceProviderAttributes, profileId: String): DatabaseServiceProviderAttributes? {
        logger.debug("Updating database service provider with id: '${attributes.id}' in profile '$profileId'")

        val currentMainItem = getItemById(attributes.id, profileId)
            ?: throw RuntimeException("Service provider with ID '$profileId:${attributes.id}' could not be updated.")

        retry("update", N_OF_ATTEMPTS) {
            tryUpdate(attributes, profileId, currentMainItem)
        }

        return getServiceProviderById(attributes.id, profileId)
    }

    private fun tryUpdate(
        attributes: DatabaseServiceProviderAttributes,
        profileId: String,
        currentMainItem: DynamoDBItem
    ): TransactionAttemptResult<Unit> {
        val currentVersion = currentMainItem.version()
        val newVersion = currentVersion + 1

        // Preserve created attribute
        val created = DatabaseServiceProvidersTable.created.from(currentMainItem)
        val commonItem = attributes.toItem(profileId, created, updated = Instant.now().epochSecond)
        commonItem.addAttr(DatabaseServiceProvidersTable.version, newVersion)

        // Current item's serviceProviderIdKey based on serviceProviderId only, must be same as new serviceProviderId!
        val currentServiceProviderIdKey = DatabaseServiceProvidersTable.serviceProviderIdKey.from(currentMainItem)

        val updateBuilder = UpdateBuilderWithMultipleUniquenessConstraints(
            _configuration,
            DatabaseServiceProvidersTable,
            // Common item, i.e. without configuration references
            commonItem,
            _pkAttribute = StringAttribute(DatabaseServiceProvidersTable.profileId.name),
            versionAndServiceProviderIdKeyConditionExpression(currentVersion, currentServiceProviderIdKey),
            _pkValue = profileId,
            _skAttribute = StringAttribute(DatabaseServiceProvidersTable.serviceProviderIdKey.name),
        )

        // Update main item
        updateBuilder.handleUniqueAttribute(
            DatabaseServiceProvidersTable.serviceProviderIdKey,
            before = currentServiceProviderIdKey,
            // New serviceProviderIdKey based on serviceProviderId only
            after = attributes.id,
            additionalAttributes = listOfNotNull(
                // Add serviceProviderIdKey as SK for base table. For the main item, it is not composite and holds serviceProviderId only
                Pair(
                    DatabaseServiceProvidersTable.serviceProviderIdKey.name,
                    DatabaseServiceProvidersTable.serviceProviderIdKey.toAttrValue(attributes.id)
                ),
                // Add composite serviceProviderNameKey as PK for serviceProviderName-based GSIs
                Pair(
                    DatabaseServiceProvidersTable.serviceProviderNameKey.name,
                    DatabaseServiceProvidersTable.serviceProviderNameKey.toAttrValue(
                        DatabaseServiceProvidersTable.serviceProviderNameKeyFor(profileId, attributes.name)
                    )
                ),
                Pair(
                    DatabaseServiceProvidersTable.tagKey.name,
                    DatabaseServiceProvidersTable.tagKey.toAttrValue(
                        DatabaseServiceProvidersTable.tagKeyFor(profileId, "")
                    )
                ).takeIf { attributes.tags.isNullOrEmpty() },
            ).toMap(),
            null,
            // Override commonItem by adding configuration references for the main item only
            attributes.addConfigurationReferencesTo(commonItem)
        )

        val currentTags = DatabaseServiceProvidersTable.tags.optionalFrom(currentMainItem) as List<String>?
        val newTags: List<String>? = attributes.tags
        val commonTagCount = Integer.min(
            currentTags?.size ?: 0,
            newTags?.size ?: 0
        )

        // 1. Update secondary items for the first commonTagCount tags
        var index = 0
        newTags?.subList(0, commonTagCount)?.forEach { newTag ->
            // Secondary item's serviceProviderIdKey based on serviceProviderId and tag with same index as the new one
            val secondaryServiceProviderIdKey = DatabaseServiceProvidersTable.serviceProviderIdKeyFor(
                DatabaseServiceProvidersTable.serviceProviderIdKey.from(currentMainItem),
                currentTags!!.elementAt(index++)
            )

            // Update secondary item for newTag
            updateBuilder.handleUniqueAttribute(
                DatabaseServiceProvidersTable.serviceProviderIdKey,
                before = secondaryServiceProviderIdKey,
                // New serviceProviderIdKey based on serviceProviderId and new tag
                after = DatabaseServiceProvidersTable.serviceProviderIdKeyFor(attributes.id, newTag),
                additionalAttributes = mapOf(
                    // Add composite tagKey as PK for tag-based GSIs
                    Pair(
                        DatabaseServiceProvidersTable.tagKey.name,
                        DatabaseServiceProvidersTable.tagKey.toAttrValue(
                            DatabaseServiceProvidersTable.tagKeyFor(profileId, newTag)
                        )
                    )
                ),
                // Override condition now based on secondaryServiceProviderIdKey
                versionAndServiceProviderIdKeyConditionExpression(currentVersion, secondaryServiceProviderIdKey),
            )
        }

        // 2. Delete secondary items if additional tags in current item
        currentTags?.subList(commonTagCount, currentTags.size)?.forEach { currentTag ->
            // Secondary item's serviceProviderIdKey based on serviceProviderId and current tag
            val secondaryServiceProviderIdKey = DatabaseServiceProvidersTable.serviceProviderIdKeyFor(
                DatabaseServiceProvidersTable.serviceProviderIdKey.from(currentMainItem),
                currentTag
            )

            // Delete secondary item for currentTag
            updateBuilder.handleUniqueAttribute(
                DatabaseServiceProvidersTable.serviceProviderIdKey,
                before = secondaryServiceProviderIdKey,
                after = null,
                additionalAttributes = mapOf(),
                versionAndServiceProviderIdKeyConditionExpression(currentVersion, secondaryServiceProviderIdKey),
            )
        }

        // 3. Create secondary items if additional new tags
        newTags?.subList(commonTagCount, newTags.size)?.forEach { newTag ->
            // Secondary item's serviceProviderIdKey based on serviceProviderId and new tag
            val secondaryServiceProviderIdKey = DatabaseServiceProvidersTable.serviceProviderIdKeyFor(
                DatabaseServiceProvidersTable.serviceProviderIdKey.from(currentMainItem),
                newTag
            )

            // Create secondary item for newTag
            updateBuilder.handleUniqueAttribute(
                DatabaseServiceProvidersTable.serviceProviderIdKey,
                before = null,
                after = secondaryServiceProviderIdKey,
                additionalAttributes = mapOf(
                    // Add composite tagKey as PK for tag-based GSIs
                    Pair(
                        DatabaseServiceProvidersTable.tagKey.name,
                        DatabaseServiceProvidersTable.tagKey.toAttrValue(
                            DatabaseServiceProvidersTable.tagKeyFor(profileId, newTag)
                        )
                    )
                )
            )
        }

        try {
            _dynamoDBClient.transactionWriteItems(updateBuilder.build())

            return TransactionAttemptResult.Success(Unit)
        } catch (e: Exception) {
            val message = "Unable to update service provider with id: '${attributes.id}' from profile '$profileId'"
            logger.trace(message, e)
            if (e.isTransactionCancelledDueToConditionFailure()) {
                return TransactionAttemptResult.Failure(
                    ConflictException("$message as version check failed")
                )
            }
            throw e
        }
    }

    override fun delete(serviceProviderId: String, profileId: String): Boolean {
        logger.debug("Deleting database service provider with id: '$serviceProviderId' from profile '$profileId'")

        val result = retry("delete", N_OF_ATTEMPTS) {
            tryDelete(serviceProviderId, profileId)
        }

        return result
    }

    private fun tryDelete(serviceProviderId: String, profileId: String): TransactionAttemptResult<Boolean> {
        logger.debug("Deleting database service provider with id: '$serviceProviderId' from profile '$profileId'")

        // Deleting a database service provider requires the deletion of the main item and all the secondary items.
        // A `getItem` is needed to obtain the `tags` required to compute the secondary item keys
        val getItemResponse = _dynamoDBClient.getItem(
            GetItemRequest.builder()
                .tableName(tableName())
                .key(DatabaseServiceProvidersTable.primaryKey(profileId, serviceProviderId))
                .build()
        )

        if (!getItemResponse.hasItem() || getItemResponse.item().isEmpty()) {
            return TransactionAttemptResult.Success(false)
        }

        val item = getItemResponse.item()
        val version = DatabaseServiceProvidersTable.version.optionalFrom(item)
            ?: throw SchemaErrorException(DatabaseServiceProvidersTable, DatabaseServiceProvidersTable.version)

        // Create a transaction with all the items (main and secondary) deletions,
        // conditioned to the version not having changed - optimistic concurrency
        val transactionItems = mutableListOf<TransactWriteItem>()

        // Delete operation for the main item
        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(tableName())
                    it.key(DatabaseServiceProvidersTable.primaryKey(profileId, serviceProviderId))
                    it.conditionExpression(
                        versionAndServiceProviderIdKeyConditionExpression(version, serviceProviderId)
                    )
                }
                .build()
        )

        // One delete operation per secondary item, i.e. per tag
        DatabaseServiceProvidersTable.tags.optionalFrom(item)?.forEach { tag ->
            transactionItems.add(
                TransactWriteItem.builder()
                    .delete {
                        it.tableName(tableName())
                        it.key(
                            DatabaseServiceProvidersTable.primaryKey(
                                profileId,
                                DatabaseServiceProvidersTable.serviceProviderIdKeyFor(serviceProviderId, tag)
                            )
                        )
                        it.conditionExpression(
                            versionAndServiceProviderIdKeyConditionExpression(
                                version,
                                DatabaseServiceProvidersTable.serviceProviderIdKeyFor(serviceProviderId, tag)
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
            return TransactionAttemptResult.Success(true)
        } catch (e: Exception) {
            val message = "Unable to delete service provider with id: '$serviceProviderId' from profile '$profileId'"
            logger.trace(message, e)
            if (e.isTransactionCancelledDueToConditionFailure()) {
                return TransactionAttemptResult.Failure(
                    ConflictException("$message as version check failed")
                )
            }
            throw e
        }
    }

    override fun getAllServiceProvidersBy(
        profileId: String,
        filters: DatabaseServiceProviderAttributesFiltering?,
        paginationRequest: PaginationRequest?,
        sortRequest: DatabaseServiceProviderAttributesSorting?,
        activeServiceProvidersOnly: Boolean
    ): PaginatedDataAccessResult<DatabaseServiceProviderAttributes> {

        val (index, dynamoDBQuery) = prepareQuery(profileId, filters, sortRequest, activeServiceProvidersOnly)

        val exclusiveStartKey = paginationRequest?.cursor?.takeIf { it.isNotBlank() }
            ?.let { QueryHelper.getExclusiveStartKey(_json, it) }

        val limit = paginationRequest?.count ?: DEFAULT_PAGE_SIZE

        val (values, lastEvaluationKey) = queryWithPagination(
            QueryRequest.builder().tableName(tableName()).configureWith(dynamoDBQuery),
            limit,
            exclusiveStartKey,
            _dynamoDBClient) {
            index.toIndexPrimaryKey(it, DatabaseServiceProvidersTable.compositePrimaryKey)
        }

        val items = values
            .map { it.toAttributes() }
            .toList()

        return PaginatedDataAccessResult(items, QueryHelper.getEncodedCursor(_json, lastEvaluationKey))
    }

    override fun getServiceProviderCountBy(
        profileId: String,
        filters: DatabaseServiceProviderAttributesFiltering?,
        activeServiceProvidersOnly: Boolean
    ): Long {
        val (_, dynamoDBQuery) = prepareQuery(profileId, filters, null, activeServiceProvidersOnly)

        val queryRequest = QueryRequest.builder()
            .tableName(tableName())
            .configureWith(dynamoDBQuery)
            .select(Select.COUNT)
            .build()

        return count(queryRequest, _dynamoDBClient)
    }

    /**
     * Business logic in this method is based on Use-case definition (Access Patterns in DynamoDB jargon)
     * which can be found from the ticket https://curity.atlassian.net/browse/IS-8005?focusedCommentId=44369
     *
     * The main idea is, based on the provided input to figure out the right Use Case.
     * Then from Use Case to identify the correct Index to be used
     * by constructing the Primary Key (which is the combination of Partition Key and Sort Key).
     * And also constructing the Key and Filter Expressions.
     */
    private fun prepareQuery(
        profileId: String,
        filters: DatabaseServiceProviderAttributesFiltering?,
        sortRequest: DatabaseServiceProviderAttributesSorting?,
        activeServiceProvidersOnly: Boolean
    ): Pair<Index, DynamoDBQuery> {
        val profileIdAttribute = DatabaseServiceProvidersTable.profileId

        var partitionKeyCondition: BinaryAttributeExpression? = null
        var sortKeyCondition: QueryPlan.RangeCondition? = null
        var sortIndexAttribute: DynamoDBAttribute<*>? = null
        var filterExpression: QueryExpression? = null

        fun addToFilterExpression(expression: QueryExpression) {
            filterExpression = filterExpression?.let { and(it, expression) } ?: expression
        }

        // handle sort attribute first if present
        sortRequest?.sortBy?.let { sortBy ->
            DatabaseServiceProvidersTable.queryCapabilities().attributeMap[sortBy]?.let { attribute ->
                sortIndexAttribute = attribute
            } ?: {
                logger.debug("Sort attribute was not found: $sortBy")
            }
        }

        // handle search terms
        filters?.searchTermsFilter?.takeIf { it.isNotBlank() }?.let {
            partitionKeyCondition = BinaryAttributeExpression(profileIdAttribute, Eq, profileId)

            val expressionPairs : List<Pair<QueryExpression, QueryExpression>> = it.split(" ").map { token ->
                Pair(
                    BinaryAttributeExpression(DatabaseServiceProvidersTable.serviceProviderId, Co, token),
                    BinaryAttributeExpression(DatabaseServiceProvidersTable.serviceProviderName, Co, token),
                )
            }

            val serviceProviderIdExpressions = expressionPairs.map { pair -> pair.first }
                .reduce { current, next -> LogicalExpression(current, And, next) }
            val serviceProviderNameExpressions = expressionPairs.map { pair -> pair.second }
                .reduce { current, next -> LogicalExpression(current, And, next) }

            // ( CONTAINS( service_provider_id, token1 ) [ AND CONTAINS(   service_provider_id, token2 ) ] )
            // OR ( CONTAINS(  service_provider_name, token1 ) [ AND CONTAINS(  service_provider_name, token2 ) ] )
            addToFilterExpression(LogicalExpression(serviceProviderIdExpressions, Or, serviceProviderNameExpressions))
        }

        // handle tags
        filters?.tagsFilter?.let { filterSet ->
            val tags = filterSet.toMutableSet()
            val tagKeyAttribute = DatabaseServiceProvidersTable.tagKey
            if (tags.isEmpty()) {
                partitionKeyCondition =
                    BinaryAttributeExpression(tagKeyAttribute, Eq, DatabaseServiceProvidersTable.tagKeyFor(profileId, ""))
            } else {
                tags.first().let {
                    partitionKeyCondition = BinaryAttributeExpression(
                        tagKeyAttribute, Eq, DatabaseServiceProvidersTable.tagKeyFor(profileId, it)
                    )
                    tags.remove(it)
                }
            }
            tags.forEach {
                addToFilterExpression(BinaryAttributeExpression(DatabaseServiceProvidersTable.tags, Co, it))
            }
        }

        // handle service provider name
        filters?.serviceProviderNameFilter?.takeIf{ it.isNotBlank() }?.let {
            val serviceProviderNameAttribute = DatabaseServiceProvidersTable.serviceProviderName
            val serviceProviderNameAttributeExpression =
                BinaryAttributeExpression(serviceProviderNameAttribute, Eq, it)
            if (partitionKeyCondition == null) {
                partitionKeyCondition = BinaryAttributeExpression(
                    DatabaseServiceProvidersTable.serviceProviderNameKey, Eq,
                    DatabaseServiceProvidersTable.serviceProviderNameKeyFor(profileId, it)
                )
            } else if (sortIndexAttribute == null) {
                sortIndexAttribute = serviceProviderNameAttribute
                sortKeyCondition = QueryPlan.RangeCondition.Binary(serviceProviderNameAttributeExpression)
            } else {
                addToFilterExpression(serviceProviderNameAttributeExpression)
            }
        }

        // handle active service providers
        if (activeServiceProvidersOnly) {
            addToFilterExpression(
                BinaryAttributeExpression(DatabaseServiceProvidersTable.enabled, Eq, DatabaseServiceProviderStatus.ENABLED.name)
            )
        }

        // resolve partition key
        partitionKeyCondition = partitionKeyCondition ?: BinaryAttributeExpression(profileIdAttribute, Eq, profileId)

        // if no sorting is present, choose default based on partition key to ensure a valid index exists
        // For profileId partition: use serviceProviderIdKey (base table)
        // For other partitions: use created timestamp
        sortIndexAttribute = (sortIndexAttribute ?: run {
            if (partitionKeyCondition.attribute == profileIdAttribute) {
                DatabaseServiceProvidersTable.serviceProviderIdKey
            } else {
                DatabaseServiceProvidersTable.created
            }
        }).let {
            // if sorting is by service provider name and partition key is profile_id the sorting key is serviceProviderNameKey
            // if sorting is by service provider id and partition key is profile_id the sorting key is serviceProviderIdKey
            // when sorting by updated/created with profileId partition, fall back to serviceProviderIdKey
            when (it) {
                DatabaseServiceProvidersTable.serviceProviderName if partitionKeyCondition.attribute == profileIdAttribute -> {
                    return@let DatabaseServiceProvidersTable.serviceProviderNameKey
                }
                DatabaseServiceProvidersTable.serviceProviderId if partitionKeyCondition.attribute == profileIdAttribute -> {
                    return@let DatabaseServiceProvidersTable.serviceProviderIdKey
                }
                DatabaseServiceProvidersTable.created if partitionKeyCondition.attribute == profileIdAttribute -> {
                    return@let DatabaseServiceProvidersTable.serviceProviderIdKey
                }
                DatabaseServiceProvidersTable.updated if partitionKeyCondition.attribute == profileIdAttribute -> {
                    return@let DatabaseServiceProvidersTable.serviceProviderIdKey
                }
                else -> {
                    it
                }
            }
        }

        // find the right index
        val index = DatabaseServiceProvidersTable.queryCapabilities().indexes.firstOrNull {
            it.partitionAttribute == partitionKeyCondition.attribute && it.sortAttribute == sortIndexAttribute
        } ?: throw UnsupportedOperationException("Unsupported combination of filter and sort attributes").also {
            logger.debug(
                "Could not find any applicable index for execution; PK:{}, SK:{}, filters:{}, sort:{}",
                partitionKeyCondition, sortIndexAttribute, filters, sortRequest
            )
        }

        // construct the key condition
        val keyCondition = QueryPlan.KeyCondition(index, partitionKeyCondition, sortKeyCondition)
        // and the filter condition
        val products = filterExpression?.let { normalize(it).products.toList() }.orEmpty()

        val dynamoDBQuery = DynamoDBQueryBuilder.buildQuery(keyCondition, products, sortRequest?.sortOrder)

        return Pair(index, dynamoDBQuery)
    }

    companion object {
        private val logger: Logger =
            LoggerFactory.getLogger(DynamoDBDatabaseServiceProviderDataAccessProvider::class.java)

        // Retry count upon delete and update transactions
        private const val N_OF_ATTEMPTS = 3

        private val oldVersion = StringAttribute("oldVersion")

        private val _conditionExpressionBuilder = ExpressionBuilder(
            "${DatabaseServiceProvidersTable.version.hashName} = ${oldVersion.colonName} AND " +
                    "${DatabaseServiceProvidersTable.serviceProviderIdKey.hashName} = ${DatabaseServiceProvidersTable.serviceProviderIdKey.colonName}",
            DatabaseServiceProvidersTable.version,
            DatabaseServiceProvidersTable.serviceProviderIdKey
        )

        private fun DynamoDBItem.version(): Long =
            DatabaseServiceProvidersTable.version.optionalFrom(this)
                ?: throw SchemaErrorException(DatabaseServiceProvidersTable, DatabaseServiceProvidersTable.version)
    }

    private fun DatabaseServiceProviderAttributes.toItem(
        profileId: String,
        created: Long,
        updated: Long
    ): MutableMap<String, AttributeValue> {
        val item = mutableMapOf<String, AttributeValue>()

        // Non-nullable attributes
        // Persist the whole DatabaseServiceProviderAttributes, but non persistable attributes, in the "attributes" attribute
        DatabaseServiceProvidersTable.attributes.addTo(
            item,
            _json.fromAttributes(
                removeAttributes(DatabaseServiceProviderAttributesHelper.DATABASE_SERVICE_PROVIDER_SEEDING_ATTRIBUTES)
            )
        )
        DatabaseServiceProvidersTable.profileId.addTo(item, profileId)
        DatabaseServiceProvidersTable.serviceProviderId.addTo(item, id)
        DatabaseServiceProvidersTable.created.addTo(item, created)
        DatabaseServiceProvidersTable.updated.addTo(item, updated)

        // Nullable attributes
        DatabaseServiceProvidersTable.enabled.addToNullable(item, enabled?.name)
        DatabaseServiceProvidersTable.tags.addToNullable(item, tags)
        // Don't set 'name' if value not valid as secondary index's key
        if (!name.isNullOrEmpty()) {
            DatabaseServiceProvidersTable.serviceProviderName.addTo(item, name)
        }

        return item
    }

    private fun tableName(): String = DatabaseServiceProvidersTable.name(_configuration)

    private fun MutableList<Attribute>.add(name: String, value: String?) = value?.let {
        this.add(Attribute.of(name, it))
    }

    private fun MutableList<Attribute>.add(name: String, value: Collection<String>?) = value?.let {
        this.add(Attribute.of(name, it))
    }

    private fun DynamoDBItem.toAttributes(): DatabaseServiceProviderAttributes {

        val result = mutableListOf<Attribute>()
        val item = this

        result.apply {
            // DDB-specific attributes ignored: PROFILE_ID, SERVICE_PROVIDER_ID_KEY, SERVICE_PROVIDER_NAME_KEY, TAG_KEY, VERSION,
            // as not part of DatabaseServiceProviderAttributes

            // Non-nullable attributes
            add(DatabaseServiceProviderAttributesHelper.ATTRIBUTES, DatabaseServiceProvidersTable.attributes.from(item))
            add(DatabaseServiceProviderAttributeKeys.ID, DatabaseServiceProvidersTable.serviceProviderId.from(item))

            // Nullable attributes
            add(Attribute.of(
                ResourceAttributes.META,
                Meta.of(DatabaseServiceProviderAttributes.RESOURCE_TYPE)
                    .withCreated(
                        DatabaseServiceProvidersTable.created.optionalFrom(
                            item
                        )?.let {
                            Instant.ofEpochSecond(
                                it
                            )
                        }
                    )
                    .withLastModified(
                        DatabaseServiceProvidersTable.updated.optionalFrom(
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
                    DatabaseServiceProviderAttributeKeys.ENABLED, DatabaseServiceProviderStatus.valueOf(
                        DatabaseServiceProvidersTable.enabled.from(item)
                    )
                )
            )
            add(DatabaseServiceProviderAttributeKeys.TAGS, DatabaseServiceProvidersTable.tags.optionalFrom(item)?.takeIf { it.isNotEmpty() })
            // 'serviceProviderName' could have been not set if value were not valid as secondary index's key
            add(DatabaseServiceProviderAttributes.DatabaseServiceProviderAttributeKeys.NAME, DatabaseServiceProvidersTable.serviceProviderName.optionalFrom(item))
        }

        val rawAttributes = Attributes.of(result)
        // Parse ATTRIBUTES attribute
        return DatabaseServiceProviderAttributesHelper.toResource(rawAttributes, ResourceQuery.Exclusions.none(), _json)
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

    private fun versionAndServiceProviderIdKeyConditionExpression(version: Long, serviceProviderIdKey: String) = object : Expression(
        _conditionExpressionBuilder
    ) {
        override val values = mapOf(
            oldVersion.colonName to DatabaseServiceProvidersTable.version.toAttrValue(version),
            DatabaseServiceProvidersTable.serviceProviderIdKey.toExpressionNameValuePair(serviceProviderIdKey)
        )
    }

    private fun DatabaseServiceProviderAttributes.addConfigurationReferencesTo(commonItem: MutableMap<String, AttributeValue>): MutableMap<String, AttributeValue> {
        val item = commonItem.toMutableMap()

        val userAuthenticationConfigAttributes: UserAuthenticationConfigAttributes? = userAuthentication
        StringAttribute(TEMPLATE_AREA).addToNullable(
            item,
            userAuthenticationConfigAttributes?.templateArea?.orElse(null)
        )
        ListStringAttribute(AUTHENTICATOR_FILTERS).addToNullable(
            item,
            userAuthenticationConfigAttributes?.authenticatorFilters
        )

        return item
    }
}
