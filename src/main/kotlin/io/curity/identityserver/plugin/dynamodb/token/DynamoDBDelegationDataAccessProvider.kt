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
package io.curity.identityserver.plugin.dynamodb.token

import io.curity.identityserver.plugin.dynamodb.DynamoDBClient
import io.curity.identityserver.plugin.dynamodb.DynamoDBItem
import io.curity.identityserver.plugin.dynamodb.EnumAttribute
import io.curity.identityserver.plugin.dynamodb.NumberLongAttribute
import io.curity.identityserver.plugin.dynamodb.PartitionAndSortIndex
import io.curity.identityserver.plugin.dynamodb.PartitionKey
import io.curity.identityserver.plugin.dynamodb.PartitionOnlyIndex
import io.curity.identityserver.plugin.dynamodb.StringAttribute
import io.curity.identityserver.plugin.dynamodb.Table
import io.curity.identityserver.plugin.dynamodb.TenantAwarePartitionAndSortIndex
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import io.curity.identityserver.plugin.dynamodb.configureWith
import io.curity.identityserver.plugin.dynamodb.count
import io.curity.identityserver.plugin.dynamodb.getMapValueForPath
import io.curity.identityserver.plugin.dynamodb.mapToAttributeValue
import io.curity.identityserver.plugin.dynamodb.query.BinaryAttributeExpression
import io.curity.identityserver.plugin.dynamodb.query.BinaryAttributeOperator
import io.curity.identityserver.plugin.dynamodb.query.DynamoDBQueryBuilder
import io.curity.identityserver.plugin.dynamodb.query.Index
import io.curity.identityserver.plugin.dynamodb.query.Product
import io.curity.identityserver.plugin.dynamodb.query.QueryPlan
import io.curity.identityserver.plugin.dynamodb.query.QueryPlanner
import io.curity.identityserver.plugin.dynamodb.query.TableQueryCapabilities
import io.curity.identityserver.plugin.dynamodb.query.UnaryAttributeExpression
import io.curity.identityserver.plugin.dynamodb.query.UnaryAttributeOperator
import io.curity.identityserver.plugin.dynamodb.query.UnsupportedQueryException
import io.curity.identityserver.plugin.dynamodb.query.and
import io.curity.identityserver.plugin.dynamodb.query.filterWith
import io.curity.identityserver.plugin.dynamodb.querySequence
import io.curity.identityserver.plugin.dynamodb.scanSequence
import io.curity.identityserver.plugin.dynamodb.toIntOrThrow
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import org.slf4j.MarkerFactory
import se.curity.identityserver.sdk.attribute.AccountAttributes
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.AuthenticationAttributes
import se.curity.identityserver.sdk.data.authorization.Delegation
import se.curity.identityserver.sdk.data.authorization.DelegationConsentResult
import se.curity.identityserver.sdk.data.authorization.DelegationStatus
import se.curity.identityserver.sdk.data.query.ResourceQuery
import se.curity.identityserver.sdk.datasource.DelegationDataAccessProvider
import se.curity.identityserver.sdk.datasource.DelegationDataAccessProvider.SetStatusResult
import se.curity.identityserver.sdk.service.authentication.TenantId
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.Select
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.utils.ImmutableMap
import java.lang.IllegalArgumentException
import kotlin.math.min

class DynamoDBDelegationDataAccessProvider(
    private val _dynamoDBClient: DynamoDBClient,
    private val _configuration: DynamoDBDataAccessProviderConfiguration
) : DelegationDataAccessProvider {
    // Lazy initialization is required to avoid cyclic dependencies while Femto containers are built.
    // TenantId should not be resolved from the configuration at DAP initialization time.
    private val _tenantId: TenantId by lazy {
        _configuration.getTenantId()
    }
    private val _jsonHandler = _configuration.getJsonHandler()

    /*
     * There is one attribute for each property in the Delegation interface:
     * - There is an extra version attribute in the table that doesn't exist in the Delegation interface
     * - The "scopeClaims" property is not stored, since it is deprecated and this DAP doesn't need to support legacy
     * delegation formats.
     */
    object DelegationTable : Table("curity-delegations") {
        val version = StringAttribute("version")
        val id = StringAttribute("id")
        val tenantId = StringAttribute("tenantId")
        val status = EnumAttribute.of<DelegationStatus>("status")
        val owner = StringAttribute("owner")

        val created = NumberLongAttribute("created")
        val expires = NumberLongAttribute("expires")
        val deletableAt = NumberLongAttribute("deletableAt")

        val scope = StringAttribute("scope")
        val claimMap = StringAttribute("claimMap")
        val clientId = StringAttribute("clientId")
        val redirectUri = StringAttribute("redirectUri")
        val authorizationCodeHash = StringAttribute("authorizationCodeHash")
        val authenticationAttributes = StringAttribute("authenticationAttributes")
        val customClaimValues = StringAttribute("customClaimValues")
        val mtlsClientCertificate = StringAttribute("mtlsClientCertificate")
        val mtlsClientCertificateX5TS256 = StringAttribute("mtlsClientCertificateX5TS256")
        val mtlsClientCertificateDN = StringAttribute("mtlsClientCertificateDN")
        val consentResult = StringAttribute("consentResult")
        val claims = StringAttribute("claims")

        val primaryKey = PartitionKey(id)
        val ownerStatusIndex = TenantAwarePartitionAndSortIndex("owner-status-index", owner, status, tenantId)
        val authorizationCodeIndex = PartitionOnlyIndex("authorization-hash-index", authorizationCodeHash)
        val clientStatusIndex = PartitionAndSortIndex("clientId-status-index", clientId, status)

        val queryCapabilities = TableQueryCapabilities(
            indexes = listOf(
                Index.from(primaryKey),
                Index.from(ownerStatusIndex),
                Index.from(authorizationCodeIndex),
                Index.from(clientStatusIndex)
            ),
            attributeMap = mapOf(
                AccountAttributes.USER_NAME to owner,
                Delegation.KEY_OWNER to owner,
                Delegation.KEY_SCOPE to scope,
                Delegation.KEY_CLIENT_ID to clientId,
                "client_id" to clientId,
                Delegation.KEY_REDIRECT_URI to redirectUri,
                "redirect_uri" to redirectUri,
                Delegation.KEY_STATUS to status,
                Delegation.KEY_EXPIRES to expires,
                "externalId" to id,
                "tenantId" to tenantId,
            )
        )
    }

    /*
     * Serializes a Delegation into an Item
     */
    private fun Delegation.toItem(): DynamoDBItem {
        val itemMap = mutableMapOf<String, AttributeValue>()
        addExtraAttributesToItemMap(
            _configuration.getDelegationAttributesFromClaims(),
            "claims", // using the exact same name as the map property
            this.claims,
            itemMap
        )
        addExtraAttributesToItemMap(
            _configuration.getDelegationAttributesFromCustomClaimValues(),
            "customClaimValues", // using the exact same name as the map property
            this.customClaimValues,
            itemMap
        )
        DelegationTable.version.addTo(itemMap, "6.2")
        DelegationTable.id.addTo(itemMap, id)
        DelegationTable.status.addTo(itemMap, status)
        DelegationTable.owner.addTo(itemMap, owner)
        DelegationTable.created.addTo(itemMap, created)
        DelegationTable.expires.addTo(itemMap, expires)
        DelegationTable.deletableAt.addTo(itemMap, expires + _configuration.getDelegationsTtlRetainDuration())
        DelegationTable.clientId.addTo(itemMap, clientId)
        DelegationTable.redirectUri.addToNullable(itemMap, redirectUri)
        DelegationTable.authorizationCodeHash.addToNullable(itemMap, authorizationCodeHash)
        DelegationTable.scope.addTo(itemMap, scope)

        DelegationTable.mtlsClientCertificate.addToNullable(itemMap, mtlsClientCertificate)
        DelegationTable.mtlsClientCertificateDN.addToNullable(itemMap, mtlsClientCertificateDN)
        DelegationTable.mtlsClientCertificateX5TS256.addToNullable(itemMap, mtlsClientCertificateX5TS256)

        DelegationTable.authenticationAttributes.addTo(itemMap, _jsonHandler.toJson(authenticationAttributes.asMap()))
        DelegationTable.consentResult.addToNullable(itemMap, consentResult?.asMap()?.let { _jsonHandler.toJson(it) })
        DelegationTable.claimMap.addTo(itemMap, _jsonHandler.toJson(claimMap))
        DelegationTable.customClaimValues.addTo(itemMap, _jsonHandler.toJson(customClaimValues))
        DelegationTable.claims.addTo(itemMap, _jsonHandler.toJson(claims))

        _tenantId.tenantId?.let {
            DelegationTable.tenantId.addTo(itemMap, _tenantId.tenantId)
        }

        return itemMap
    }

    /*
     * Deserializes an item into a Delegation implementation
     */
    private fun Map<String, AttributeValue>.toDelegation() = DynamoDBDelegation(
        version = DelegationTable.version.from(this),
        id = DelegationTable.id.from(this),
        status = DelegationTable.status.from(this),
        owner = DelegationTable.owner.from(this),
        created = DelegationTable.created.from(this),
        expires = DelegationTable.expires.from(this),
        clientId = DelegationTable.clientId.from(this),
        redirectUri = DelegationTable.redirectUri.optionalFrom(this),
        authorizationCodeHash = DelegationTable.authorizationCodeHash.optionalFrom(this),
        authenticationAttributes = DelegationTable.authenticationAttributes.from(this).let {
            AuthenticationAttributes.fromAttributes(
                Attributes.fromMap(
                    _jsonHandler.fromJson(it)
                )
            )
        },
        consentResult = DelegationTable.consentResult.optionalFrom(this)?.let {
            DelegationConsentResult.fromMap(
                _jsonHandler.fromJson(it)
            )
        },
        scope = DelegationTable.scope.from(this),
        claimMap = DelegationTable.claimMap.from(this).let { _jsonHandler.fromJson(it) },
        customClaimValues = DelegationTable.customClaimValues.from(this).let { _jsonHandler.fromJson(it) },
        claims = DelegationTable.claims.from(this).let { _jsonHandler.fromJson(it) },
        mtlsClientCertificate = DelegationTable.mtlsClientCertificate.optionalFrom(this),
        mtlsClientCertificateDN = DelegationTable.mtlsClientCertificateDN.optionalFrom(this),
        mtlsClientCertificateX5TS256 = DelegationTable.mtlsClientCertificateX5TS256.optionalFrom(this)
    )

    override fun getById(id: String): Delegation? {
        val request = GetItemRequest.builder()
            .tableName(DelegationTable.name(_configuration))
            .key(mapOf(DelegationTable.id.toNameValuePair(id)))
            .consistentRead(true)
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }
        val item = response.item()

        // Only valid (i.e. status == issue) delegations are retrieved here
        // to mimic the JDBC DAP behavior.
        val status = DelegationTable.status.from(item)
        if (status != DelegationStatus.issued) {
            return null
        }
        return item.toDelegation()
    }

    override fun getByAuthorizationCodeHash(authorizationCodeHash: String): Delegation? {
        val index = DelegationTable.authorizationCodeIndex
        val request = QueryRequest.builder()
            .tableName(DelegationTable.name(_configuration))
            .indexName(index.name)
            .keyConditionExpression(index.expression)
            .expressionAttributeValues(index.expressionValueMap(authorizationCodeHash))
            .expressionAttributeNames(index.expressionNameMap)
            .build()

        val response = _dynamoDBClient.query(request)

        if (!response.hasItems() || response.items().isEmpty()) {
            return null
        }

        // If multiple entries exist, we use the first one
        return response.items().first().toDelegation()
    }

    override fun create(delegation: Delegation) {
        val request = PutItemRequest.builder()
            .tableName(DelegationTable.name(_configuration))
            .item(delegation.toItem())
            .build()

        _dynamoDBClient.putItem(request)
    }

    override fun setStatus(id: String, newStatus: DelegationStatus): Long {
        val request = UpdateItemRequest.builder()
            .tableName(DelegationTable.name(_configuration))
            .key(mapOf(DelegationTable.id.toNameValuePair(id)))
            .conditionExpression(updateConditionExpression)
            .updateExpression("SET ${DelegationTable.status.hashName} = ${DelegationTable.status.colonName}")
            .expressionAttributeValues(mapOf(DelegationTable.status.toExpressionNameValuePair(newStatus)))
            .expressionAttributeNames(mapOf(DelegationTable.status.toNamePair()))
            .build()

        try {
            _dynamoDBClient.updateItem(request)
        } catch (_: ConditionalCheckFailedException) {
            // this exception means the entry does not exist
            return 0
        }
        return 1
    }

    override fun getByOwner(owner: String, startIndex: Long, count: Long): Collection<Delegation> =
        getByOwnerAndMaybeClient(owner, null, DelegationStatus.issued, startIndex, count)

    private fun getByOwnerAndMaybeClient(
        owner: String,
        client: String?,
        delegationStatus: DelegationStatus,
        startIndex: Long,
        count: Long
    ): Collection<Delegation> {
        val validatedStartIndex = startIndex.toIntOrThrow("startIndex")
        val validatedCount = count.toIntOrThrow("count")
        val index = DelegationTable.ownerStatusIndex
        val filterExpression = if (client != null) {
            "${index.filterExpression(_tenantId)} and ${DelegationTable.clientId.hashName} = ${DelegationTable.clientId.colonName}"
        } else {
            index.filterExpression(_tenantId)
        }
        val expressionValues = if (client != null) {
            index.expressionValueMap(_tenantId, owner, delegationStatus) +
                    DelegationTable.clientId.toExpressionNameValuePair(client)
        } else {
            index.expressionValueMap(_tenantId, owner, delegationStatus)
        }
        val expressionNames = if (client != null) {
            index.expressionNameMap + DelegationTable.clientId.toNamePair()
        } else {
            index.expressionNameMap
        }

        val request = QueryRequest.builder()
            .tableName(DelegationTable.name(_configuration))
            .indexName(index.name)
            .keyConditionExpression(index.keyConditionExpression)
            .filterExpression(filterExpression)
            .expressionAttributeValues(expressionValues)
            .expressionAttributeNames(expressionNames)
            .limit(validatedCount)
            .build()

        return querySequence(request, _dynamoDBClient)
            .drop(validatedStartIndex)
            .map { it.toDelegation() }
            .take(validatedCount)
            .toList()
    }

    override fun getCountByOwner(owner: String): Long {
        val index = DelegationTable.ownerStatusIndex
        val request = QueryRequest.builder()
            .tableName(DelegationTable.name(_configuration))
            .indexName(index.name)
            .keyConditionExpression(index.keyConditionExpression)
            .expressionAttributeValues(index.expressionValueMap(_tenantId, owner, DelegationStatus.issued))
            .filterExpression(index.filterExpression(_tenantId))
            .expressionAttributeNames(index.expressionNameMap)
            .select(Select.COUNT)
            .build()

        return count(request, _dynamoDBClient)
    }

    override fun getAllActive(startIndex: Long, count: Long): Collection<Delegation> {
        val validatedStartIndex = startIndex.toIntOrThrow("startIndex")
        val validatedCount = count.toIntOrThrow("count")
        val request = createAllActiveScanRequestBuilder().build()

        return scanSequence(request, _dynamoDBClient)
            .drop(validatedStartIndex)
            .map { it.toDelegation() }
            .take(validatedCount)
            .toList()
    }

    override fun getCountAllActive(): Long {
        val request = createAllActiveScanRequestBuilder()
            .select(Select.COUNT)
            .build()

        return count(request, _dynamoDBClient)
    }

    private fun createAllActiveScanRequestBuilder(): ScanRequest.Builder {
        val expressionValues = ImmutableMap.Builder<String, AttributeValue>()
            .put(issuedStatusExpressionAttribute.first, issuedStatusExpressionAttribute.second)
        val tenantIdExpression: String
        if (_tenantId.tenantId != null) {
            tenantIdExpression = "${DelegationTable.tenantId.hashName} = ${DelegationTable.tenantId.colonName}"
            expressionValues.put(
                DelegationTable.tenantId.colonName,
                DelegationTable.tenantId.toAttrValue(_tenantId.tenantId)
            )
        } else {
            tenantIdExpression = "attribute_not_exists(${DelegationTable.tenantId.hashName})"
        }

        return ScanRequest.builder()
            .tableName(DelegationTable.name(_configuration))
            .filterExpression(
                "${DelegationTable.status.hashName} = ${DelegationTable.status.colonName} " +
                        "AND $tenantIdExpression"
            )
            .expressionAttributeValues(expressionValues.build())
            .expressionAttributeNames(mapOf(DelegationTable.status.toNamePair(), DelegationTable.tenantId.toNamePair()))
    }

    override fun getAll(resourceQuery: ResourceQuery): Collection<DynamoDBDelegation> = try {
        val comparator = getComparatorFor(resourceQuery)

        val queryPlan = if (resourceQuery.filter != null) {
            QueryPlanner(DelegationTable.queryCapabilities, _tenantId).build(resourceQuery.filter)
        } else {
            QueryPlan.UsingScan.fullScan()
        }

        val values = when (queryPlan) {
            is QueryPlan.UsingQueries -> query(queryPlan)
            is QueryPlan.UsingScan -> scan(queryPlan)
        }

        val sortedValues = if (comparator != null) {
            values.sortedWith(
                if (resourceQuery.sorting.sortOrder == ResourceQuery.Sorting.SortOrder.ASCENDING) {
                    comparator
                } else {
                    comparator.reversed()
                }
            )
        } else {
            values
        }

        val validatedStartIndex = resourceQuery.pagination.startIndex.toIntOrThrow("pagination.startIndex")
        val validatedCount = resourceQuery.pagination.count.toIntOrThrow("pagination.count")

        sortedValues
            .drop(validatedStartIndex)
            .take(validatedCount)
            .map { it.toDelegation() }
            .toList()
    } catch (e: UnsupportedQueryException) {
        _logger.debug("Unable to process query. Reason is '{}'", e.message)
        _logger.debug(MASK_MARKER, "The query that failed = '{}", resourceQuery)
        throw _configuration.getExceptionFactory().externalServiceException(e.message)
    }

    override fun setStatusByOwner(owner: String, status: DelegationStatus): SetStatusResult {
        _logger.trace("setStatusByOwner called: new status={}", status)
        // We try to get one more than the maximum
        val statusOfDelegationsToChange = if (status == DelegationStatus.revoked) {
            DelegationStatus.issued
        } else {
            DelegationStatus.revoked
        }
        val delegations = getByOwnerAndMaybeClient(
            owner, null,
            statusOfDelegationsToChange, 0, MAX_UPDATABLE_DELEGATIONS + 1
        ).toTypedArray()
        if (delegations.size > MAX_UPDATABLE_DELEGATIONS) {
            _logger.debug(
                MASK_MARKER,
                "Unable to setStatusByOwner for '{}' because delegation counts exceeds maximum '{}'",
                owner, MAX_UPDATABLE_DELEGATIONS
            )
            return SetStatusResult.TooManyDelegations.INSTANCE
        }
        for (startIndex in delegations.indices step DELEGATION_UPDATE_TRANSACTION_SIZE) {
            updateDelegations(delegations, status, startIndex, DELEGATION_UPDATE_TRANSACTION_SIZE)
        }
        return SetStatusResult.Success(delegations.size.toLong())
    }

    override fun setStatusByOwnerAndClient(owner: String, clientId: String, status: DelegationStatus): SetStatusResult {
        _logger.trace("setStatusByOwnerAndClient called: new status={}", status)
        val statusOfDelegationsToChange = if (status == DelegationStatus.revoked) {
            DelegationStatus.issued
        } else {
            DelegationStatus.revoked
        }
        // We try to get one more than the maximum
        val delegations = getByOwnerAndMaybeClient(
            owner, clientId,
            statusOfDelegationsToChange, 0, MAX_UPDATABLE_DELEGATIONS + 1
        ).toTypedArray()
        if (delegations.size > MAX_UPDATABLE_DELEGATIONS) {
            _logger.debug(
                MASK_MARKER,
                "Unable to setStatusByOwnerAndClient for owner '{}' and client '{}' " +
                        "because delegation counts exceeds maximum '{}'",
                owner, clientId, MAX_UPDATABLE_DELEGATIONS
            )
            return SetStatusResult.TooManyDelegations.INSTANCE
        }
        for (startIndex in delegations.indices step DELEGATION_UPDATE_TRANSACTION_SIZE) {
            _logger.debug("Updating delegations, startIndex={}", startIndex)
            updateDelegations(delegations, status, startIndex, DELEGATION_UPDATE_TRANSACTION_SIZE)
        }
        return SetStatusResult.Success(delegations.size.toLong())
    }

    private fun updateDelegations(
        delegationsToUpdate: Array<Delegation>,
        status: DelegationStatus,
        startIndex: Int,
        delegationUpdateTransactionSize: Int
    ) {
        val statusAttribute = DelegationTable.status
        val lastIndexPlusOne = min(
            startIndex + delegationUpdateTransactionSize,
            delegationsToUpdate.size
        )
        val transactionItems = delegationsToUpdate.slice(
            startIndex until lastIndexPlusOne
        ).map { delegationToUpdate ->
            TransactWriteItem.builder()
                .update {
                    it.tableName(DelegationTable.name(_configuration))
                    it.key(mapOf(DelegationTable.id.toNameValuePair(delegationToUpdate.id)))
                    it.updateExpression("SET ${statusAttribute.hashName} = ${statusAttribute.colonName}")
                    it.expressionAttributeNames(mapOf(statusAttribute.toNamePair()))
                    it.expressionAttributeValues(mapOf(statusAttribute.toExpressionNameValuePair(status)))
                }
                .build()
        }
        val request = TransactWriteItemsRequest.builder()
            .transactItems(transactionItems)
            .build()
        try {
            _dynamoDBClient.transactionWriteItems(request)
        } catch (e: Exception) {
            _logger.debug("Error while updating delegations with startIndex: {} - {}", startIndex, e)
            if (startIndex != 0) {
                _logger.info(
                    "Error while updating delegation batch other than the first ({}), " +
                            "resulting in a partial delegation update",
                    startIndex
                )
            }
            throw e
        }
    }

    private fun query(queryPlan: QueryPlan.UsingQueries): Sequence<DynamoDBItem> {
        val nOfQueries = queryPlan.queries.entries.size
        if (nOfQueries > MAX_QUERIES) {
            throw UnsupportedQueryException.QueryRequiresTooManyOperations(nOfQueries, MAX_QUERIES)
        }
        if (nOfQueries == 1) {
            // Special case where the plan as a single query,
            // which means we can use streaming and don't need to buffer the multiple queries in the
            // temporary result
            return queryWithASinglePlannedQuery(queryPlan)
        }
        val result = linkedMapOf<String, Map<String, AttributeValue>>()
        val tenantIdProduct = getTenantIdProduct()
        queryPlan.queries.forEach { query ->
            val products = query.value.map { product -> and(product, tenantIdProduct) }
            val dynamoDBQuery = DynamoDBQueryBuilder.buildQuery(query.key, products)

            val queryRequest = QueryRequest.builder()
                .tableName(DelegationTable.name(_configuration))
                .configureWith(dynamoDBQuery)
                .build()

            querySequence(queryRequest, _dynamoDBClient)
                .filterWith(products)
                .forEach {
                    result[DelegationTable.id.from(it)] = it
                }
        }
        return result.values.asSequence()
    }

    private fun queryWithASinglePlannedQuery(queryPlan: QueryPlan.UsingQueries): Sequence<DynamoDBItem> {
        val (keyCondition, queryProducts) = queryPlan.queries.entries.singleOrNull()
            ?: throw IllegalArgumentException("queryPlan must have a single query")
        val tenantIdProduct = getTenantIdProduct()
        val products = queryProducts.map { product -> and(product, tenantIdProduct) }
        val dynamoDBQuery = DynamoDBQueryBuilder.buildQuery(keyCondition, products)

        val queryRequest = QueryRequest.builder()
            .tableName(DelegationTable.name(_configuration))
            .configureWith(dynamoDBQuery)
            .build()

        return querySequence(queryRequest, _dynamoDBClient)
            .filterWith(products)
    }

    private fun scan(queryPlan: QueryPlan.UsingScan): Sequence<DynamoDBItem> {
        val scanRequestBuilder = ScanRequest.builder()
            .tableName(DelegationTable.name(_configuration))
        val queryExpression = and(getTenantIdProduct(), queryPlan.expression)
        val dynamoDBScan = DynamoDBQueryBuilder.buildScan(queryExpression)
        scanRequestBuilder.configureWith(dynamoDBScan)
        return scanSequence(scanRequestBuilder.build(), _dynamoDBClient)
            .filterWith(queryExpression.products)
    }

    private fun getTenantIdProduct(): Product {
        val tenantExpression = if (_tenantId.tenantId != null) {
            BinaryAttributeExpression(DelegationTable.tenantId, BinaryAttributeOperator.Eq, _tenantId.tenantId)
        } else {
            UnaryAttributeExpression(DelegationTable.tenantId, UnaryAttributeOperator.NotPr)
        }
        return Product(setOf(tenantExpression))
    }

    private fun getComparatorFor(resourceQuery: ResourceQuery): Comparator<Map<String, AttributeValue>>? {
        return if (resourceQuery.sorting != null && resourceQuery.sorting.sortBy != null) {
            DelegationTable.queryCapabilities.attributeMap[resourceQuery.sorting.sortBy]
                ?.let { attribute ->
                    attribute.comparator()
                        ?: throw UnsupportedQueryException.UnsupportedSortAttribute(resourceQuery.sorting.sortBy)
                }
                ?: throw UnsupportedQueryException.UnknownSortAttribute(resourceQuery.sorting.sortBy)
        } else {
            null
        }
    }

    companion object {
        private val _logger = LoggerFactory.getLogger(DynamoDBDelegationDataAccessProvider::class.java)
        private val MASK_MARKER: Marker = MarkerFactory.getMarker("MASK")
        private val issuedStatusExpressionAttribute =
            DelegationTable.status.toExpressionNameValuePair(DelegationStatus.issued)
        private val updateConditionExpression = "attribute_exists(${DelegationTable.id})"

        private const val MAX_QUERIES = 8

        private const val MAX_UPDATABLE_DELEGATIONS = 200L
        private const val DELEGATION_UPDATE_TRANSACTION_SIZE = 100

        /**
         * Adds extra attributes obtained from a map, into the delegation's item attribute map.
         *
         * @param pathsToMap The list with the paths ('.' separated strings) for the map entries that should be mapped.
         * @param prefix The prefix that should be added when building the attribute name.
         * @param sourceMap map from which to retrieve the additional attribute values.
         * @param attributeValueMap the mutable attribute value map where to add the new attributes.
         */
        private fun addExtraAttributesToItemMap(
            pathsToMap: List<String>,
            prefix: String,
            sourceMap: Map<String, Any>,
            attributeValueMap: MutableMap<String, AttributeValue>
        ) {
            pathsToMap.forEach { path ->
                val pathSegments = path.split('.')
                val attributeValue = sourceMap.getMapValueForPath(pathSegments)
                    ?.let { mapToAttributeValue(it) }
                if (attributeValue != null) {
                    attributeValueMap[attributeNameFor(prefix, pathSegments)] = attributeValue
                }
            }
        }

        /**
         * Computes the attribute name by adding a prefix and joining the path with '_'
         * E.g. an additional attributes obtained from [Delegation.getClaims] with the claim path
         * "name1.name2.name3" will have the name "claims_name1_name2_name3".
         *
         * @param prefix the prefix
         * @param path the list with the path segments
         */
        private fun attributeNameFor(prefix: String, path: List<String>): String =
            "${prefix}_${path.joinToString("_")}"
    }
}

