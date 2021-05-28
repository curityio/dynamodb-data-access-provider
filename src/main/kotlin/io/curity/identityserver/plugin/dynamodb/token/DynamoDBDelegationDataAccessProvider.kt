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
import io.curity.identityserver.plugin.dynamodb.PartitionOnlyIndex
import io.curity.identityserver.plugin.dynamodb.PrimaryKey
import io.curity.identityserver.plugin.dynamodb.StringAttribute
import io.curity.identityserver.plugin.dynamodb.Table
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import io.curity.identityserver.plugin.dynamodb.configureWith
import io.curity.identityserver.plugin.dynamodb.count
import io.curity.identityserver.plugin.dynamodb.query.DynamoDBQueryBuilder
import io.curity.identityserver.plugin.dynamodb.query.Index
import io.curity.identityserver.plugin.dynamodb.query.QueryPlan
import io.curity.identityserver.plugin.dynamodb.query.QueryPlanner
import io.curity.identityserver.plugin.dynamodb.query.TableQueryCapabilities
import io.curity.identityserver.plugin.dynamodb.query.UnsupportedQueryException
import io.curity.identityserver.plugin.dynamodb.query.filterWith
import io.curity.identityserver.plugin.dynamodb.querySequence
import io.curity.identityserver.plugin.dynamodb.scanSequence
import io.curity.identityserver.plugin.dynamodb.toIntOrThrow
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.attribute.AccountAttributes
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.AuthenticationAttributes
import se.curity.identityserver.sdk.data.authorization.Delegation
import se.curity.identityserver.sdk.data.authorization.DelegationConsentResult
import se.curity.identityserver.sdk.data.authorization.DelegationStatus
import se.curity.identityserver.sdk.data.query.ResourceQuery
import se.curity.identityserver.sdk.datasource.DelegationDataAccessProvider
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.Select
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

class DynamoDBDelegationDataAccessProvider(
    private val _dynamoDBClient: DynamoDBClient,
    private val _configuration: DynamoDBDataAccessProviderConfiguration
) : DelegationDataAccessProvider
{
    private val _jsonHandler = _configuration.getJsonHandler()

    /*
     * There is one attribute for each property in the Delegation interface:
     * - There is an extra version attribute in the table that doesn't exist in the Delegation interface
     * - The "scopeClaims" property is not stored, since it is deprecated and this DAP doesn't need to support legacy
     * delegation formats.
     */
    object DelegationTable : Table("curity-delegations")
    {
        val version = StringAttribute("version")
        val id = StringAttribute("id")
        val status = EnumAttribute.of<DelegationStatus>("status")
        val owner = StringAttribute("owner")

        val created = NumberLongAttribute("created")
        val expires = NumberLongAttribute("expires")

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

        val primaryKey = PrimaryKey(id)
        val ownerStatusIndex = PartitionAndSortIndex("owner-status-index", owner, status)
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
                "externalId" to id
            )
        )
    }

    /*
     * Serializes a Delegation into an Item
     */
    private fun Delegation.toItem(): DynamoDBItem
    {
        val res = mutableMapOf<String, AttributeValue>()
        DelegationTable.version.addTo(res, "6.2")
        DelegationTable.id.addTo(res, id)
        DelegationTable.status.addTo(res, status)
        DelegationTable.owner.addTo(res, owner)
        DelegationTable.created.addTo(res, created)
        DelegationTable.expires.addTo(res, expires)
        DelegationTable.clientId.addTo(res, clientId)
        DelegationTable.redirectUri.addToNullable(res, redirectUri)
        DelegationTable.authorizationCodeHash.addToNullable(res, authorizationCodeHash)
        DelegationTable.scope.addTo(res, scope)

        DelegationTable.mtlsClientCertificate.addToNullable(res, mtlsClientCertificate)
        DelegationTable.mtlsClientCertificateDN.addToNullable(res, mtlsClientCertificateDN)
        DelegationTable.mtlsClientCertificateX5TS256.addToNullable(res, mtlsClientCertificateX5TS256)

        DelegationTable.authenticationAttributes.addTo(res, _jsonHandler.toJson(authenticationAttributes.asMap()))
        DelegationTable.consentResult.addToNullable(res, consentResult?.asMap()?.let { _jsonHandler.toJson(it) })
        DelegationTable.claimMap.addTo(res, _jsonHandler.toJson(claimMap))
        DelegationTable.customClaimValues.addTo(res, _jsonHandler.toJson(customClaimValues))
        DelegationTable.claims.addTo(res, _jsonHandler.toJson(claims))
        return res
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
        authorizationCodeHash = DelegationTable.redirectUri.optionalFrom(this),
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

    override fun getById(id: String): Delegation?
    {
        val request = GetItemRequest.builder()
            .tableName(DelegationTable.name)
            .key(mapOf(DelegationTable.id.toNameValuePair(id)))
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty())
        {
            return null
        }
        val item = response.item()

        // Only valid (i.e. status == issue) delegations are retrieved here
        // to mimic the JDBC DAP behavior.
        val status = DelegationTable.status.from(item)
        if (status != DelegationStatus.issued)
        {
            return null
        }
        return item.toDelegation()
    }

    override fun getByAuthorizationCodeHash(authorizationCodeHash: String): Delegation?
    {
        val index = DelegationTable.authorizationCodeIndex
        val request = QueryRequest.builder()
            .tableName(DelegationTable.name)
            .indexName(index.name)
            .keyConditionExpression(index.expression)
            .expressionAttributeValues(index.expressionValueMap(authorizationCodeHash))
            .expressionAttributeNames(index.expressionNameMap)
            .build()

        val response = _dynamoDBClient.query(request)

        if (!response.hasItems() || response.items().isEmpty())
        {
            return null
        }

        // If multiple entries exist, we use the first one
        return response.items().first().toDelegation()
    }

    override fun create(delegation: Delegation)
    {
        val request = PutItemRequest.builder()
            .tableName(DelegationTable.name)
            .item(delegation.toItem())
            .build()

        _dynamoDBClient.putItem(request)
    }

    override fun setStatus(id: String, newStatus: DelegationStatus): Long
    {
        val request = UpdateItemRequest.builder()
            .tableName(DelegationTable.name)
            .key(mapOf(DelegationTable.id.toNameValuePair(id)))
            .updateExpression("SET ${DelegationTable.status.hashName} = ${DelegationTable.status.colonName}")
            .expressionAttributeValues(mapOf(DelegationTable.status.toExpressionNameValuePair(newStatus)))
            .expressionAttributeNames(mapOf(DelegationTable.status.toNamePair()))
            .returnValues(ReturnValue.UPDATED_NEW)
            .build()

        val response = _dynamoDBClient.updateItem(request)

        return if (response.hasAttributes() && response.attributes().isNotEmpty())
        {
            1
        } else
        {
            0
        }
    }

    override fun getByOwner(owner: String, startIndex: Long, count: Long): Collection<Delegation>
    {
        val validatedStartIndex = startIndex.toIntOrThrow("startIndex")
        val validatedCount = count.toIntOrThrow("count")
        val index = DelegationTable.ownerStatusIndex
        val request = QueryRequest.builder()
            .tableName(DelegationTable.name)
            .indexName(index.name)
            .keyConditionExpression(index.keyConditionExpression)
            .expressionAttributeValues(
                index.expressionValueMap(owner, DelegationStatus.issued)
            )
            .expressionAttributeNames(index.expressionNameMap)
            .limit(validatedCount)
            .build()

        return querySequence(request, _dynamoDBClient)
            .drop(validatedStartIndex)
            .map { it.toDelegation() }
            .take(validatedCount)
            .toList()
    }

    override fun getCountByOwner(owner: String): Long
    {
        val index = DelegationTable.ownerStatusIndex
        val request = QueryRequest.builder()
            .tableName(DelegationTable.name)
            .indexName(index.name)
            .keyConditionExpression(index.keyConditionExpression)
            .expressionAttributeValues(
                index.expressionValueMap(owner, DelegationStatus.issued)
            )
            .expressionAttributeNames(index.expressionNameMap)
            .select(Select.COUNT)
            .build()

        return count(request, _dynamoDBClient)
    }

    override fun getAllActive(startIndex: Long, count: Long): Collection<Delegation>
    {
        val validatedStartIndex = startIndex.toIntOrThrow("startIndex")
        val validatedCount = count.toIntOrThrow("count")

        val request = ScanRequest.builder()
            .tableName(DelegationTable.name)
            .filterExpression("${DelegationTable.status.hashName} = ${DelegationTable.status.colonName}")
            .expressionAttributeValues(issuedStatusExpressionAttributeMap)
            .expressionAttributeNames(issuedStatusExpressionAttributeNameMap)
            .build()

        return scanSequence(request, _dynamoDBClient)
            .drop(validatedStartIndex)
            .map { it.toDelegation() }
            .take(validatedCount)
            .toList()
    }

    override fun getCountAllActive(): Long
    {
        val request = ScanRequest.builder()
            .tableName(DelegationTable.name)
            .filterExpression("${DelegationTable.status.hashName} = ${DelegationTable.status.colonName}")
            .expressionAttributeValues(issuedStatusExpressionAttributeMap)
            .expressionAttributeNames(issuedStatusExpressionAttributeNameMap)
            .select(Select.COUNT)
            .build()

        return count(request, _dynamoDBClient)
    }

    override fun getAll(resourceQuery: ResourceQuery): Collection<DynamoDBDelegation> = try
    {
        val comparator = getComparatorFor(resourceQuery)

        val queryPlan = if (resourceQuery.filter != null)
        {
            QueryPlanner(DelegationTable.queryCapabilities).build(resourceQuery.filter)
        } else
        {
            QueryPlan.UsingScan.fullScan()
        }

        val values = when (queryPlan)
        {
            is QueryPlan.UsingQueries -> query(queryPlan)
            is QueryPlan.UsingScan -> scan(queryPlan)
        }

        val sortedValues = if (comparator != null)
        {
            values.sortedWith(
                if (resourceQuery.sorting.sortOrder == ResourceQuery.Sorting.SortOrder.ASCENDING)
                {
                    comparator
                } else
                {
                    comparator.reversed()
                }
            )
        } else
        {
            values
        }

        val validatedStartIndex = resourceQuery.pagination.startIndex.toIntOrThrow("pagination.startIndex")
        val validatedCount = resourceQuery.pagination.count.toIntOrThrow("pagination.count")

        sortedValues
            .drop(validatedStartIndex)
            .take(validatedCount)
            .map { it.toDelegation() }
            .toList()
    } catch (e: UnsupportedQueryException)
    {
        _logger.debug("Unable to process query. Reason is '{}', query = '{}", e.message, resourceQuery)
        throw _configuration.getExceptionFactory().externalServiceException(e.message)
    }

    private fun query(queryPlan: QueryPlan.UsingQueries): Sequence<DynamoDBItem>
    {
        val nOfQueries = queryPlan.queries.entries.size
        if (nOfQueries > MAX_QUERIES)
        {
            throw UnsupportedQueryException.QueryRequiresTooManyOperations(nOfQueries, MAX_QUERIES)
        }
        val result = linkedMapOf<String, Map<String, AttributeValue>>()
        queryPlan.queries.forEach { query ->
            val dynamoDBQuery = DynamoDBQueryBuilder.buildQuery(query.key, query.value)

            val queryRequest = QueryRequest.builder()
                .tableName(DelegationTable.name)
                .configureWith(dynamoDBQuery)
                .build()

            querySequence(queryRequest, _dynamoDBClient)
                .filterWith(query.value)
                .forEach {
                    result[DelegationTable.id.from(it)] = it
                }
        }
        return result.values.asSequence()
    }

    private fun scan(queryPlan: QueryPlan.UsingScan): Sequence<DynamoDBItem>
    {
        val scanRequestBuilder = ScanRequest.builder()
            .tableName(DelegationTable.name)

        val dynamoDBScan = DynamoDBQueryBuilder.buildScan(queryPlan.expression)
        scanRequestBuilder.configureWith(dynamoDBScan)
        return scanSequence(scanRequestBuilder.build(), _dynamoDBClient)
            .filterWith(queryPlan.expression.products)
    }

    private fun getComparatorFor(resourceQuery: ResourceQuery): Comparator<Map<String, AttributeValue>>?
    {
        return if (resourceQuery.sorting != null && resourceQuery.sorting.sortBy != null)
        {
            DelegationTable.queryCapabilities.attributeMap[resourceQuery.sorting.sortBy]
                ?.let { attribute ->
                    attribute.comparator()
                        ?: throw UnsupportedQueryException.UnsupportedSortAttribute(resourceQuery.sorting.sortBy)
                }
                ?: throw UnsupportedQueryException.UnknownSortAttribute(resourceQuery.sorting.sortBy)
        } else
        {
            null
        }
    }

    companion object
    {
        private val _logger = LoggerFactory.getLogger(DynamoDBDelegationDataAccessProvider::class.java)
        private val issuedStatusExpressionAttribute =
            DelegationTable.status.toExpressionNameValuePair(DelegationStatus.issued)
        private val issuedStatusExpressionAttributeMap = mapOf(issuedStatusExpressionAttribute)
        private val issuedStatusExpressionAttributeName = DelegationTable.status.toNamePair()
        private val issuedStatusExpressionAttributeNameMap = mapOf(issuedStatusExpressionAttributeName)

        private const val MAX_QUERIES = 8
    }
}

