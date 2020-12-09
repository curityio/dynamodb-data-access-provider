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
package io.curity.dynamoDBDataAccessProvider.token

import io.curity.dynamoDBDataAccessProvider.DynamoDBClient
import io.curity.dynamoDBDataAccessProvider.configuration.DynamoDBDataAccessProviderDataAccessProviderConfig
import io.curity.dynamoDBDataAccessProvider.toAttributeValue
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.data.authorization.Delegation
import se.curity.identityserver.sdk.data.authorization.DelegationStatus
import se.curity.identityserver.sdk.data.query.Filter
import se.curity.identityserver.sdk.data.query.ResourceQuery
import se.curity.identityserver.sdk.datasource.DelegationDataAccessProvider
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.QueryResponse
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.util.EnumMap
import se.curity.identityserver.sdk.data.query.Filter.AttributeOperator.*

class DynamoDBDelegationDataAccessProvider(private val dynamoDBClient: DynamoDBClient, configuration: DynamoDBDataAccessProviderDataAccessProviderConfig): DelegationDataAccessProvider
{
    private val jsonHandler = configuration.getJsonHandler()

    override fun getById(id: String): Delegation?
    {
        val request = GetItemRequest.builder()
                .tableName(tableName)
                .key(getKey(id))
                .build()

        val response = dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        return toDelegation(response.item())
    }

    override fun getByAuthorizationCodeHash(authorizationCodeHash: String): Delegation?
    {
        val request = QueryRequest.builder()
                .tableName(tableName)
                .indexName("authorization-hash-index")
                .keyConditionExpression("authorizationCodeHash = :hash")
                .expressionAttributeValues(mapOf(Pair(":hash", AttributeValue.builder().s(authorizationCodeHash).build())))
                .build()

        val response = dynamoDBClient.query(request)

        if (!response.hasItems() || response.items().isEmpty()) {
            return null
        }

        return toDelegation(response.items().first())
    }

    override fun create(delegation: Delegation)
    {
        val request = PutItemRequest.builder()
                .tableName(tableName)
                .item(delegation.toItem())
                .build()

        dynamoDBClient.putItem(request)
    }

    override fun setStatus(id: String, newStatus: DelegationStatus): Long
    {
        val request = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(getKey(id))
                .updateExpression("SET #status = :status")
                .expressionAttributeValues(mapOf(Pair(":status", AttributeValue.builder().s(newStatus.name).build())))
                .expressionAttributeNames(mapOf(Pair("#status", "status")))
                .build()

        val response = dynamoDBClient.updateItem(request)

        return if (response.sdkHttpResponse().isSuccessful) {
            1
        } else {
            0
        }
    }

    override fun getByOwner(owner: String, startIndex: Long, count: Long): MutableCollection<out Delegation>
    {
        //TODO implement proper paging and possible DynamoDB paging

        val request = QueryRequest.builder()
                .tableName(tableName)
                .indexName("owner-index")
                .keyConditionExpression("owner = :owner AND #status = issued")
                .expressionAttributeValues(mapOf(Pair(":owner", AttributeValue.builder().s(owner).build())))
                .expressionAttributeNames(mapOf(Pair("#status", "status")))
                .limit(count.toInt())
                .build()

        val response = dynamoDBClient.query(request)

        if (!response.hasItems() || response.items().isEmpty()) {
            return mutableListOf()
        }

        val result = mutableListOf<Delegation>()
        response.items().forEach {delegationItem -> result.add(toDelegation(delegationItem))}

        return result
    }

    override fun getCountByOwner(owner: String): Long
    {
        var count = 0L

        var response: QueryResponse? = null
        do {
            response = queryForCount(owner, response?.lastEvaluatedKey())
            count += response.count()
        } while (response?.hasLastEvaluatedKey() == true)

        return count
    }

    private fun queryForCount(owner: String, exclusiveStartKey: Map<String, AttributeValue>?): QueryResponse
    {
        val requestBuilder = QueryRequest.builder()
                .tableName(tableName)
                .indexName("owner-index")
                .keyConditionExpression("owner = :owner")
                .expressionAttributeValues(mapOf(Pair(":owner", AttributeValue.builder().s(owner).build())))
                .projectionExpression("id")

        if (exclusiveStartKey != null) {
            requestBuilder.exclusiveStartKey(exclusiveStartKey)
        }

        return dynamoDBClient.query(requestBuilder.build())
    }

    override fun getAllActive(startIndex: Long, count: Long): MutableCollection<out Delegation>
    {
        // TODO implement proper paging and possible DynamoDB paging

        val request = ScanRequest.builder()
                .tableName(tableName)
                .filterExpression("#status = :status")
                .expressionAttributeValues(mapOf(Pair(":status", AttributeValue.builder().s("issued").build())))
                .expressionAttributeNames(mapOf(Pair("#status", "status")))
                .build()

        val response = dynamoDBClient.scan(request)

        if (!response.hasItems() || response.items().isEmpty()) {
            return mutableListOf()
        }

        val resultList = mutableListOf<Delegation>()
        response.items().forEach { item -> resultList.add(toDelegation(item)) }

        return resultList
    }

    override fun getCountAllActive(): Long
    {
        var count = 0L
        var response: ScanResponse? = null

        do {
            response = queryForActiveCount(response?.lastEvaluatedKey())
            count += response.count()
        } while (response?.hasLastEvaluatedKey() == true)

        return count
    }

    private fun queryForActiveCount(exclusiveStartKey: Map<String, AttributeValue>?): ScanResponse {
        val requestBuilder = ScanRequest.builder()
                .tableName(tableName)
                .filterExpression("status = :status")
                .expressionAttributeValues(mapOf(Pair(":status", AttributeValue.builder().s("issued").build())))
                .expressionAttributeNames(mapOf(Pair("#status", "status")))
                .projectionExpression("id")

        if (exclusiveStartKey != null) {
            requestBuilder.exclusiveStartKey(exclusiveStartKey)
        }

        return dynamoDBClient.scan(requestBuilder.build())
    }

    override fun getAll(query: ResourceQuery): MutableCollection<out Delegation>
    {
        // TODO implement sorting - can be tricky
        // TODO implement pagination and possible DynamoDB pagination

        val requestBuilder = ScanRequest.builder()
                .tableName(tableName)

        if (!query.attributesEnumeration.isNeutral) {
            val attributesEnumeration = query.attributesEnumeration

            if (attributesEnumeration is ResourceQuery.Inclusions) {
                requestBuilder.attributesToGet(attributesEnumeration.attributes)
            } else {
                // must be exclusions
                requestBuilder.attributesToGet(possibleAttributes.minus(attributesEnumeration.attributes))
            }
        }

        if (query.filter != null) {
            val filterParser = FilterParser(query.filter)

            logger.warn("Calling getAll with filter: {}, values set: {}. Values: {}", filterParser.parsedFilter, filterParser.attributeValues.count(),
                filterParser.attributeValues.map { value -> value.value.s() }.joinToString(", ")
            )

            requestBuilder.filterExpression(filterParser.parsedFilter)
            requestBuilder.expressionAttributeValues(filterParser.attributeValues)

            if (filterParser.attributesNamesMap.isNotEmpty()) {
                requestBuilder.expressionAttributeNames(filterParser.attributesNamesMap)
            }
        }

        val response = dynamoDBClient.scan(requestBuilder.build())

        val result = mutableListOf<Delegation>()

        if (response.hasItems() && response.items().isNotEmpty()) {
            response.items().forEach {item -> result.add(toDelegation(item))}
        }

        return result
    }

    private fun getKey(id: String): Map<String, AttributeValue> =
            mapOf(Pair("id", AttributeValue.builder().s(id).build()))

    private fun Delegation.toItem(): Map<String, AttributeValue>
    {
        val parameters: MutableMap<String, AttributeValue> = HashMap(12)
        parameters["id"] = AttributeValue.builder().s(id).build()
        parameters["owner"] = AttributeValue.builder().s(owner).build()
        parameters["created"] = AttributeValue.builder().s(created.toString()).build()
        parameters["expires"] = AttributeValue.builder().s(expires.toString()).build()
        parameters["scope"] = AttributeValue.builder().s(scope).build()
        parameters["scopeClaims"] = jsonHandler.toJson(scopeClaims.map { scopeClaim -> scopeClaim.asMap() }).toAttributeValue()
        parameters["clientId"] = AttributeValue.builder().s(clientId).build()

        if (!redirectUri.isNullOrEmpty()) {
            parameters["redirectUri"] = AttributeValue.builder().s(redirectUri).build()
        }
        parameters["status"] = AttributeValue.builder().s(status.name).build()
        parameters["claims"] = AttributeValue.builder().s(jsonHandler.toJson(claims)).build()
        parameters["claimMap"] = AttributeValue.builder().s(jsonHandler.toJson(claimMap)).build()
        parameters["customClaimValues"] = AttributeValue.builder().s(jsonHandler.toJson(customClaimValues)).build()
        parameters["authenticationAttributes"] = AttributeValue.builder().s(jsonHandler.toJson(authenticationAttributes.asMap())).build()

        if (!authorizationCodeHash.isNullOrEmpty()) {
            parameters["authorizationCodeHash"] = AttributeValue.builder().s(authorizationCodeHash).build()
        }

        if (!mtlsClientCertificate.isNullOrEmpty()) {
            parameters["mtlsClientCertificate"] = AttributeValue.builder().s(mtlsClientCertificate).build()
        }

        if (!mtlsClientCertificateDN.isNullOrEmpty()) {
            parameters["mtlsClientCertificateDN"] = AttributeValue.builder().s(mtlsClientCertificateDN).build()
        }

        if (!mtlsClientCertificateX5TS256.isNullOrEmpty()) {
            parameters["mtlsClientCertificateX5TS256"] = AttributeValue.builder().s(mtlsClientCertificateX5TS256).build()
        }

        if (consentResult != null) {
            parameters["consentResult"] = AttributeValue.builder().s(jsonHandler.toJson(consentResult.asMap())).build()
        }

        return parameters
    }

    private fun toDelegation(item: Map<String, AttributeValue>): Delegation = DelegationData(
        item["id"],
        item["owner"],
        item["created"],
        item["expires"],
        item["scope"],
        item["scopeClaims"],
        item["clientId"],
        item["redirectUri"],
        item["status"],
        item["claims"],
        item["claimMap"],
        item["customClaimValues"],
        item["authenticationAttributes"],
        item["authorizationCodeHash"],
        item["mtlsClientCertificate"],
        item["mtlsClientCertificateDN"],
        item["mtlsClientCertificateX5TS256"],
        item["consentResult"],
        jsonHandler
    )

    companion object {
        private const val tableName = "curity-delegations"
        private val logger = LoggerFactory.getLogger(DynamoDBDelegationDataAccessProvider::class.java)
        private val possibleAttributes = listOf(
                "id", "owner", "created", "expires", "scope", "scopeClaims", "clientId",
                "redirectUri", "status", "claims", "claimMap", "customClaimValues", "authenticationAttributes",
                "authorizationCodeHash", "mtlsClientCertificate", "mtlsClientCertificateDN", "mtlsClientCertificateX5TS256",
                "consentResult")
    }
}
