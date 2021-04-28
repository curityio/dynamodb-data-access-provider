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
import io.curity.identityserver.plugin.dynamodb.Index
import io.curity.identityserver.plugin.dynamodb.Index2
import io.curity.identityserver.plugin.dynamodb.NumberLongAttribute
import io.curity.identityserver.plugin.dynamodb.SchemaErrorException
import io.curity.identityserver.plugin.dynamodb.StringAttribute
import io.curity.identityserver.plugin.dynamodb.Table
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import io.curity.identityserver.plugin.dynamodb.count
import io.curity.identityserver.plugin.dynamodb.intOrThrow
import io.curity.identityserver.plugin.dynamodb.querySequence
import io.curity.identityserver.plugin.dynamodb.scanSequence
import io.curity.identityserver.plugin.dynamodb.toAttributeValue
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.data.authorization.Delegation
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
    private val dynamoDBClient: DynamoDBClient,
    configuration: DynamoDBDataAccessProviderConfiguration
) : DelegationDataAccessProvider
{
    private val jsonHandler = configuration.getJsonHandler()

    override fun getById(id: String): Delegation?
    {
        val request = GetItemRequest.builder()
            .tableName(DelegationTable.name)
            .key(mapOf(DelegationTable.id.toNameValuePair(id)))
            .build()

        val response = dynamoDBClient.getItem(request)

        if (!response.hasItem())
        {
            return null
        }
        val item = response.item()

        val status =
            DelegationStatus.valueOf(
                DelegationTable.status.from(item)
                    ?: throw SchemaErrorException(DelegationTable, DelegationTable.status))
        if (status != DelegationStatus.issued)
        {
            return null
        }
        return toDelegation(item)
    }

    override fun getByAuthorizationCodeHash(authorizationCodeHash: String): Delegation?
    {
        val request = QueryRequest.builder()
            .tableName(DelegationTable.name)
            .indexName(DelegationTable.authorizationCodeIndex.name)
            .keyConditionExpression(DelegationTable.authorizationCodeIndex.expression)
            .expressionAttributeValues(DelegationTable.authorizationCodeIndex.expressionValueMap(authorizationCodeHash))
            .expressionAttributeNames(DelegationTable.authorizationCodeIndex.expressionNameMap)
            .build()

        val response = dynamoDBClient.query(request)

        if (!response.hasItems() || response.items().isEmpty())
        {
            return null
        }

        // If multiple entries exist, we use the first one
        return toDelegation(response.items().first())
    }

    override fun create(delegation: Delegation)
    {
        val request = PutItemRequest.builder()
            .tableName(DelegationTable.name)
            .item(delegation.toItem())
            .build()

        dynamoDBClient.putItem(request)
    }

    override fun setStatus(id: String, newStatus: DelegationStatus): Long
    {
        val request = UpdateItemRequest.builder()
            .tableName(DelegationTable.name)
            .key(mapOf(DelegationTable.id.toNameValuePair(id)))
            .updateExpression("SET ${DelegationTable.status.hashName} = ${DelegationTable.status.colonName}")
            .expressionAttributeValues(mapOf(DelegationTable.status.toExpressionNameValuePair(newStatus.toString())))
            .expressionAttributeNames(mapOf(DelegationTable.status.toNamePair()))
            .returnValues(ReturnValue.UPDATED_NEW)
            .build()

        val response = dynamoDBClient.updateItem(request)

        return if (response.hasAttributes() && response.attributes().isNotEmpty()) {
            1
        } else {
            0
        }

    }

    override fun getByOwner(owner: String, startIndex: Long, count: Long): Collection<Delegation>
    {
        val validatedStartIndex = startIndex.intOrThrow("startIndex")
        val validatedCount = count.intOrThrow("count")
        val request = QueryRequest.builder()
            .tableName(DelegationTable.name)
            .indexName(DelegationTable.ownerStatusIndex.name)
            .keyConditionExpression(DelegationTable.ownerStatusIndex.keyConditionExpression)
            .expressionAttributeValues(
                DelegationTable.ownerStatusIndex.expressionValueMap(owner, DelegationStatus.issued.toString())
            )
            .expressionAttributeNames(DelegationTable.ownerStatusIndex.expressionNameMap)
            .limit(count.toInt())
            .build()

        return querySequence(request, dynamoDBClient)
            .drop(validatedStartIndex)
            .map{ responseItem -> toDelegation(responseItem)}
            .take(validatedCount)
            .toList()
    }

    override fun getCountByOwner(owner: String): Long
    {
        val request = QueryRequest.builder()
            .tableName(DelegationTable.name)
            .indexName(DelegationTable.ownerStatusIndex.name)
            .keyConditionExpression(DelegationTable.ownerStatusIndex.keyConditionExpression)
            .expressionAttributeValues(
                DelegationTable.ownerStatusIndex.expressionValueMap(owner, DelegationStatus.issued.toString())
            )
            .expressionAttributeNames(DelegationTable.ownerStatusIndex.expressionNameMap)
            .select(Select.COUNT)
            .build()

        return count(request, dynamoDBClient)
    }


    override fun getAllActive(startIndex: Long, count: Long): Collection<Delegation>
    {
        val validatedStartIndex = startIndex.intOrThrow("startIndex")
        val validatedCount = count.intOrThrow("count")

        val request = ScanRequest.builder()
            .tableName(DelegationTable.name)
            .filterExpression("#status = :status")
            .expressionAttributeValues(issuedStatusExpressionAttributeMap)
            .expressionAttributeNames(issuedStatusExpressionAttributeNameMap)
            .build()


        return scanSequence(request, dynamoDBClient)
            .drop(validatedStartIndex)
            .map{ responseItem -> toDelegation(responseItem)}
            .take(validatedCount)
            .toList()
    }

    override fun getCountAllActive(): Long
    {
        val request = ScanRequest.builder()
            .tableName(DelegationTable.name)
            .filterExpression("#status = :status")
            .expressionAttributeValues(issuedStatusExpressionAttributeMap)
            .expressionAttributeNames(issuedStatusExpressionAttributeNameMap)
            .build()

        return count(request, dynamoDBClient)
    }

    override fun getAll(query: ResourceQuery): MutableCollection<out Delegation>
    {
        // TODO implement sorting - can be tricky
        // TODO implement pagination and possible DynamoDB pagination

        val requestBuilder = ScanRequest.builder()
            .tableName(tableName)

        if (!query.attributesEnumeration.isNeutral)
        {
            val attributesEnumeration = query.attributesEnumeration

            // TODO stop using the attributesToGet and use projectionExpression instead
            if (attributesEnumeration is ResourceQuery.Inclusions)
            {
                requestBuilder.attributesToGet(attributesEnumeration.attributes)
            } else
            {
                // must be exclusions
                requestBuilder.attributesToGet(possibleAttributes.minus(attributesEnumeration.attributes))
            }
        }

        if (query.filter != null)
        {
            val filterParser = DelegationsFilterParser(query.filter)

            logger.warn(
                "Calling getAll with filter: {}, values set: {}. Values: {}",
                filterParser.parsedFilter,
                filterParser.attributeValues.count(),
                filterParser.attributeValues.map { value -> value.value.s() }.joinToString(", ")
            )

            requestBuilder.filterExpression(filterParser.parsedFilter)
            requestBuilder.expressionAttributeValues(filterParser.attributeValues)

            if (filterParser.attributesNamesMap.isNotEmpty())
            {
                requestBuilder.expressionAttributeNames(filterParser.attributesNamesMap)
            }
        }

        val response = dynamoDBClient.scan(requestBuilder.build())

        val result = mutableListOf<Delegation>()

        if (response.hasItems() && response.items().isNotEmpty())
        {
            response.items().forEach { item -> result.add(toDelegation(item)) }
        }

        return result
    }

    private fun Delegation.toItem(): Map<String, AttributeValue>
    {
        val parameters: MutableMap<String, AttributeValue> = HashMap(12)
        parameters["id"] = AttributeValue.builder().s(id).build()
        parameters["owner"] = AttributeValue.builder().s(owner).build()
        parameters["created"] = AttributeValue.builder().s(created.toString()).build()
        parameters["expires"] = AttributeValue.builder().s(expires.toString()).build()
        parameters["scope"] = AttributeValue.builder().s(scope).build()
        parameters["scopeClaims"] =
            jsonHandler.toJson(scopeClaims.map { scopeClaim -> scopeClaim.asMap() }).toAttributeValue()
        parameters["clientId"] = AttributeValue.builder().s(clientId).build()

        if (!redirectUri.isNullOrEmpty())
        {
            parameters["redirectUri"] = AttributeValue.builder().s(redirectUri).build()
        }
        parameters["status"] = AttributeValue.builder().s(status.name).build()
        parameters["claims"] = AttributeValue.builder().s(jsonHandler.toJson(claims)).build()
        parameters["claimMap"] = AttributeValue.builder().s(jsonHandler.toJson(claimMap)).build()
        parameters["customClaimValues"] = AttributeValue.builder().s(jsonHandler.toJson(customClaimValues)).build()
        parameters["authenticationAttributes"] =
            AttributeValue.builder().s(jsonHandler.toJson(authenticationAttributes.asMap())).build()

        if (!authorizationCodeHash.isNullOrEmpty())
        {
            parameters["authorizationCodeHash"] = AttributeValue.builder().s(authorizationCodeHash).build()
        }

        if (!mtlsClientCertificate.isNullOrEmpty())
        {
            parameters["mtlsClientCertificate"] = AttributeValue.builder().s(mtlsClientCertificate).build()
        }

        if (!mtlsClientCertificateDN.isNullOrEmpty())
        {
            parameters["mtlsClientCertificateDN"] = AttributeValue.builder().s(mtlsClientCertificateDN).build()
        }

        if (!mtlsClientCertificateX5TS256.isNullOrEmpty())
        {
            parameters["mtlsClientCertificateX5TS256"] =
                AttributeValue.builder().s(mtlsClientCertificateX5TS256).build()
        }

        if (consentResult != null)
        {
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

    object DelegationTable : Table("curity-delegations")
    {
        val id = StringAttribute("id")
        val status = StringAttribute("status")
        val owner = StringAttribute("owner")
        val authorizationCode = StringAttribute("authorizationCodeHash")

        val created = NumberLongAttribute("created")
        val expires = NumberLongAttribute("expires")

        val scope = StringAttribute("scope")
        val scopeClaims = StringAttribute("scopeClaims")
        val claimMap = StringAttribute("claimMap")
        val clientId = StringAttribute("clientId")
        val redirectUri = StringAttribute("redirectUri")
        val authorizationCodeHash = StringAttribute("authorizationCodeHash")
        val authenticationAttributes = StringAttribute("authenticationAttributes")
        val customClaimValues = StringAttribute("customClaimValues")
        val enumActiveStatus = StringAttribute("enumActiveStatus")
        val mtlsClientCertificate = StringAttribute("mtlsClientCertificate")
        val mtlsClientCertificateX5TS256 = StringAttribute("mtlsClientCertificateX5TS256")
        val mtlsClientCertificateDN = StringAttribute("mtlsClientCertificateDN")
        val consentResult = StringAttribute("consentResult")
        val claims = StringAttribute("claims")

        val ownerStatusIndex = Index2("owner-status-index", owner, status)
        val authorizationCodeIndex = Index("authorization-hash-index", authorizationCode)
    }

    companion object
    {
        private const val tableName = "curity-delegations"
        private val logger = LoggerFactory.getLogger(DynamoDBDelegationDataAccessProvider::class.java)
        private val possibleAttributes = listOf(
            "id", "owner", "created", "expires", "scope", "scopeClaims", "clientId",
            "redirectUri", "status", "claims", "claimMap", "customClaimValues", "authenticationAttributes",
            "authorizationCodeHash", "mtlsClientCertificate", "mtlsClientCertificateDN", "mtlsClientCertificateX5TS256",
            "consentResult"
        )
        private val issuedStatusKey = Pair("status", "issued".toAttributeValue())
        private val issuedStatusExpressionAttribute = Pair(":status", "issued".toAttributeValue())
        private val issuedStatusExpressionAttributeMap = mapOf(issuedStatusExpressionAttribute)
        private val issuedStatusExpressionAttributeName = Pair("#status", "status")
        private val issuedStatusExpressionAttributeNameMap = mapOf(issuedStatusExpressionAttributeName)
        private val ownerExpressionAttributeName = Pair("#owner", "owner")
    }
}
