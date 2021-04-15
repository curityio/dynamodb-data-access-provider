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
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.data.authorization.Token
import se.curity.identityserver.sdk.data.authorization.TokenStatus
import se.curity.identityserver.sdk.datasource.TokenDataAccessProvider
import se.curity.identityserver.sdk.service.Json
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

class DynamoDBTokenDataAccessProvider(configuration: DynamoDBDataAccessProviderConfiguration, private val dynamoDBClient: DynamoDBClient): TokenDataAccessProvider
{
    private val jsonHandler = configuration.getJsonHandler()

    override fun getByHash(hash: String): Token?
    {
        val request = GetItemRequest.builder()
                .tableName(tableName)
                .key(getKey(hash))
                .build()

        val response = dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        return getToken(response.item())
    }

    override fun getById(id: String): Token?
    {
        val request = QueryRequest.builder()
                .tableName(tableName)
                .indexName("id-index")
                .keyConditionExpression("id = :id")
                .expressionAttributeValues(mapOf(Pair(":id", AttributeValue.builder().s(id).build())))
                .limit(1)
                .build()

        val response = dynamoDBClient.query(request)

        if (!response.hasItems() || response.items().isEmpty()) {
            return null
        }

        val responseItem = response.items().first()

        return getToken(responseItem)
    }

    override fun create(token: Token)
    {
        val request = PutItemRequest.builder()
                .tableName(tableName)
                .item(token.toParametersMap(jsonHandler))
                .build()

        dynamoDBClient.putItem(request)
    }

    override fun getStatus(tokenHash: String): String?
    {
        val token = getByHash(tokenHash)

        return token?.status?.toString()
    }

    override fun setStatusByTokenHash(tokenHash: String, newStatus: TokenStatus): Long
    {
        val request = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(getKey(tokenHash))
                .expressionAttributeValues(mapOf<String, AttributeValue>(Pair(":status", AttributeValue.builder().s(newStatus.name).build())))
                .updateExpression("SET #status = :status")
                .expressionAttributeNames(mapOf(Pair("#status", "status")))
                .returnValues(ReturnValue.UPDATED_NEW)
                .build()

        val response = dynamoDBClient.updateItem(request)

        return if (response.hasAttributes() && response.attributes().isNotEmpty()) {
            1
        } else {
            0
        }
    }

    override fun setStatus(tokenId: String, newStatus: TokenStatus): Long
    {
        val token = getById(tokenId) ?: return 0L

        return setStatusByTokenHash(token.tokenHash, newStatus)
    }

    private fun Token.toParametersMap(jsonHandler: Json): Map<String, AttributeValue>
    {
//        logger.warn("Token data, tokenHash: {}", tokenHash)
//        logger.warn("Token data, delegationsId: {}", delegationsId)
//        logger.warn("Token data, purpose: {}", purpose)
//        logger.warn("Token data, usage: {}", usage)
//        logger.warn("Token data, format: {}", format)
//        logger.warn("Token data, created: {}", created)
//        logger.warn("Token data, expires: {}", expires)
//        logger.warn("Token data, scope: {}", scope)
//        logger.warn("Token data, status.name: {}", status.name)
//        logger.warn("Token data, issuer: {}", issuer)
//        logger.warn("Token data, subject: {}", subject)
//        logger.warn("Token data, serAudience: {}", jsonHandler.toJson(audience.values))
//        logger.warn("Token data, notBefore: {}", notBefore)
//        logger.warn("Token data, data: {}", jsonHandler.toJson(data))

        val parameters: MutableMap<String, AttributeValue> = HashMap(15)
        parameters["tokenHash"] = AttributeValue.builder().s(tokenHash).build()

        if (!id.isNullOrEmpty()) {
            parameters["id"] = AttributeValue.builder().s(id).build()
        }

        parameters["delegationsId"] = AttributeValue.builder().s(delegationsId).build()
        parameters["purpose"] = AttributeValue.builder().s(purpose).build()
        parameters["usage"] = AttributeValue.builder().s(usage).build()
        parameters["format"] = AttributeValue.builder().s(format).build()
        parameters["created"] = AttributeValue.builder().s(created.toString()).build()
        parameters["expires"] = AttributeValue.builder().s(expires.toString()).build()
        parameters["scope"] = AttributeValue.builder().s(scope).build()
        parameters["status"] = AttributeValue.builder().s(status.name).build()
        parameters["issuer"] = AttributeValue.builder().s(issuer).build()
        parameters["subject"] = AttributeValue.builder().s(subject).build()
        parameters["serializedAudience"] = AttributeValue.builder().s(jsonHandler.toJson(audience.values)).build()
        parameters["notBefore"] = AttributeValue.builder().s(notBefore.toString()).build()
        parameters["serializedTokenData"] = AttributeValue.builder().s(jsonHandler.toJson(data)).build()

        return parameters
    }

    private fun getToken(tokenMap: Map<String, AttributeValue>): Token =
        TokenData(
            tokenMap["tokenHash"],
            tokenMap["id"],
            tokenMap["delegationsId"],
            tokenMap["purpose"],
            tokenMap["usage"],
            tokenMap["format"],
            tokenMap["created"],
            tokenMap["expires"],
            tokenMap["scope"],
            tokenMap["status"],
            tokenMap["issuer"],
            tokenMap["subject"],
            tokenMap["serializedAudience"],
            tokenMap["notBefore"],
            tokenMap["serializedTokenData"],
            jsonHandler
        )

    private fun getKey(hash: String): Map<String, AttributeValue> =
            mapOf(Pair("tokenHash", AttributeValue.builder().s(hash).build()))

    companion object {
        private const val tableName = "curity-tokens"
        private val logger = LoggerFactory.getLogger(DynamoDBTokenDataAccessProvider::class.java)
    }
}
