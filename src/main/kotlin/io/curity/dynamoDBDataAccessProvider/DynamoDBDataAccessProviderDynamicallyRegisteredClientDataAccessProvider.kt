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
package io.curity.dynamoDBDataAccessProvider

import io.curity.dynamoDBDataAccessProvider.configuration.DynamoDBDataAccessProviderDataAccessProviderConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.attribute.Attribute
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.MapAttributeValue
import se.curity.identityserver.sdk.attribute.scim.v2.Meta
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DynamicallyRegisteredClientAttributes
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DynamicallyRegisteredClientAttributes.ATTRIBUTES
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DynamicallyRegisteredClientAttributes.AUTHENTICATED_USER
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DynamicallyRegisteredClientAttributes.CLIENT_ID
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DynamicallyRegisteredClientAttributes.CLIENT_SECRET
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DynamicallyRegisteredClientAttributes.GRANT_TYPES
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DynamicallyRegisteredClientAttributes.INITIAL_CLIENT
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DynamicallyRegisteredClientAttributes.INSTANCE_OF_CLIENT
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DynamicallyRegisteredClientAttributes.REDIRECT_URIS
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DynamicallyRegisteredClientAttributes.SCOPE
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DynamicallyRegisteredClientAttributes.STATUS
import se.curity.identityserver.sdk.datasource.DynamicallyRegisteredClientDataAccessProvider
import se.curity.identityserver.sdk.errors.ConflictException
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant.now

class DynamoDBDataAccessProviderDynamicallyRegisteredClientDataAccessProvider(configuration: DynamoDBDataAccessProviderDataAccessProviderConfig, private val dynamoDBClient: DynamoDBClient): DynamicallyRegisteredClientDataAccessProvider
{
    private val jsonHandler = configuration.getJsonHandler()

    override fun getByClientId(clientId: String): DynamicallyRegisteredClientAttributes?
    {
        logger.debug("Getting dynamic client with id: {}", clientId)

        val request = GetItemRequest.builder()
                .tableName(tableName)
                .key(clientId.toKey("clientId"))
                .build()
    
        val response = dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        val result = mutableListOf<Attribute>()
        val item = response.item()
        
        result.add(Attribute.of(CLIENT_ID, item["clientId"]?.s()))
        result.add(Attribute.of(CLIENT_SECRET, item["clientSecret"]?.s()))

        if (item["instanceOfClient"] != null) {
            result.add(Attribute.of(INSTANCE_OF_CLIENT, item["instanceOfClient"]!!.s()))
        }

        result.add(Attribute.of(Meta.CREATED, item["created"]!!.s().toLong()))
        result.add(Attribute.of(Meta.LAST_MODIFIED, item["updated"]!!.s().toLong()))

        if (item["initialClient"] != null) {
            result.add(Attribute.of(INITIAL_CLIENT, item["initialClient"]!!.s()))
        }

        if (item["authenticatedUser"] != null) {
            result.add(Attribute.of(AUTHENTICATED_USER, item["authenticatedUser"]!!.s()))
        }

        if (item["attributes"] != null) {
            result.add(Attribute.of(ATTRIBUTES, MapAttributeValue.of(jsonHandler.toAttributes(item["attributes"]!!.s()))))
        }
        result.add(Attribute.of(STATUS, DynamicallyRegisteredClientAttributes.Status.valueOf(item["dcrStatus"]!!.s())))

        if (item["scope"] != null) {
            result.add(Attribute.of(SCOPE, item["scope"]!!.l().map { scope -> scope.s() }))
        }

        if (item["redirectUris"] != null) {
            result.add(Attribute.of(REDIRECT_URIS, item["redirectUris"]!!.l().map { uri -> uri.s() }))
        }

        if (item["grantTypes"] != null) {
            result.add(Attribute.of(GRANT_TYPES, item["grantTypes"]!!.l().map { grant -> grant.s() }))
        }

        return DynamicallyRegisteredClientAttributes.of(result)

    }

    override fun create(dynamicallyRegisteredClientAttributes: DynamicallyRegisteredClientAttributes)
    {
        logger.debug("Received request to CREATE dynamic client with id: {}", dynamicallyRegisteredClientAttributes.clientId)

        val created = dynamicallyRegisteredClientAttributes.meta?.created ?: now()

        val item = mutableMapOf(
                Pair("clientId", dynamicallyRegisteredClientAttributes.clientId.toAttributeValue()),
                Pair("clientSecret", dynamicallyRegisteredClientAttributes.clientSecret.toAttributeValue()),
                Pair("created", created.epochSecond.toAttributeValue()),
                Pair("updated", now().epochSecond.toAttributeValue()),
                Pair("dcrStatus", dynamicallyRegisteredClientAttributes.status.name.toAttributeValue())
        )

        if (dynamicallyRegisteredClientAttributes.authenticatedUser != null) {
            item["authenticatedUser"] = dynamicallyRegisteredClientAttributes.authenticatedUser.toAttributeValue()
        }

        if (dynamicallyRegisteredClientAttributes.initialClient != null) {
            item["initialClient"] = dynamicallyRegisteredClientAttributes.initialClient.toAttributeValue()
        }
        if (dynamicallyRegisteredClientAttributes.instanceOfClient != null) {
            item["instanceOfClient"] = dynamicallyRegisteredClientAttributes.instanceOfClient.toAttributeValue()
        }

        if (dynamicallyRegisteredClientAttributes.attributes != null && !dynamicallyRegisteredClientAttributes.attributes.isEmpty) {
            item["attributes"] = jsonHandler.fromAttributes(Attributes.of(dynamicallyRegisteredClientAttributes.attributes)).toAttributeValue()
        }

        if (!dynamicallyRegisteredClientAttributes.scope.isNullOrEmpty()) {
            item["scope"] = dynamicallyRegisteredClientAttributes.scope.toAttributeValue()
        }

        if (!dynamicallyRegisteredClientAttributes.redirectUris.isNullOrEmpty()) {
            item["redirectUris"] = dynamicallyRegisteredClientAttributes.redirectUris.toAttributeValue()
        }

        if (!dynamicallyRegisteredClientAttributes.grantTypes.isNullOrEmpty()) {
            item["grantTypes"] = dynamicallyRegisteredClientAttributes.grantTypes.toAttributeValue()
        }

        val request = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build()

        try {
            dynamoDBClient.putItem(request)
        } catch (exception: DynamoDbException) {
            if (exception.awsErrorDetails().errorCode() == "ConditionalCheckFailedException") {
                val newException = ConflictException("Client ${dynamicallyRegisteredClientAttributes.clientId} is already registered")
                logger.trace("Client ${dynamicallyRegisteredClientAttributes.clientId} is already registered", newException)
                throw newException
            }
            logger.trace("Creating dynamic client threw an exception", exception)
        }
    }

    override fun update(dynamicallyRegisteredClientAttributes: DynamicallyRegisteredClientAttributes)
    {
        logger.debug("Received request to UPDATE dynamic client for client : {}", dynamicallyRegisteredClientAttributes.clientId)

        val updateExpression = "SET clientSecret = :clientSecret, updated = :updated, attributes = :attributes, dcrStatus = :dcrStatus, scope = :scope, redirectUris = :redirectUris, grantTypes = :grantTypes"

        val valuesToUpdate = mapOf(
                Pair(":clientSecret", dynamicallyRegisteredClientAttributes.clientSecret.toAttributeValue()),
                Pair(":updated", now().epochSecond.toAttributeValue()),
                Pair(":attributes", jsonHandler.fromAttributes(Attributes.of(dynamicallyRegisteredClientAttributes.attributes)).toAttributeValue()),
                Pair(":dcrStatus", dynamicallyRegisteredClientAttributes.status.name.toAttributeValue()),
                Pair(":scope", dynamicallyRegisteredClientAttributes.scope.toAttributeValue() ),
                Pair(":redirectUris", dynamicallyRegisteredClientAttributes.redirectUris.toAttributeValue()),
                Pair(":grantTypes", dynamicallyRegisteredClientAttributes.grantTypes.toAttributeValue())
        )

        val request = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(dynamicallyRegisteredClientAttributes.clientId.toKey("clientId"))
                .updateExpression(updateExpression)
                .expressionAttributeValues(valuesToUpdate)
                .build()

        dynamoDBClient.updateItem(request)
    }

    override fun delete(clientId: String)
    {
        logger.debug("Received request to DELETE dynamic client : {}", clientId)

        val request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(clientId.toKey("clientId"))
                .build()

        dynamoDBClient.deleteItem(request)
    }

    companion object
    {
        private val logger: Logger = LoggerFactory.getLogger(DynamoDBDataAccessProviderCredentialDataAccessProvider::class.java)
        private const val tableName = "curity-dynamic-clients"
    }
}
