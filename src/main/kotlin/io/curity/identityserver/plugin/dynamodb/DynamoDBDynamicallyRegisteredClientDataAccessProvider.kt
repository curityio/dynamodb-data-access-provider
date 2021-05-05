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
package io.curity.identityserver.plugin.dynamodb

import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.attribute.Attribute
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.MapAttributeValue
import se.curity.identityserver.sdk.attribute.scim.v2.Meta
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes.META
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
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant.now
import java.time.Instant.ofEpochSecond

class DynamoDBDynamicallyRegisteredClientDataAccessProvider(
    configuration: DynamoDBDataAccessProviderConfiguration,
    private val _dynamoDBClient: DynamoDBClient
) : DynamicallyRegisteredClientDataAccessProvider
{
    private val _jsonHandler = configuration.getJsonHandler()

    private object DcrTable : Table("curity-dynamic-clients")
    {
        val clientId = StringAttribute("clientId")
        val clientSecret = StringAttribute("clientSecret")
        val instanceOfClient = StringAttribute("instanceOfClient")
        val created = NumberLongAttribute("created")
        val updated = NumberLongAttribute("updated")
        val initialClient = StringAttribute("initialClient")
        val authenticatedUser = StringAttribute("authenticatedUser")
        val attributes = StringAttribute("attributes")
        val status = StringAttribute("status")
        val scope = ListStringAttribute("scope")
        val redirectUris = ListStringAttribute("redirectUris")
        val grantTypes = ListStringAttribute("grantTypes")

        fun key(value: String) = mapOf(clientId.toNameValuePair(value))
    }

    private fun DynamoDBItem.toAttributes(): DynamicallyRegisteredClientAttributes
    {

        val result = mutableListOf<Attribute>()
        val item = this

        result.apply {
            // Non-nullable
            add(CLIENT_ID, DcrTable.clientId.from(item))
            add(
                Attribute.of(
                    META,
                    Meta.of(DynamicallyRegisteredClientAttributes.RESOURCE_TYPE)
                        .withCreated(ofEpochSecond(DcrTable.created.from(item)))
                        .withLastModified(ofEpochSecond(DcrTable.updated.from(item)))
                )
            )
            add(Attribute.of(STATUS, DynamicallyRegisteredClientAttributes.Status.valueOf(DcrTable.status.from(item))))
            add(ATTRIBUTES, MapAttributeValue.of(_jsonHandler.toAttributes(DcrTable.attributes.from(item))))

            // Nullable
            add(CLIENT_SECRET, DcrTable.clientSecret.fromOpt(item))
            add(INSTANCE_OF_CLIENT, DcrTable.instanceOfClient.fromOpt(item))
            add(INITIAL_CLIENT, DcrTable.initialClient.fromOpt(item))
            add(AUTHENTICATED_USER, DcrTable.authenticatedUser.fromOpt(item))
            add(SCOPE, DcrTable.scope.fromOpt(item))
            add(REDIRECT_URIS, DcrTable.redirectUris.fromOpt(item))
            add(GRANT_TYPES, DcrTable.grantTypes.fromOpt(item))
        }

        return DynamicallyRegisteredClientAttributes.of(Attributes.of(result))
    }

    private fun DynamicallyRegisteredClientAttributes.toItem(): DynamoDBItem
    {
        val now = now()
        val created = meta?.created ?: now

        val item = mutableMapOf<String, AttributeValue>()

        // Non-nullable
        DcrTable.clientId.addTo(item, clientId)
        DcrTable.created.addTo(item, created.epochSecond)
        DcrTable.updated.addTo(item, now.epochSecond)
        DcrTable.status.addTo(item, status.name)
        DcrTable.attributes.addTo(item, _jsonHandler.fromAttributes(Attributes.of(attributes)))

        // Nullable
        DcrTable.clientSecret.addToOpt(item, clientSecret)
        DcrTable.instanceOfClient.addToOpt(item, instanceOfClient)
        DcrTable.initialClient.addToOpt(item, initialClient)
        DcrTable.authenticatedUser.addToOpt(item, authenticatedUser)
        // Empty lists are not converted into attributes
        DcrTable.scope.addToOpt(item, scope)
        DcrTable.redirectUris.addToOpt(item, redirectUris)
        DcrTable.grantTypes.addToOpt(item, grantTypes)

        return item
    }

    private fun MutableList<Attribute>.add(name: String, value: String?) = value?.let {
        this.add(Attribute.of(name, it))
    }

    private fun MutableList<Attribute>.add(name: String, value: Collection<String>?) = value?.let {
        this.add(Attribute.of(name, it))
    }

    private fun MutableList<Attribute>.add(name: String, value: MapAttributeValue?) = value?.let {
        this.add(Attribute.of(name, it))
    }

    override fun getByClientId(clientId: String): DynamicallyRegisteredClientAttributes?
    {
        logger.debug("Getting dynamic client with id: {}", clientId)

        val request = GetItemRequest.builder()
            .tableName(DcrTable.name)
            .key(DcrTable.key(clientId))
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty())
        {
            return null
        }

        return response.item().toAttributes()
    }

    override fun create(dynamicallyRegisteredClientAttributes: DynamicallyRegisteredClientAttributes)
    {
        logger.debug(
            "Received request to CREATE dynamic client with id: {}",
            dynamicallyRegisteredClientAttributes.clientId
        )

        val request = PutItemRequest.builder()
            .tableName(DcrTable.name)
            .conditionExpression("attribute_not_exists(${DcrTable.clientId.name})")
            .item(dynamicallyRegisteredClientAttributes.toItem())
            .build()

        try
        {
            _dynamoDBClient.putItem(request)
        } catch (exception: ConditionalCheckFailedException)
        {
            val newException =
                ConflictException("Client ${dynamicallyRegisteredClientAttributes.clientId} is already registered")
            logger.trace(
                "Client ${dynamicallyRegisteredClientAttributes.clientId} is already registered",
                newException
            )
            throw newException
        }
    }

    override fun update(dynamicallyRegisteredClientAttributes: DynamicallyRegisteredClientAttributes)
    {
        logger.debug(
            "Received request to UPDATE dynamic client for client : {}",
            dynamicallyRegisteredClientAttributes.clientId
        )

        val builder = UpdateExpressionsBuilder()
        dynamicallyRegisteredClientAttributes.apply {
            // The client secret is only updated if not null
            if (!clientSecret.isNullOrEmpty())
            {
                builder.update(DcrTable.clientSecret, clientSecret)
            }
            builder.update(DcrTable.updated, now().epochSecond)
            builder.update(DcrTable.status, status.name)
            builder.update(DcrTable.scope, scope)
            builder.update(DcrTable.redirectUris, redirectUris)
            builder.update(DcrTable.grantTypes, grantTypes)
            builder.update(
                DcrTable.attributes,
                _jsonHandler.fromAttributes(Attributes.of(dynamicallyRegisteredClientAttributes.attributes))
            )
        }

        val requestBuilder = UpdateItemRequest.builder()
            .tableName(DcrTable.name)
            .key(DcrTable.key(dynamicallyRegisteredClientAttributes.clientId))
            .apply { builder.applyTo(this) }

        _dynamoDBClient.updateItem(requestBuilder.build())
    }

    override fun delete(clientId: String)
    {
        logger.debug("Received request to DELETE dynamic client : {}", clientId)

        val request = DeleteItemRequest.builder()
            .tableName(DcrTable.name)
            .key(DcrTable.key(clientId))
            .build()

        _dynamoDBClient.deleteItem(request)
    }

    companion object
    {
        private val logger: Logger = LoggerFactory.getLogger(DynamoDBCredentialDataAccessProvider::class.java)
    }
}
