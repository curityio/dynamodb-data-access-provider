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
import io.curity.identityserver.plugin.dynamodb.token.UserAccountFilterParser
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.attribute.AccountAttributes
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.scim.v2.Meta
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.LinkedAccount
import se.curity.identityserver.sdk.data.query.ResourceQuery
import se.curity.identityserver.sdk.data.query.ResourceQueryResult
import se.curity.identityserver.sdk.data.update.AttributeUpdate
import se.curity.identityserver.sdk.datasource.UserAccountDataAccessProvider
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

class DynamoDBUserAccountDataAccessProvider(private val dynamoDBClient: DynamoDBClient, configuration: DynamoDBDataAccessProviderConfiguration): UserAccountDataAccessProvider
{
    private val jsonHandler = configuration.getJsonHandler()

    override fun getByUserName(userName: String, attributesEnumeration: ResourceQuery.AttributesEnumeration?): ResourceAttributes<*>?
    {
        logger.debug("Received request to get account by username : {}", userName)

        val requestBuilder = GetItemRequest.builder()
                .tableName(tableName)
                .key(userName.toKey("userName"))

        if (attributesEnumeration != null && !attributesEnumeration.isNeutral && attributesEnumeration is ResourceQuery.Inclusions)
        {
            requestBuilder.attributesToGet(requiredAttributesToGet + attributesEnumeration.attributes)
        }

        val request = requestBuilder.build()

        val response = dynamoDBClient.getItem(request)

        return if (response.hasItem()) response.item().toAccountAttributes(attributesEnumeration) else null
    }

    override fun getByEmail(email: String, attributesEnumeration: ResourceQuery.AttributesEnumeration?): ResourceAttributes<*>?
    {
        logger.debug("Received request to get account by email : {}", email)

        return retrieveByQuery("email-index", "email = :email", ":email", email, attributesEnumeration)
    }

    override fun getByPhone(phone: String, attributesEnumeration: ResourceQuery.AttributesEnumeration?): ResourceAttributes<*>?
    {
        logger.debug("Received request to get account by phone number : {}", phone)

        return retrieveByQuery("phone-index", "phone = :phone", ":phone", phone, attributesEnumeration)
    }

    private fun retrieveByQuery(indexName: String, keyCondition: String, attributeKey: String, attributeValue: String, attributesEnumeration: ResourceQuery.AttributesEnumeration?): ResourceAttributes<*>?
    {
        val requestBuilder = QueryRequest.builder()
                .tableName(tableName)
                .indexName(indexName)
                .keyConditionExpression(keyCondition)
                .expressionAttributeValues(mapOf(Pair(attributeKey, AttributeValue.builder().s(attributeValue).build())))
                .limit(1)

        if (attributesEnumeration != null && !attributesEnumeration.isNeutral && attributesEnumeration is ResourceQuery.Inclusions)
        {
            requestBuilder.attributesToGet(requiredAttributesToGet + attributesEnumeration.attributes)
        }

        val request = requestBuilder.build()

        val response = dynamoDBClient.query(request)

        return if (response.hasItems() && response.items().isNotEmpty())
        {
            response.items().first().toAccountAttributes(attributesEnumeration)
        }
        else
        {
            null
        }
    }

    override fun create(accountAttributes: AccountAttributes): AccountAttributes
    {

        logger.debug("Received request to create account with data : {}", accountAttributes)

        val item = accountAttributes.toItem()
        val request = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression("attribute_not_exists(userName)")
                .build()

        dynamoDBClient.putItem(request)

        return item.toAccountAttributes()
    }

    override fun update(accountAttributes: AccountAttributes,
                        attributesEnumeration: ResourceQuery.AttributesEnumeration): ResourceAttributes<*>?
    {
        logger.debug("Received request to update account with data : {}", accountAttributes)

        val username = accountAttributes["userName"].value as String

        updateAccount(username, accountAttributes)

        return getByUserName(username, attributesEnumeration)
    }

    override fun update(accountId: String, map: Map<String, Any>,
                        attributesEnumeration: ResourceQuery.AttributesEnumeration): ResourceAttributes<*>?
    {
        logger.debug("Received request to update account with id:{} and data : {}", accountId, map)

        updateAccount(accountId, AccountAttributes.fromMap(map))

        return getByUserName(accountId, attributesEnumeration)
    }

    private fun updateAccount(accountId: String, accountAttributes: AccountAttributes) {
        val attributesToUpdate = mutableMapOf<String, AttributeValue>()
        val attributesToRemoveFromItem = mutableListOf<String>()
        val updateExpressionParts = mutableListOf<String>()


        if (accountAttributes.emails != null) {
            val email = accountAttributes.emails.primaryOrFirst
            if (email != null && !email.isEmpty) {
                updateExpressionParts.add("email = :email")
                attributesToUpdate[":email"] = email.significantValue.toAttributeValue()
            }
        } else {
            attributesToRemoveFromItem.add("email")
        }

        if (accountAttributes.phoneNumbers != null) {
            val phone = accountAttributes.phoneNumbers.primaryOrFirst
            if (phone != null && !phone.isEmpty) {
                updateExpressionParts.add("phone = :phone")
                attributesToUpdate[":phone"] = phone.significantValue.toAttributeValue()
            }
        } else {
            attributesToRemoveFromItem.add("phone")
        }

        if (accountAttributes["active"] != null) {
            updateExpressionParts.add("active = :active")
            attributesToUpdate[":active"] = (accountAttributes["active"].value as Boolean).toAttributeValue()
        }

        val attributesToPersist = Attributes.of(accountAttributes.filter { attribute -> !attributesToRemove.contains(attribute.name.value) })

        if (!attributesToPersist.isEmpty) {
            updateExpressionParts.add("attributes = :attributes")
            attributesToUpdate[":attributes"] = jsonHandler.fromAttributes(attributesToPersist).toAttributeValue()
        }

        updateExpressionParts.add("updated = :updated")
        attributesToUpdate[":updated"] = Instant.now().epochSecond.toAttributeValue()

        val requestBuilder = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(accountId.toKey("userName"))

        var updateExpression = ""

        if (updateExpressionParts.isNotEmpty()) {
            updateExpression += "SET ${updateExpressionParts.joinToString(", ")} "
            requestBuilder
                    .expressionAttributeValues(attributesToUpdate)

        }

        if (attributesToRemoveFromItem.isNotEmpty()) {
            updateExpression += "REMOVE ${attributesToRemoveFromItem.joinToString(", ")} "
        }

        requestBuilder.updateExpression(updateExpression)

        dynamoDBClient.updateItem(requestBuilder.build())
    }

    override fun patch(accountId: String, attributeUpdate: AttributeUpdate,
                       attributesEnumeration: ResourceQuery.AttributesEnumeration): ResourceAttributes<*>?
    {
        logger.debug("Received patch request with accountId:{} and data : {}", accountId, attributeUpdate)

        // TODO - updating values and removing fields should read data from db and compare
        // all fields, as some of the data can be set in the "attributes" blob. Currently the
        // implementation is naive and assumes we're only editing/removing root attributes which
        // live in their own columns (not in the "attributes" blob)

        if (!attributeUpdate.attributeAdditions.isEmpty || !attributeUpdate.attributeReplacements.isEmpty) {
            updateAccount(accountId, AccountAttributes.of(attributeUpdate.attributeAdditions + attributeUpdate.attributeReplacements))
        }

        if (attributeUpdate.attributeDeletions.attributeNamesToDelete.isNotEmpty()) {
            val request = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(accountId.toKey("userName"))
                    .updateExpression("REMOVE ${attributeUpdate.attributeDeletions.attributeNamesToDelete.joinToString(", ")}")
                    .build()

            dynamoDBClient.updateItem(request)
        }

        return getByUserName(accountId, attributesEnumeration)
    }

    override fun link(linkingAccountManager: String, localAccountId: String, foreignDomainName: String, foreignUserName: String)
    {
        /**
         * The account links table has primary key: foreignUserName@foreignDomainName-linkingAccountManager
         * Secondary Index: primary key: foreignId@Domain with sort key linkingAccountManager
         * Attributes: linkingAccountManager, localAccountId
         */
        val item = mutableMapOf<String, AttributeValue>()
        item["localAccountId"] = localAccountId.toAttributeValue()
        item["linkingAccountManager"] = linkingAccountManager.toAttributeValue()
        item["foreignAccountAtDomain"] = "$foreignUserName@$foreignDomainName".toAttributeValue()
        item["foreignAccountId"] = foreignUserName.toAttributeValue()
        item["foreignDomainName"] = foreignDomainName.toAttributeValue()
        item["localIdToforeignIdAtdomainForManager"] = "$foreignUserName@$foreignDomainName-$linkingAccountManager".toAttributeValue()
        item["created"] = Instant.now().epochSecond.toString().toAttributeValue()

        val request = PutItemRequest.builder()
                .tableName(linksTableName)
                .item(item)
                .build()

        dynamoDBClient.putItem(request)
    }

    override fun listLinks(linkingAccountManager: String, localAccountId: String): Collection<LinkedAccount>
    {
        val attributes = mapOf(
                Pair(":manager", AttributeValue.builder().s(linkingAccountManager).build()),
                Pair(":accountId", AttributeValue.builder().s(localAccountId).build())
        )
        val request = QueryRequest.builder()
                .tableName(linksTableName)
                .indexName("list-links-index")
                .keyConditionExpression("linkingAccountManager = :manager AND localAccountId = :accountId")
                .expressionAttributeValues(attributes)
                .build()

        val response = dynamoDBClient.query(request)

        return if (response.hasItems() && response.items().isNotEmpty())
        {
            response.items().map { item -> LinkedAccount.of(item["foreignDomainName"]?.s(), item["foreignAccountId"]?.s(), "", item["created"]?.s()) }
        }
        else
        {
            emptyList()
        }
    }

    override fun resolveLink(linkingAccountManager: String, foreignDomainName: String, foreignAccountId: String): AccountAttributes?
    {
        val request = GetItemRequest.builder()
                .tableName(linksTableName)
                .key(getLinksKey(foreignAccountId, foreignDomainName, linkingAccountManager))
                .build()

        val response = dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty())
        {
            return null
        }

        val localAccountId = response.item()["localAccountId"]!!.s()

        return getById(localAccountId)
    }

    override fun deleteLink(linkingAccountManager: String, localAccountId: String, foreignDomainName: String, foreignAccountId: String): Boolean
    {
        val request = DeleteItemRequest.builder()
                .tableName(linksTableName)
                .key(getLinksKey(foreignAccountId, foreignDomainName, linkingAccountManager))
                .build()

        val response = dynamoDBClient.deleteItem(request)

        return response.sdkHttpResponse().isSuccessful
    }

    override fun delete(accountId: String)
    {
        logger.debug("Received request to delete account with accountId: {}", accountId)

        val request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(accountId.toKey("userName"))
                .build()

        dynamoDBClient.deleteItem(request)
    }

    override fun getAll(query: ResourceQuery): ResourceQueryResult
    {
        //TODO should implement pagination and proper handling of DynamoDB pagination

        val requestBuilder = ScanRequest.builder()
                .tableName(tableName)

        if (query.filter != null)
        {
            val parsedFilter = UserAccountFilterParser(query.filter)

            requestBuilder
                    .filterExpression(parsedFilter.parsedFilter)
                    .expressionAttributeValues(parsedFilter.attributeValues)
            if (parsedFilter.attributesNamesMap.isNotEmpty())
            {
                requestBuilder.expressionAttributeNames(parsedFilter.attributesNamesMap)
            }

        }

        if (!query.attributesEnumeration.isNeutral && query.attributesEnumeration is ResourceQuery.Inclusions) {
            requestBuilder.attributesToGet(requiredAttributesToGet + query.attributesEnumeration.attributes)
        }

        if (query.pagination?.count != null) {
            requestBuilder.limit(query.pagination.count.toInt())
        }

        val response = dynamoDBClient.scan(requestBuilder.build())

        val results = mutableListOf<ResourceAttributes<*>>()

        response.items().forEach { item ->
            results.add(item.toAccountAttributes(query.attributesEnumeration))
        }

        return ResourceQueryResult(results, response.count().toLong(), 0, query.pagination.count)
    }

    override fun getAll(startIndex: Long, count: Long): ResourceQueryResult
    {
        logger.debug("Received request to get all accounts with startIndex :{} and count: {}", startIndex, count)

        // TODO: should implement pagination and take into account DynamoDBs possible pagination

        val request = ScanRequest.builder()
                .tableName(tableName)
                .build()

        val response = dynamoDBClient.scan(request)
        val results = mutableListOf<ResourceAttributes<*>>()

        response.items().forEach { item ->
            results.add(item.toAccountAttributes())
        }

        return ResourceQueryResult(results, response.count().toLong(), 0, count)
    }

    private fun getLinksKey(foreignAccountId: String, foreignDomainName: String, linkingAccountManager: String): Map<String, AttributeValue> =
            mapOf(Pair("localIdToforeignIdAtdomainForManager", AttributeValue.builder().s("$foreignAccountId@$foreignDomainName-$linkingAccountManager").build()))

    private fun AccountAttributes.toItem(): MutableMap<String, AttributeValue>
    {
        val item = mutableMapOf<String, AttributeValue>()

        item["userName"] = userName.toAttributeValue()
        item["active"] = isActive.toAttributeValue()
        val now = Instant.now().epochSecond.toString().toAttributeValue()
        item["updated"] = now
        item["created"] = now

        if (!password.isNullOrEmpty())
        {
            item["password"] = password.toAttributeValue()
        }

        if (emails != null)
        {
            val email = emails.primaryOrFirst
            if (email != null && !email.isEmpty)
            {
                item["email"] = email.significantValue.toAttributeValue()
            }
        }

        if (phoneNumbers != null)
        {
            val phone = phoneNumbers.primaryOrFirst
            if (phone != null && !phone.isEmpty)
            {
                item["phone"] = phone.significantValue.toAttributeValue()
            }
        }

        val attributesToPersist = Attributes.of(filter { attribute -> !attributesToRemove.contains(attribute.name.value) })

        if (!attributesToPersist.isEmpty)
        {
            item["attributes"] = jsonHandler.fromAttributes(attributesToPersist).toAttributeValue()
        }

        return item
    }

    private fun Map<String, AttributeValue>.toAccountAttributes(): AccountAttributes = toAccountAttributes(null)
    private fun Map<String, AttributeValue>.toAccountAttributes(attributesEnumeration: ResourceQuery.AttributesEnumeration?): AccountAttributes
    {
        val map = mutableMapOf<String, Any?>()

        map["id"] = this["userName"]?.s()

        forEach {(key, value) ->
            when (key) {
                "active" -> map["active"] = value.bool()
                "email" -> {} // skip, emails are in attributes
                "phone" -> {} // skip, phones are in attributes
                "attributes" -> map.putAll(jsonHandler.fromJson(this["attributes"]?.s()) ?: emptyMap<String, Any>())
                "created" -> {} // skip, this goes to meta
                "updated" -> {} // skip, this goes to meta
                "password" -> {} // do not return passwords
                else -> map[key] = value.s()
            }
        }

        if (attributesEnumeration.includeMeta()) {
            // TODO - should the zone be system default or UTC?
            val zoneId = if (map["timezone"] != null) ZoneId.of(map["timezone"].toString()) else ZoneId.systemDefault()

            map["meta"] = mapOf(
                    Pair(Meta.RESOURCE_TYPE, AccountAttributes.RESOURCE_TYPE),
                    Pair("created", ZonedDateTime.ofInstant(Instant.ofEpochSecond(this["created"]?.s()?.toLong() ?: -1L), zoneId).toString()),
                    Pair("lastModified", ZonedDateTime.ofInstant(Instant.ofEpochSecond(this["updated"]?.s()?.toLong() ?: -1L), zoneId).toString())
            )
        }

        val accountAttributes = AccountAttributes.fromMap(map)

        return if (attributesEnumeration != null) {
            // Most of the attributes are in the "attributes" blob json, so the fields have to be filtered separately here
            AccountAttributes.of(accountAttributes.filter(attributesEnumeration))
        } else {
            accountAttributes
        }
    }



    private fun ResourceQuery.AttributesEnumeration?.includeMeta() =
            this == null || isNeutral || (this is ResourceQuery.Inclusions && attributes.contains("meta")) || (this is ResourceQuery.Exclusions && !attributes.contains("meta"))

    companion object
    {
        private const val tableName = "curity-accounts"
        private const val linksTableName = "curity-links"
        private val logger: Logger = LoggerFactory.getLogger(DynamoDBCredentialDataAccessProvider::class.java)

        private val attributesToRemove = listOf("active", "password", "userName", "id", "schemas")
        private val requiredAttributesToGet = setOf("userName", "attributes", "created", "updated")
    }
}
