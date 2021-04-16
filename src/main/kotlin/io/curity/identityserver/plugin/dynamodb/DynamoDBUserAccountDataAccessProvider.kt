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
import se.curity.identityserver.sdk.errors.ConflictException
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

class DynamoDBUserAccountDataAccessProvider(
    private val _client: DynamoDBClient,
    configuration: DynamoDBDataAccessProviderConfiguration
) : UserAccountDataAccessProvider
{
    private val jsonHandler = configuration.getJsonHandler()

    override fun getById(
        accountId: String
    ): AccountAttributes = fromAttributes(getById(accountId, ResourceQuery.Exclusions.none()))

    override fun getById(
        accountId: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration?
    ): ResourceAttributes<*>?
    {
        logger.debug("Received request to get account by ID : {}", accountId)

        val requestBuilder = GetItemRequest.builder()
            .tableName(AccountsTable.name)
            .key(AccountsTable.key(accountId))

        if (attributesEnumeration != null && !attributesEnumeration.isNeutral && attributesEnumeration is ResourceQuery.Inclusions)
        {
            requestBuilder.attributesToGet(requiredAttributesToGet + attributesEnumeration.attributes)
        }

        val request = requestBuilder.build()

        val response = _client.getItem(request)

        return if (response.hasItem()) response.item().toAccountAttributes(attributesEnumeration) else null
    }

    override fun getByUserName(
        userName: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration?
    ): ResourceAttributes<*>?
    {
        logger.debug("Received request to get account by userName : {}", userName)

        return retrieveByIndexQuery(AccountsTable.UserNameIndex, userName, attributesEnumeration)
    }

    override fun getByEmail(
        email: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration?
    ): ResourceAttributes<*>?
    {
        logger.debug("Received request to get account by email : {}", email)

        return retrieveByIndexQuery(AccountsTable.EmailIndex, email, attributesEnumeration)
    }

    override fun getByPhone(
        phone: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration?
    ): ResourceAttributes<*>?
    {
        logger.debug("Received request to get account by phone number : {}", phone)

        return retrieveByIndexQuery(AccountsTable.PhoneIndex, phone, attributesEnumeration)
    }

    private fun retrieveByIndexQuery(
        index: Index<String>,
        keyValue: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration?
    ): ResourceAttributes<*>?
    {
        val keyAttribute = index.attribute
        val requestBuilder = QueryRequest.builder()
            .tableName(AccountsTable.name)
            .indexName(index.name)
            .keyConditionExpression("${keyAttribute.hashName} = ${keyAttribute.colonName}")
            .expressionAttributeNames(index.expressionNameMap)
            .expressionAttributeValues(mapOf(keyAttribute.toExpressionNameValuePair(keyValue)))
            .limit(1)

        if (attributesEnumeration != null && !attributesEnumeration.isNeutral && attributesEnumeration is ResourceQuery.Inclusions)
        {
            requestBuilder.attributesToGet(requiredAttributesToGet + attributesEnumeration.attributes)
        }

        val request = requestBuilder.build()

        val response = _client.query(request)

        return if (response.hasItems() && response.items().isNotEmpty())
        {
            response.items().first().toAccountAttributes(attributesEnumeration)
        } else
        {
            null
        }
    }

    override fun create(accountAttributes: AccountAttributes): AccountAttributes
    {
        logger.debug("Received request to create account with data : {}", accountAttributes)
        val item = accountAttributes.toItem()
        val accountId = generateRandomId()
        val accountIdPk = AccountsTable.accountId.uniquenessValueFrom(accountId)
        item[AccountsTable.pk.name] = AccountsTable.pk.toAttrValue(accountIdPk)
        item[AccountsTable.accountId.name] = AccountsTable.accountId.toAttrValue(accountId)
        item[AccountsTable.version.name] = AccountsTable.version.toAttrValue(0)

        val transactionItems = mutableListOf<TransactWriteItem>()

        transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(AccountsTable.name)
                    it.conditionExpression("attribute_not_exists(${AccountsTable.pk})")
                    it.item(item)
                }
                .build()
        )

        val userNameAttr = item[AccountsTable.userName.name] ?: throw Exception("Excepted userName")
        transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(AccountsTable.name)
                    it.conditionExpression("attribute_not_exists(${AccountsTable.pk})")
                    it.item(
                        mapOf(
                            AccountsTable.pk.uniqueKeyEntryFor(AccountsTable.userName, userNameAttr.s()),
                            AccountsTable.accountId.toNameValuePair(accountId),
                            AccountsTable.version.toNameValuePair(0)
                        )
                    )
                }
                .build()
        )

        item[AccountsTable.email.name]?.also { emailAttr ->
            transactionItems.add(
                TransactWriteItem.builder()
                    .put {
                        it.tableName(AccountsTable.name)
                        it.conditionExpression("attribute_not_exists(${AccountsTable.pk})")
                        it.item(
                            mapOf(
                                AccountsTable.pk.uniqueKeyEntryFor(AccountsTable.email, emailAttr.s()),
                                AccountsTable.accountId.toNameValuePair(accountId),
                                AccountsTable.version.toNameValuePair(0)
                            )
                        )
                    }
                    .build()
            )
        }

        item[AccountsTable.phone.name]?.also { phoneNumberAttr ->
            transactionItems.add(
                TransactWriteItem.builder()
                    .put {
                        it.tableName(AccountsTable.name)
                        it.conditionExpression("attribute_not_exists(${AccountsTable.pk})")
                        it.item(
                            mapOf(
                                AccountsTable.pk.uniqueKeyEntryFor(AccountsTable.phone, phoneNumberAttr.s()),
                                AccountsTable.accountId.toNameValuePair(accountId),
                                AccountsTable.version.toNameValuePair(0)
                            )
                        )
                    }
                    .build()
            )
        }

        val request = TransactWriteItemsRequest.builder()
            .transactItems(transactionItems)
            .build()

        try
        {
            _client.transactionWriteItems(request)
        } catch (ex: Exception)
        {
            if (ex.isTransactionCancelledDueToConditionFailure())
            {
                throw ConflictException(
                    "Unable to create user as uniqueness check failed"
                )
            }
            throw ex
        }

        return item.toAccountAttributes()
    }

    override fun delete(accountId: String) = retry("delete", N_OF_ATTEMPTS) { tryDelete(accountId) }

    private fun tryDelete(accountId: String): TransactionAttemptResult<Unit>
    {
        logger.debug("Received request to delete account with accountId: {}", accountId)

        val getItemResponse = _client.getItem(
            GetItemRequest.builder()
                .tableName(AccountsTable.name)
                .key(AccountsTable.key(accountId))
                .build()
        )

        if (!getItemResponse.hasItem())
        {
            return TransactionAttemptResult.Success(Unit)
        }
        val item = getItemResponse.item()
        val version = AccountsTable.version.from(item) ?: throw Exception("TODO")
        val userName = AccountsTable.userName.from(item) ?: throw Exception("TODO")
        val email = AccountsTable.email.from(item)
        val phone = AccountsTable.phone.from(item)

        val transactionItems = mutableListOf<TransactWriteItem>()

        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(AccountsTable.name)
                    it.key(AccountsTable.key(accountId))
                    it.conditionExpression(newConditionExpression(version, accountId))
                }
                .build()
        )
        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(AccountsTable.name)
                    it.key(mapOf(AccountsTable.pk.uniqueKeyEntryFor(AccountsTable.userName, userName)))
                    it.conditionExpression(newConditionExpression(version, accountId))
                }
                .build()
        )
        if (email != null)
        {
            transactionItems.add(
                TransactWriteItem.builder()
                    .delete {
                        it.tableName(AccountsTable.name)
                        it.key(mapOf(AccountsTable.pk.uniqueKeyEntryFor(AccountsTable.email, email)))
                        it.conditionExpression(newConditionExpression(version, accountId))
                    }
                    .build()
            )
        }
        if (phone != null)
        {
            transactionItems.add(
                TransactWriteItem.builder()
                    .delete {
                        it.tableName(AccountsTable.name)
                        it.key(mapOf(AccountsTable.pk.uniqueKeyEntryFor(AccountsTable.phone, phone)))
                        it.conditionExpression(newConditionExpression(version, accountId))
                    }
                    .build()
            )
        }

        val request = TransactWriteItemsRequest.builder()
            .transactItems(transactionItems)
            .build()

        try
        {
            _client.transactionWriteItems(request)
            return TransactionAttemptResult.Success(Unit)
        } catch (ex: Exception)
        {
            if (ex.isTransactionCancelledDueToConditionFailure())
            {
                return TransactionAttemptResult.Failure(
                    ConflictException("Unable to delete user")
                )
            }
            throw ex
        }
    }

    override fun update(
        accountAttributes: AccountAttributes,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>?
    {
        logger.debug("Received request to update account with data : {}", accountAttributes)

        val id = accountAttributes.id
        updateAccount(id, accountAttributes)

        // TODO is this really required
        return getById(id, attributesEnumeration)
    }

    override fun update(
        accountId: String, map: Map<String, Any>,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>?
    {
        logger.debug("Received request to update account with id:{} and data : {}", accountId, map)

        updateAccount(accountId, AccountAttributes.fromMap(map))

        return getByUserName(accountId, attributesEnumeration)
    }

    private fun updateAccount(accountId: String, accountAttributes: AccountAttributes) =
        retry("updateAccount", N_OF_ATTEMPTS) { tryUpdateAccount(accountId, accountAttributes) }

    private fun tryUpdateAccount(accountId: String, accountAttributes: AccountAttributes)
            : TransactionAttemptResult<Unit>
    {
        val key = AccountsTable.key(accountId)
        val getItemResponse = _client.getItem(
            GetItemRequest.builder()
                .tableName(AccountsTable.name)
                .key(key)
                .build()
        )

        if (!getItemResponse.hasItem())
        {
            throw Exception("TODO")
        }

        val item = getItemResponse.item()
        val observedVersion = AccountsTable.version.from(item) ?: throw Exception("TODO")
        val newVersion = observedVersion + 1
        val now = Instant.now().epochSecond

        val updateBuilder = UpdateBuilder(
            AccountsTable,
            key,
            AccountsTable.pk,
            newConditionExpression(observedVersion, accountId),
            newVersion,
            AccountsTable.version,
            arrayOf(
                AccountsTable.version.toNameValuePair(newVersion),
                AccountsTable.accountId.toNameValuePair(accountId)
            )

        )

        updateBuilder.handleUniqueAttribute(
            AccountsTable.userName,
            AccountsTable.userName.from(item),
            accountAttributes.userName
        )

        updateBuilder.handleUniqueAttribute(
            AccountsTable.email,
            AccountsTable.email.from(item),
            accountAttributes.emails.primaryOrFirst?.significantValue
        )

        updateBuilder.handleUniqueAttribute(
            AccountsTable.phone,
            AccountsTable.phone.from(item),
            accountAttributes.phoneNumbers.primaryOrFirst?.significantValue
        )

        updateBuilder.handleNonUniqueAttribute(
            AccountsTable.active,
            AccountsTable.active.from(item),
            accountAttributes.isActive
        )

        val attributesToPersist =
            Attributes.of(accountAttributes.filter { attribute -> !attributesToRemove.contains(attribute.name.value) })

        updateBuilder.handleNonUniqueAttribute(
            AccountsTable.attributes,
            AccountsTable.attributes.from(item),
            jsonHandler.fromAttributes(attributesToPersist)
        )

        updateBuilder.handleNonUniqueAttribute(
            AccountsTable.updated,
            null,
            now
        )

        updateBuilder.handleNonUniqueAttribute(
            AccountsTable.version,
            observedVersion,
            observedVersion + 1
        )

        try
        {
            _client.transactionWriteItems(updateBuilder.build())
            return TransactionAttemptResult.Success(Unit)
        } catch (ex: Exception)
        {
            if (ex.isTransactionCancelledDueToConditionFailure())
            {
                return TransactionAttemptResult.Failure(ex)
            }
            throw ex
        }
    }

    override fun patch(
        accountId: String, attributeUpdate: AttributeUpdate,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>?
    {
        logger.debug("Received patch request with accountId:{} and data : {}", accountId, attributeUpdate)

        // TODO - updating values and removing fields should read data from db and compare
        // all fields, as some of the data can be set in the "attributes" blob. Currently the
        // implementation is naive and assumes we're only editing/removing root attributes which
        // live in their own columns (not in the "attributes" blob)

        if (!attributeUpdate.attributeAdditions.isEmpty || !attributeUpdate.attributeReplacements.isEmpty)
        {
            updateAccount(
                accountId,
                AccountAttributes.of(attributeUpdate.attributeAdditions + attributeUpdate.attributeReplacements)
            )
        }

        if (attributeUpdate.attributeDeletions.attributeNamesToDelete.isNotEmpty())
        {
            val request = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(accountId.toKey("userName"))
                .updateExpression("REMOVE ${attributeUpdate.attributeDeletions.attributeNamesToDelete.joinToString(", ")}")
                .build()

            _client.updateItem(request)
        }

        return getByUserName(accountId, attributesEnumeration)
    }

    override fun link(
        linkingAccountManager: String,
        localAccountId: String,
        foreignDomainName: String,
        foreignUserName: String
    )
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
        item["localIdToforeignIdAtdomainForManager"] =
            "$foreignUserName@$foreignDomainName-$linkingAccountManager".toAttributeValue()
        item["created"] = Instant.now().epochSecond.toString().toAttributeValue()

        val request = PutItemRequest.builder()
            .tableName(linksTableName)
            .item(item)
            .build()

        _client.putItem(request)
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

        val response = _client.query(request)

        return if (response.hasItems() && response.items().isNotEmpty())
        {
            response.items().map { item ->
                LinkedAccount.of(
                    item["foreignDomainName"]?.s(),
                    item["foreignAccountId"]?.s(),
                    "",
                    item["created"]?.s()
                )
            }
        } else
        {
            emptyList()
        }
    }

    override fun resolveLink(
        linkingAccountManager: String,
        foreignDomainName: String,
        foreignAccountId: String
    ): AccountAttributes?
    {
        val request = GetItemRequest.builder()
            .tableName(linksTableName)
            .key(getLinksKey(foreignAccountId, foreignDomainName, linkingAccountManager))
            .build()

        val response = _client.getItem(request)

        if (!response.hasItem() || response.item().isEmpty())
        {
            return null
        }

        val localAccountId = response.item()["localAccountId"]!!.s()

        return getById(localAccountId)
    }

    override fun deleteLink(
        linkingAccountManager: String,
        localAccountId: String,
        foreignDomainName: String,
        foreignAccountId: String
    ): Boolean
    {
        val request = DeleteItemRequest.builder()
            .tableName(linksTableName)
            .key(getLinksKey(foreignAccountId, foreignDomainName, linkingAccountManager))
            .build()

        val response = _client.deleteItem(request)

        return response.sdkHttpResponse().isSuccessful
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

        if (!query.attributesEnumeration.isNeutral && query.attributesEnumeration is ResourceQuery.Inclusions)
        {
            requestBuilder.attributesToGet(requiredAttributesToGet + query.attributesEnumeration.attributes)
        }

        if (query.pagination?.count != null)
        {
            requestBuilder.limit(query.pagination.count.toInt())
        }

        val response = _client.scan(requestBuilder.build())

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

        val response = _client.scan(request)
        val results = mutableListOf<ResourceAttributes<*>>()

        response.items().forEach { item ->
            results.add(item.toAccountAttributes())
        }

        return ResourceQueryResult(results, response.count().toLong(), 0, count)
    }

    private fun getLinksKey(
        foreignAccountId: String,
        foreignDomainName: String,
        linkingAccountManager: String
    ): Map<String, AttributeValue> =
        mapOf(
            Pair(
                "localIdToforeignIdAtdomainForManager",
                AttributeValue.builder().s("$foreignAccountId@$foreignDomainName-$linkingAccountManager").build()
            )
        )

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

        val attributesToPersist =
            Attributes.of(filter { attribute -> !attributesToRemove.contains(attribute.name.value) })

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

        map["id"] = this["accountId"]?.s()

        forEach { (key, value) ->
            when (key)
            {
                AccountsTable.pk.name ->
                { /*ignore*/
                }
                "active" -> map["active"] = value.bool()
                "email" ->
                {
                } // skip, emails are in attributes
                "phone" ->
                {
                } // skip, phones are in attributes
                "attributes" -> map.putAll(jsonHandler.fromJson(this["attributes"]?.s()) ?: emptyMap<String, Any>())
                "created" ->
                {
                } // skip, this goes to meta
                "updated" ->
                {
                } // skip, this goes to meta
                "password" ->
                {
                } // do not return passwords
                else -> map[key] = value.s()
            }
        }

        if (attributesEnumeration.includeMeta())
        {
            // TODO - should the zone be system default or UTC?
            val zoneId = if (map["timezone"] != null) ZoneId.of(map["timezone"].toString()) else ZoneId.systemDefault()

            map["meta"] = mapOf(
                Pair(Meta.RESOURCE_TYPE, AccountAttributes.RESOURCE_TYPE),
                Pair(
                    "created",
                    ZonedDateTime.ofInstant(Instant.ofEpochSecond(this["created"]?.s()?.toLong() ?: -1L), zoneId)
                        .toString()
                ),
                Pair(
                    "lastModified",
                    ZonedDateTime.ofInstant(Instant.ofEpochSecond(this["updated"]?.s()?.toLong() ?: -1L), zoneId)
                        .toString()
                )
            )
        }

        val accountAttributes = AccountAttributes.fromMap(map)

        return if (attributesEnumeration != null)
        {
            // Most of the attributes are in the "attributes" blob json, so the fields have to be filtered separately here
            AccountAttributes.of(accountAttributes.filter(attributesEnumeration))
        } else
        {
            accountAttributes
        }
    }

    private fun ResourceQuery.AttributesEnumeration?.includeMeta() =
        this == null || isNeutral || (this is ResourceQuery.Inclusions && attributes.contains("meta")) || (this is ResourceQuery.Exclusions && !attributes.contains(
            "meta"
        ))

    object AccountsTable : Table("curity-accounts")
    {
        val pk = KeyStringAttribute("pk")
        val accountId = UniqueStringAttribute("accountId") { value -> "ai#$value" }
        val version = NumberLongAttribute("version")
        val userName = UniqueStringAttribute("userName") { value -> "un#$value" }
        val email = UniqueStringAttribute("email") { value -> "em#$value" }
        val phone = UniqueStringAttribute("phone") { value -> "pn#$value" }
        val password = StringAttribute("password")
        val active = BooleanAttribute("active")
        val attributes = StringAttribute("attributes")
        val created = NumberLongAttribute("created")
        val updated = NumberLongAttribute("updated")

        fun key(accountIdValue: String) = mapOf(
            pk.name to pk.toAttrValue(accountId.uniquenessValueFrom(accountIdValue))
        )

        object UserNameIndex : Index<String>("userName-index", userName)

        object EmailIndex : Index<String>("email-index", email)

        object PhoneIndex : Index<String>("phone-index", phone)
    }

    private fun newConditionExpression(version: Long, accountId: String) = object : Expression(
        _conditionExpressionBuilder
    )
    {
        override val values = mapOf(
            ":oldVersion" to AccountsTable.version.toAttrValue(version),
            AccountsTable.accountId.toExpressionNameValuePair(accountId)
        )
    }

    companion object
    {

        private val _conditionExpressionBuilder = ExpressionBuilder(
            "#version = :oldVersion AND #accountId = :accountId",
            AccountsTable.version, AccountsTable.accountId
        )

        private const val tableName = "curity-accounts"
        private const val linksTableName = "curity-links"
        private val logger: Logger = LoggerFactory.getLogger(DynamoDBCredentialDataAccessProvider::class.java)

        private val attributesToRemove = listOf("active", "password", "userName", "id", "schemas")
        private val requiredAttributesToGet = setOf("userName", "attributes", "created", "updated")

        private const val N_OF_ATTEMPTS = 10;
    }
}


