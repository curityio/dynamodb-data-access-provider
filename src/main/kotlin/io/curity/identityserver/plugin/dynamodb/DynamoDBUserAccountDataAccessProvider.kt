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
import se.curity.identityserver.sdk.attribute.Attribute
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
import java.lang.UnsupportedOperationException
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

/**
 * The users table has three additional uniqueness restrictions, other than the accountId:
 * - The `username` must be unique.
 * - The optional `email` must be unique.
 * - The optional `phone` must be unique.
 * The users table uses the following design to support these additional uniqueness constraints
 * [https://aws.amazon.com/blogs/database/simulating-amazon-dynamodb-unique-constraints-using-transactions/]
 *
 * Example:
 *
 * | pk                    | version | accountId  | userName | email             | phone     | other attributes
 * | ai#1234               | 12      | 1234       | alice    | alice@example.com | 123456789 | ...      // main item
 * | un#alice              | 12      | 1234       | (absent) | (absent)          | (absent)  | (absent) // secondary item
 * | em#alice@example.com  | 12      | 1234       | (absent) | (absent)          | (absent)  | (absent) // secondary item
 * | pn#123456789          | 12      | 1234       | (absent) | (absent)          | (absent)  | (absent) // secondary item
 *
 * In the following we call "main item" to the item using the `accountId` for the partition key.
 * This is also the item containing all the user account attributes.
 * We call "secondary item" to the items using the `userName`, `email`, and `phone` for the partition key.
 * These secondary items only exist to ensure uniqueness. They don't carry other relevant information.
 *
 * The is a version attribute to support optimistic concurrency when updating or deleting the multiple item from an user
 * on a transaction.
 *
 * The `userName`, `email`, and `phone` attributes:
 * - Are also included in the main item.
 * - Are used as secondary global indexes, to support the `getByNnnn` methods.
 *
 */

class DynamoDBUserAccountDataAccessProvider(
    private val _client: DynamoDBClient,
    configuration: DynamoDBDataAccessProviderConfiguration
) : UserAccountDataAccessProvider
{
    private val jsonHandler = configuration.getJsonHandler()

    override fun getById(
        accountId: String
    ): AccountAttributes? = fromAttributes(getById(accountId, ResourceQuery.Exclusions.none()))

    override fun getById(
        accountId: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>?
    {
        logger.debug("Received request to get account by ID : {}", accountId)

        val requestBuilder = GetItemRequest.builder()
            .tableName(AccountsTable.name)
            .key(AccountsTable.key(accountId))

        val request = requestBuilder.build()

        val response = _client.getItem(request)

        return if (response.hasItem())
        {
            response.item().toAccountAttributes(attributesEnumeration)
        } else
        {
            null
        }
    }

    override fun getByUserName(
        userName: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>?
    {
        logger.debug("Received request to get account by userName : {}", userName)

        return retrieveByIndexQuery(AccountsTable.userNameIndex, userName, attributesEnumeration)
    }

    override fun getByEmail(
        email: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>?
    {
        logger.debug("Received request to get account by email : {}", email)

        return retrieveByIndexQuery(AccountsTable.emailIndex, email, attributesEnumeration)
    }

    override fun getByPhone(
        phone: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>?
    {
        logger.debug("Received request to get account by phone number : {}", phone)

        return retrieveByIndexQuery(AccountsTable.phoneIndex, phone, attributesEnumeration)
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
        val userName = accountAttributes.userName
        item[AccountsTable.pk.name] = AccountsTable.pk.toAttrValue(accountIdPk)
        item[AccountsTable.accountId.name] = AccountsTable.accountId.toAttrValue(accountId)
        item[AccountsTable.version.name] = AccountsTable.version.toAttrValue(0)

        val transactionItems = mutableListOf<TransactWriteItem>()

        // Add main item
        transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(AccountsTable.name)
                    it.conditionExpression("attribute_not_exists(${AccountsTable.pk})")
                    it.item(item)
                }
                .build()
        )

        val accountIdAttr = AccountsTable.accountId.toNameValuePair(accountId)
        val versionAttr = AccountsTable.version.toNameValuePair(0)
        val userNameAttr = AccountsTable.userName.toAttrValue(userName)

        // the item put can only happen if the item does not exist
        val writeConditionExpression = "attribute_not_exists(${AccountsTable.pk.name})"

        // Add secondary item with userName
        transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(AccountsTable.name)
                    it.conditionExpression(writeConditionExpression)
                    it.item(
                        mapOf(
                            AccountsTable.pk.uniqueKeyEntryFor(AccountsTable.userName, userNameAttr.s()),
                            accountIdAttr,
                            versionAttr
                        )
                    )
                }
                .build()
        )

        // Add secondary item with email, if email is present
        item[AccountsTable.email.name]?.also { emailAttr ->
            transactionItems.add(
                TransactWriteItem.builder()
                    .put {
                        it.tableName(AccountsTable.name)
                        it.conditionExpression(writeConditionExpression)
                        it.item(
                            mapOf(
                                AccountsTable.pk.uniqueKeyEntryFor(AccountsTable.email, emailAttr.s()),
                                accountIdAttr,
                                versionAttr
                            )
                        )
                    }
                    .build()
            )
        }

        // Add secondary item with phone, if phone is present
        item[AccountsTable.phone.name]?.also { phoneNumberAttr ->
            transactionItems.add(
                TransactWriteItem.builder()
                    .put {
                        it.tableName(AccountsTable.name)
                        it.conditionExpression(writeConditionExpression)
                        it.item(
                            mapOf(
                                AccountsTable.pk.uniqueKeyEntryFor(AccountsTable.phone, phoneNumberAttr.s()),
                                accountIdAttr,
                                versionAttr
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
                    "Unable to create user with username '${accountAttributes.userName}' as uniqueness check failed"
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

        // Deleting an account requires the deletion of the main item and all the secondary items.
        // A `getItem` is needed to obtain the `userName`, `email`, and `phone` required to compute the
        // secondary item keys.
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
        val version =
            AccountsTable.version.fromOpt(item) ?: throw SchemaErrorException(AccountsTable, AccountsTable.version)
        val userName =
            AccountsTable.userName.fromOpt(item) ?: throw SchemaErrorException(AccountsTable, AccountsTable.userName)
        // email and phone may not exist
        val email = AccountsTable.email.fromOpt(item)
        val phone = AccountsTable.phone.fromOpt(item)

        // Create a transaction with all the items (main and secondary) deletions,
        // conditioned to the version not having changed - optimistic concurrency.
        val transactionItems = mutableListOf<TransactWriteItem>()

        val conditionExpression = newConditionExpression(version, accountId)

        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(AccountsTable.name)
                    it.key(AccountsTable.key(accountId))
                    it.conditionExpression(conditionExpression)
                }
                .build()
        )
        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(AccountsTable.name)
                    it.key(mapOf(AccountsTable.pk.uniqueKeyEntryFor(AccountsTable.userName, userName)))
                    it.conditionExpression(conditionExpression)
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
                        it.conditionExpression(conditionExpression)
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
                        it.conditionExpression(conditionExpression)
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

        // TODO is this really required - the JDBC DAP does it
        return getById(id, attributesEnumeration)
    }

    override fun update(
        accountId: String, map: Map<String, Any>,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>?
    {
        logger.debug("Received request to update account with id:{} and data : {}", accountId, map)

        updateAccount(accountId, AccountAttributes.fromMap(map))

        // TODO is this really required - the JDBC DAP does it
        return getById(accountId, attributesEnumeration)
    }

    private fun updateAccount(accountId: String, accountAttributes: AccountAttributes) =
        retry("updateAccount", N_OF_ATTEMPTS) {
            val observedItem = getItemByAccountId(accountId) ?: return@retry TransactionAttemptResult.Success(null)
            tryUpdateAccount(accountId, accountAttributes, observedItem)
        }

    private fun tryUpdateAccount(accountId: String, accountAttributes: AccountAttributes, observedItem: Item)
            : TransactionAttemptResult<Unit>
    {
        val key = AccountsTable.key(accountId)
        val observedVersion = observedItem.version()
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
            AccountsTable.userName.fromOpt(observedItem),
            accountAttributes.userName
        )

        updateBuilder.handleUniqueAttribute(
            AccountsTable.email,
            AccountsTable.email.fromOpt(observedItem),
            accountAttributes.emails.primaryOrFirst?.significantValue
        )

        updateBuilder.handleUniqueAttribute(
            AccountsTable.phone,
            AccountsTable.phone.fromOpt(observedItem),
            accountAttributes.phoneNumbers.primaryOrFirst?.significantValue
        )

        updateBuilder.handleNonUniqueAttribute(
            AccountsTable.active,
            AccountsTable.active.fromOpt(observedItem),
            accountAttributes.isActive
        )

        val attributesToPersist = serialize(accountAttributes)

        updateBuilder.handleNonUniqueAttribute(
            AccountsTable.attributes,
            AccountsTable.attributes.fromOpt(observedItem),
            attributesToPersist
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
        retry("updateAccount", N_OF_ATTEMPTS)
        {
            val observedItem = getItemByAccountId(accountId) ?: return@retry TransactionAttemptResult.Success(null)
            val observedAttributes = observedItem.toAccountAttributes()

            if (attributeUpdate.attributeAdditions.contains(AccountAttributes.PASSWORD) ||
                attributeUpdate.attributeReplacements.contains(AccountAttributes.PASSWORD)
            )
            {
                logger.info(
                    "Received an account with a password to update. Cannot update passwords using this method, " +
                            "so the password will be ignored."
                )
            }

            var newAttributes = attributeUpdate.applyOn<Attributes>(observedAttributes)
                .with(Attribute.of(ResourceAttributes.ID, accountId))
                .removeAttribute(ResourceAttributes.META)

            if (newAttributes.contains(AccountAttributes.PASSWORD))
            {
                newAttributes = newAttributes.removeAttribute(AccountAttributes.PASSWORD)
            }

            tryUpdateAccount(accountId, fromAttributes(newAttributes), observedItem)
        }

        // TODO is this really required - the JDBC DAP does it
        return getById(accountId, attributesEnumeration)
    }

    private fun getItemByAccountId(accountId: String): Item?
    {
        val requestBuilder = GetItemRequest.builder()
            .tableName(AccountsTable.name)
            .key(AccountsTable.key(accountId))

        val request = requestBuilder.build()

        val response = _client.getItem(request)
        return if (response.hasItem()) response.item() else null
    }

    override fun link(
        linkingAccountManager: String,
        localAccountId: String,
        foreignDomainName: String,
        foreignUserName: String
    )
    {
        val request = PutItemRequest.builder()
            .tableName(linksTableName)
            .item(LinksTable.createItem(linkingAccountManager, localAccountId, foreignDomainName, foreignUserName))
            .build()

        _client.putItem(request)
    }

    override fun listLinks(linkingAccountManager: String, localAccountId: String): Collection<LinkedAccount>
    {
        val request = QueryRequest.builder()
            .tableName(linksTableName)
            .indexName(LinksTable.listLinksIndex.name)
            .keyConditionExpression(LinksTable.listLinksIndex.keyConditionExpression)
            .expressionAttributeValues(
                LinksTable.listLinksIndex.expressionValueMap(
                    localAccountId,
                    linkingAccountManager
                )
            )
            .expressionAttributeNames(LinksTable.listLinksIndex.expressionNameMap)
            .build()

        return querySequence(request, _client)
            .map { item ->
                LinkedAccount.of(
                    LinksTable.linkedAccountDomainName.fromOpt(item),
                    LinksTable.linkedAccountId.fromOpt(item),
                    NO_LINK_DESCRIPTION,
                    LinksTable.created.fromOpt(item).toString()
                )
            }
            .toList()
    }

    override fun resolveLink(
        linkingAccountManager: String,
        foreignDomainName: String,
        foreignAccountId: String
    ): AccountAttributes?
    {
        val request = GetItemRequest.builder()
            .tableName(LinksTable.name)
            .key(mapOf(LinksTable.pk.toNameValuePair(foreignAccountId, foreignDomainName)))
            .build()

        val response = _client.getItem(request)

        if (!response.hasItem() || response.item().isEmpty())
        {
            return null
        }

        val item = response.item()

        val itemAccountManager = LinksTable.linkingAccountManager.fromOpt(item)
            ?: throw SchemaErrorException(LinksTable, LinksTable.linkingAccountManager)

        if (itemAccountManager != linkingAccountManager)
        {
            return null
        }

        val localAccountId = LinksTable.localAccountId.fromOpt(item)
            ?: throw SchemaErrorException(LinksTable, LinksTable.localAccountId)

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
            .tableName(LinksTable.name)
            .key(mapOf(LinksTable.pk.toNameValuePair(foreignAccountId, foreignDomainName)))
            .conditionExpression(
                "${LinksTable.localAccountId.name} = ${LinksTable.localAccountId.colonName} AND "
                        + "${LinksTable.linkingAccountManager.name} = ${LinksTable.linkingAccountManager.colonName}"
            )
            .expressionAttributeValues(
                mapOf(
                    LinksTable.localAccountId.toExpressionNameValuePair(localAccountId),
                    LinksTable.linkingAccountManager.toExpressionNameValuePair(linkingAccountManager)
                )
            )
            .build()

        val response = _client.deleteItem(request)

        return response.sdkHttpResponse().isSuccessful
    }

    override fun getAll(query: ResourceQuery): ResourceQueryResult
    {
        //TODO IS-5851 avoid scan? should implement pagination and proper handling of DynamoDB pagination?

        val requestBuilder = ScanRequest.builder()
            .tableName(tableName)
            // By using the userName index only main entries are considered and the secondary ones are skipped
            // This is possible because the userName index has all the main entries (i.e. all users have userName)
            .indexName(AccountsTable.userNameIndex.name)

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

        val request = ScanRequest.builder()
            .tableName(tableName)
            // By using the userName index only main entries are considered and the secondary ones are skipped
            // This is possible because the userName index has all the main entries (i.e. all users have userName)
            .indexName(AccountsTable.userNameIndex.name)
            .build()

        val response = _client.scan(request)
        if (response.hasLastEvaluatedKey())
        {
            // TODO IS-5851 is this an acceptable behavior?
            throw UnsupportedOperationException("DynamoDB doesn't support getAll on a table that exceeds the maximum page size")
        }

        val results = response.items().asSequence()
            .drop(startIndex.toInt())
            .map { it.toAccountAttributes() }
            .take(count.toInt())
            .toList()

        return ResourceQueryResult(results, response.count().toLong(), startIndex, results.size.toLong())
    }

    private fun AccountAttributes.toItem(): MutableMap<String, AttributeValue>
    {
        val item = mutableMapOf<String, AttributeValue>()

        item.addAttr(AccountsTable.userName, userName)
        item.addAttr(AccountsTable.active, isActive)
        val now = Instant.now().epochSecond
        item.addAttr(AccountsTable.created, now)
        item.addAttr(AccountsTable.updated, now)


        if (!password.isNullOrEmpty())
        {
            item.addAttr(AccountsTable.password, password)
        }

        if (emails != null)
        {
            val email = emails.primaryOrFirst
            if (email != null && !email.isEmpty)
            {
                item.addAttr(AccountsTable.email, email.significantValue)
            }
        }

        if (phoneNumbers != null)
        {
            val phone = phoneNumbers.primaryOrFirst
            if (phone != null && !phone.isEmpty)
            {
                item.addAttr(AccountsTable.phone, phone.significantValue)
            }
        }

        serialize(this)?.let {
            item.addAttr(AccountsTable.attributes, it)
        }

        return item
    }

    private fun serialize(accountAttributes: AccountAttributes): String?
    {
        val filteredAttributes =
            Attributes.of(
                removeLinkedAccounts(accountAttributes)
                    .filter {
                        !attributesToRemove.contains(it.name.value)
                    }
            )

        return if (!filteredAttributes.isEmpty)
        {
            jsonHandler.fromAttributes(filteredAttributes)
        } else
        {
            null
        }
    }

    private fun Map<String, AttributeValue>.toAccountAttributes(): AccountAttributes = toAccountAttributes(null)
    private fun Map<String, AttributeValue>.toAccountAttributes(attributesEnumeration: ResourceQuery.AttributesEnumeration?): AccountAttributes
    {
        val map = mutableMapOf<String, Any?>()

        map["id"] = AccountsTable.accountId.fromOpt(this)

        forEach { (key, value) ->
            when (key)
            {
                AccountsTable.pk.name ->
                { /*ignore*/
                }
                AccountsTable.active.name -> map["active"] = value.bool()
                AccountsTable.email.name ->
                {
                } // skip, emails are in attributes
                AccountsTable.phone.name ->
                {
                } // skip, phones are in attributes
                AccountsTable.attributes.name -> map.putAll(
                    jsonHandler.fromJson(AccountsTable.attributes.fromOpt(this)) ?: emptyMap<String, Any>()
                )
                AccountsTable.created.name ->
                {
                } // skip, this goes to meta
                AccountsTable.updated.name ->
                {
                } // skip, this goes to meta
                AccountsTable.password.name ->
                {
                } // do not return passwords
                else -> map[key] = value.s()
            }
        }

        if (attributesEnumeration.includeMeta())
        {
            val zoneId =
                if (map["timezone"] != null) ZoneId.of(map["timezone"].toString()) else ZoneId.of("UTC")

            map["meta"] = mapOf(
                Pair(Meta.RESOURCE_TYPE, AccountAttributes.RESOURCE_TYPE),
                Pair(
                    "created",
                    ZonedDateTime.ofInstant(Instant.ofEpochSecond(AccountsTable.created.fromOpt(this) ?: -1L), zoneId)
                        .toString()
                ),
                Pair(
                    "lastModified",
                    ZonedDateTime.ofInstant(Instant.ofEpochSecond(AccountsTable.updated.fromOpt(this) ?: -1L), zoneId)
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

        val userNameIndex = Index("userName-index", userName)

        val emailIndex = Index("email-index", email)

        val phoneIndex = Index("phone-index", phone)
    }

    object LinksTable : Table("curity-links")
    {
        val pk =
            StringCompositeAttribute2("linkedAccountId_linkedAccountDomainName") { linkedAccountId, linkedAccountDomainName -> "$linkedAccountId@$linkedAccountDomainName" }
        val localAccountId = StringAttribute("accountId")
        val linkedAccountId = StringAttribute("linkedAccountId")
        val linkedAccountDomainName = StringAttribute("linkedAccountDomainName")
        val linkingAccountManager = StringAttribute("linkingAccountManager")
        val created = NumberLongAttribute("created")

        val listLinksIndex = Index2("list-links-index", localAccountId, linkingAccountManager)

        fun createItem(
            linkingAccountManagerValue: String,
            localAccountIdValue: String,
            foreignDomainNameValue: String,
            foreignUserNameValue: String
        ) = mapOf(
            pk.toNameValuePair(foreignUserNameValue, foreignDomainNameValue),
            localAccountId.toNameValuePair(localAccountIdValue),
            linkedAccountId.toNameValuePair(foreignUserNameValue),
            linkedAccountDomainName.toNameValuePair(foreignDomainNameValue),
            linkingAccountManager.toNameValuePair(linkingAccountManagerValue),
            created.toNameValuePair(Instant.now().epochSecond)
        )
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

        private val attributesToRemove = listOf(
            // these are SDK attribute names and not DynamoDB table attribute names
            "active", "password", "userName", "id", "schemas"
        )

        private val NO_LINK_DESCRIPTION: String? = null

        private const val N_OF_ATTEMPTS = 3

        private fun removeLinkedAccounts(account: AccountAttributes): AccountAttributes
        {
            var withoutLinks = account
            for (linkedAccount in account.linkedAccounts.toList())
            {
                logger.trace(
                    "Removing linked account before persisting to accounts table '{}'",
                    linkedAccount
                )
                withoutLinks = account.removeLinkedAccounts(linkedAccount)
            }
            return withoutLinks.removeAttribute(AccountAttributes.LINKED_ACCOUNTS)
        }

        private fun Item.version(): Long =
            AccountsTable.version.fromOpt(this)
                ?: throw SchemaErrorException(
                    AccountsTable,
                    AccountsTable.version
                )
    }
}

private typealias Item = Map<String, AttributeValue>
