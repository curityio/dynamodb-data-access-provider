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

import io.curity.identityserver.plugin.dynamodb.DynamoDBUserAccountDataAccessProvider.Companion.UNIQUE_ACCOUNT_PHONE_NUMBER
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import io.curity.identityserver.plugin.dynamodb.query.BinaryAttributeExpression
import io.curity.identityserver.plugin.dynamodb.query.BinaryAttributeOperator
import io.curity.identityserver.plugin.dynamodb.query.DisjunctiveNormalForm
import io.curity.identityserver.plugin.dynamodb.query.DynamoDBQueryBuilder
import io.curity.identityserver.plugin.dynamodb.query.Index
import io.curity.identityserver.plugin.dynamodb.query.Product
import io.curity.identityserver.plugin.dynamodb.query.QueryHelper
import io.curity.identityserver.plugin.dynamodb.query.QueryPlan
import io.curity.identityserver.plugin.dynamodb.query.QueryPlan.RangeCondition
import io.curity.identityserver.plugin.dynamodb.query.QueryPlanner
import io.curity.identityserver.plugin.dynamodb.query.TableQueryCapabilities
import io.curity.identityserver.plugin.dynamodb.query.UnsupportedQueryException
import io.curity.identityserver.plugin.dynamodb.query.filterWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import org.slf4j.MarkerFactory
import se.curity.identityserver.sdk.attribute.AccountAttributes
import se.curity.identityserver.sdk.attribute.Attribute
import se.curity.identityserver.sdk.attribute.AttributeValue.formatAsStringAttributeValue
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.CredentialAttributes
import se.curity.identityserver.sdk.attribute.SubjectAttributes
import se.curity.identityserver.sdk.attribute.scim.v2.Meta
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.LinkedAccount
import se.curity.identityserver.sdk.data.query.ResourceQuery
import se.curity.identityserver.sdk.data.query.ResourceQueryResult
import se.curity.identityserver.sdk.data.update.AttributeUpdate
import se.curity.identityserver.sdk.datasource.CredentialDataAccessProviderFactory
import se.curity.identityserver.sdk.datasource.CredentialManagementDataAccessProvider
import se.curity.identityserver.sdk.datasource.CredentialStoringDataAccessProvider
import se.curity.identityserver.sdk.datasource.CredentialStoringDataAccessProvider.GetResult
import se.curity.identityserver.sdk.datasource.PageableUserAccountDataAccessProvider
import se.curity.identityserver.sdk.datasource.UserAccountDataAccessProvider
import se.curity.identityserver.sdk.datasource.db.TableCapabilities.TableCapability
import se.curity.identityserver.sdk.datasource.errors.DataSourceCapabilityException
import se.curity.identityserver.sdk.datasource.pagination.PaginatedDataAccessResult
import se.curity.identityserver.sdk.datasource.pagination.PaginationRequest
import se.curity.identityserver.sdk.datasource.query.AttributesFiltering
import se.curity.identityserver.sdk.datasource.query.AttributesSorting
import se.curity.identityserver.sdk.errors.ConflictException
import se.curity.identityserver.sdk.service.authentication.TenantId
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.Select
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.UUID

/**
 * The users table has three additional uniqueness restrictions, other than the accountId:
 * - The `username` must be unique.
 * - The optional `email` must be unique.
 * - The optional `phone` must be unique (only when [UNIQUE_ACCOUNT_PHONE_NUMBER] is true, which is the default).
 * The users table uses the following design to support these additional uniqueness constraints
 * [https://aws.amazon.com/blogs/database/simulating-amazon-dynamodb-unique-constraints-using-transactions/],
 * which is based on adding multiple items per "entity", which in this case is an account.
 *
 * Also, getting by `userName`, and `email` should use reads with strong consistency guarantees.
 * This is also the case for `phone` number when [UNIQUE_ACCOUNT_PHONE_NUMBER] is true.
 * Since strong consistency reads are not supported by Global Secondary Indexes, the design used here replicates all the data
 * into all the items inserted for the same entity.
 * The cost should be similar to having the indexes, because those also replicate the data.
 *
 * Example:
 *
 * | pk                    | version | accountId  | userName | email             | phone     | other attributes
 * | ai#1234               | 12      | 1234       | alice    | alice@example.com | 123456789 | ...      // main item
 * | un#alice              | 12      | 1234       | alice    | alice@example.com | 123456789 | ...      // secondary item
 * | em#alice@example.com  | 12      | 1234       | alice    | alice@example.com | 123456789 | ...      // secondary item
 * | pn#123456789          | 12      | 1234       | alice    | alice@example.com | 123456789 | ...      // secondary item only when [UNIQUE_ACCOUNT_PHONE_NUMBER] true
 *
 * In the following we call "main item" the item using the `accountId` for the partition key.
 * We call "secondary item" the items using the `userName`, `email`, and `phone` for the partition key.
 * Both the main item and the secondary items carry all the information.
 * - This means that a `getByEmail` can use the email secondary item without needing an additional index and therefore
 * allowing for strong consistent reads.
 * - However, this also means that any change to an account must be reflected in the main item and all the secondary items
 * (account updates are not frequent).
 *
 * There is a version attribute to support optimistic concurrency when updating or deleting the multiple item from a user
 * on a transaction.
 *
 * ## Multi-tenancy in curity-accounts table:
 *
 * To allow any username, phone number, email, account ID to exist in different tenants, the pk includes the tenant ID.
 * The following table describes the format of the primary key for the default or custom tenant.
 * | item type | default tenant format  | custom tenant format
 * | accountId | ai#1234                | ai(tenant1)#1234
 * | username  | un#johndoe             | un(tenant1)#johndoe
 * | phone     | pn#+46123456789        | pn(tenant1)#+46123456789
 * | email     | em#johndoe@example.com | em(tenant1)#johndoe@example.com
 *
 * Moreover, the following table describes the format of the initial attributes for the default or custom tenant.
 * | initial attribute | value               | default tenant format  | custom tenant format
 * | userNameInitial   | johndoe             | joh                    | un(tenant1)#joh
 * | emailInitial      | johndoe@example.com | joh                    | em(tenant1)#joh
 *
 * Note that when an account is owned by a custom tenant, a `tenantId` attribute is persisted with the account main and
 * secondary items. This may help filtering all items for a given tenant. Accounts owned by default tenant do not have
 * a `tenantId` attribute.
 *
 * ## Multi-tenancy in curity-links table:
 *
 * The partition key for a linked account is formatted as follow to enforce uniqueness :
 * - for default tenant: `{linkedAccountId}@{linkedAccountDomainName}` , e.g. `johndoe@facebook`
 * - for custom tenant: `{linkedAccountId}@{linkedAccountDomainName}.{tenantId}', e.g. `johndoe@facebook.tenant1`
 *
 * Note that when a link is owned by a custom tenant, a `tenantId` attribute is persisted with the link item.
 * This may help filtering all items for a given tenant. Links owned by default tenant do not have
 * a `tenantId` attribute.
 *
 */
class DynamoDBUserAccountDataAccessProvider(
    private val _dynamoDBClient: DynamoDBClient,
    private val _configuration: DynamoDBDataAccessProviderConfiguration
) : UserAccountDataAccessProvider, PageableUserAccountDataAccessProvider,
    CredentialDataAccessProviderFactory,
    CredentialStoringDataAccessProvider,
    CredentialStoringDataAccessProvider.CredentialAttributesUpdater,
    CredentialManagementDataAccessProvider.SubjectUpdater {
    private val jsonHandler = _configuration.getJsonHandler()

    // Lazy initialization is required to avoid cyclic dependencies while Femto containers are built.
    // TenantId should not be resolved from the configuration at DAP initialization time.
    private val _tenantId: TenantId by lazy {
        _configuration.getTenantId()
    }

    companion object {
        /**
         * As explained above, by default the phone number of an account must be unique among all accounts of the table,
         * and leads to the creation of a secondary item. However, it is possible to lift this restriction by setting this
         * system property to ``false`` on all nodes.
         *
         * Once done, it will be possible to have a given phone number shared by more than one of the accounts created
         * after this change. But it will no longer be possible to request accounts using [getByPhone], ``null`` will
         * be systematically returned.
         */
        const val UNIQUE_ACCOUNT_PHONE_NUMBER_PROPERTY =
            "se.curity:identity-server.plugin.dynamodb:unique-account-phone-number"
        private val UNIQUE_ACCOUNT_PHONE_NUMBER =
            System.getProperty(UNIQUE_ACCOUNT_PHONE_NUMBER_PROPERTY, "true").toBoolean()

        private val _conditionExpressionBuilder = ExpressionBuilder(
            "#version = :oldVersion AND #accountId = :accountId",
            AccountsTable.version, AccountsTable.accountId
        )

        private val _logger: Logger = LoggerFactory.getLogger(DynamoDBUserAccountDataAccessProvider::class.java)
        private val MASK_MARKER: Marker = MarkerFactory.getMarker("MASK")

        private val attributesToRemove = listOf(
            // these are SDK attribute names and not DynamoDB table attribute names
            "active", "password", "userName", "id", "schemas"
        )

        private val NO_LINK_DESCRIPTION: String? = null

        private const val N_OF_ATTEMPTS = 3

        /**
         * Length of the "initial" for userName and email attributes.
         * Since the initial attribute is used as the partition key, it is set to a value greater than one
         * in order to well balance the partitions for increased performance and
         * decreased DynamoDB read capacity unit cost.
         * Warning: Changing this value is a breaking change: the indexes will need to be rebuilt.
         */
        const val INITIAL_LENGTH = 3

        const val GETALLBY_OPERATION = "getAllBy"
        const val COUNTBY_OPERATION = "countBy"

        private fun removeLinkedAccounts(account: AccountAttributes): AccountAttributes {
            var withoutLinks = account
            for (linkedAccount in account.linkedAccounts.toList()) {
                _logger.trace(
                    MASK_MARKER,
                    "Removing linked account before persisting to accounts table '{}'",
                    linkedAccount
                )
                withoutLinks = account.removeLinkedAccounts(linkedAccount)
            }
            return withoutLinks.removeAttribute(AccountAttributes.LINKED_ACCOUNTS)
        }

        private fun DynamoDBItem.version(): Long =
            AccountsTable.version.optionalFrom(this)
                ?: throw SchemaErrorException(
                    AccountsTable,
                    AccountsTable.version
                )

        private fun DynamoDBItem.accountId(): String =
            AccountsTable.accountId.optionalFrom(this)
                ?: throw SchemaErrorException(
                    AccountsTable,
                    AccountsTable.accountId
                )

        private fun getFilterForItemsWithAccountIdPk(tenantId: TenantId) = DynamoDBScan(
            filterExpression = "begins_with(pk, :pkPrefix)",
            valueMap = mapOf(
                ":pkPrefix" to AttributeValue.builder().s(AccountsTable.accountId.toValuePrefix(tenantId)).build()
            ),
            nameMap = mapOf()
        )

        private const val MAX_QUERIES = 8
    }

    private val tableName = AccountsTable.name(_configuration)

    override fun getById(
        accountId: String
    ): AccountAttributes? = fromAttributes(getById(accountId, ResourceQuery.Exclusions.none()))

    override fun getById(
        accountId: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>? {
        _logger.debug(MASK_MARKER, "Received request to get account by ID : {}", accountId)

        return retrieveByUniqueAttribute(AccountsTable.accountId, accountId, attributesEnumeration)
    }

    override fun getByUserName(
        userName: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>? {
        _logger.debug(MASK_MARKER, "Received request to get account by userName : {}", userName)

        return retrieveByUniqueAttribute(AccountsTable.userName, userName, attributesEnumeration)
    }

    override fun getByEmail(
        email: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>? {
        _logger.debug(MASK_MARKER, "Received request to get account by email : {}", email)

        return retrieveByUniqueAttribute(AccountsTable.email, email, attributesEnumeration)
    }

    override fun getByPhone(
        phone: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>? {
        _logger.debug(MASK_MARKER, "Received request to get account by phone number : {}", phone)

        return if (UNIQUE_ACCOUNT_PHONE_NUMBER) {
            retrieveByUniqueAttribute(AccountsTable.phone, phone, attributesEnumeration)
        } else {
            _logger.debug("Returning null as '{}' is false", UNIQUE_ACCOUNT_PHONE_NUMBER_PROPERTY)
            null
        }
    }

    private fun retrieveByUniqueAttribute(
        key: TenantAwareUniqueAttribute,
        keyValue: String,
        attributesEnumeration: ResourceQuery.AttributesEnumeration?
    ): ResourceAttributes<*>? {
        val requestBuilder = GetItemRequest.builder()
            .tableName(tableName)
            .key(AccountsTable.keyFrom(key, _tenantId, keyValue))
            .consistentRead(true)

        val request = requestBuilder.build()

        val response = _dynamoDBClient.getItem(request)

        return if (response.hasItem()) {
            response.item().toAccountAttributes(attributesEnumeration)
        } else {
            null
        }
    }

    override fun create(accountAttributes: AccountAttributes): AccountAttributes {
        _logger.debug(MASK_MARKER, "Received request to create account with data : {}", accountAttributes)

        if (accountAttributes.password != null) {
            _logger.warn(
                "Account creation data includes a password, which isn't the preferred way to store passwords. " +
                        "Callers should be updated to use the appropriate Credential Management services."
            )
        }


        val accountId = UUID.randomUUID().toString()
        val now = Instant.now().epochSecond

        // the commonItem contains the attributes that will be on both the primary and secondary items.
        val commonItem = accountAttributes.toItem(accountId, now, now)

        val userName = accountAttributes.userName
        commonItem.addAttr(AccountsTable.version, 0)

        val transactionItems = mutableListOf<TransactWriteItem>()

        // the item put can only happen if the item does not exist
        val writeConditionExpression = "attribute_not_exists(${AccountsTable.pk.name})"

        // Add main item
        // Fill the initials for userName and email (if present) only for the main item,
        // to build a partial Global Secondary Index on these attributes.
        val mainItemAdditionalAttributes = mutableMapOf(
            AccountsTable.pk.uniqueKeyEntryFor(AccountsTable.accountId, _tenantId, accountId),
            AccountsTable.userNameInitial.uniqueKeyEntryFor(_tenantId, userName)
        )

        commonItem[AccountsTable.email.name]?.also {
            AccountsTable.emailInitial.uniqueKeyEntryFor(_tenantId, it.s()).also { pair ->
                mainItemAdditionalAttributes[pair.first] = pair.second
            }
        }

        addTransactionItem(
            commonItem,
            mainItemAdditionalAttributes,
            transactionItems, writeConditionExpression
        )

        // Add secondary item with userName
        addTransactionItem(
            commonItem,
            AccountsTable.keyFrom(AccountsTable.userName, _tenantId, userName),
            transactionItems, writeConditionExpression
        )

        // Add secondary item with email, if email is present
        commonItem[AccountsTable.email.name]?.also { emailAttr ->
            addTransactionItem(
                commonItem,
                AccountsTable.keyFrom(AccountsTable.email, _tenantId, emailAttr.s()),
                transactionItems, writeConditionExpression
            )
        }

        if (UNIQUE_ACCOUNT_PHONE_NUMBER) {
            // Add secondary item with phone, if phone is present
            commonItem[AccountsTable.phone.name]?.also { phoneNumberAttr ->
                addTransactionItem(
                    commonItem,
                    AccountsTable.keyFrom(AccountsTable.phone, _tenantId, phoneNumberAttr.s()),
                    transactionItems, writeConditionExpression
                )
            }
        }

        val request = TransactWriteItemsRequest.builder()
            .transactItems(transactionItems)
            .build()

        try {
            _dynamoDBClient.transactionWriteItems(request)
        } catch (ex: Exception) {
            if (ex.isTransactionCancelledDueToConditionFailure()) {
                val exceptionCause = ex.cause
                if (exceptionCause is TransactionCanceledException) {
                    ex.validateKnownUniqueConstraintsForAccountMutations(
                        exceptionCause.cancellationReasons(),
                        transactionItems
                    )
                } else {
                    throw ConflictException(
                        "Unable to create user with username '${accountAttributes.userName}' as uniqueness check failed"
                    )
                }
            }
            throw ex
        }

        val mainItem = commonItem + mainItemAdditionalAttributes
        return mainItem.toAccountAttributes()
    }

    private fun addTransactionItem(
        commonItem: MutableMap<String, AttributeValue>,
        itemAttributes: Map<String, AttributeValue>,
        transactionItems: MutableList<TransactWriteItem>,
        writeConditionExpression: String
    ) {
        val item = commonItem + itemAttributes
        transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(tableName)
                    it.conditionExpression(writeConditionExpression)
                    it.item(item)
                }
                .build()
        )
    }

    override fun delete(accountId: String) = retry("delete", N_OF_ATTEMPTS) { tryDelete(accountId) }

    override fun getAllBy(
        activeAccountsOnly: Boolean,
        paginationRequest: PaginationRequest?,
        sortRequest: AttributesSorting?,
        filterRequest: AttributesFiltering?
    ): PaginatedDataAccessResult<AccountAttributes> {
        validateGetAllByRequest(GETALLBY_OPERATION, sortRequest, filterRequest)

        val (items, cursor) = when (val queryPlan = buildQueryPlan(filterRequest, activeAccountsOnly)) {
            is QueryPlan.UsingQueries -> query(queryPlan, sortRequest, paginationRequest)
            is QueryPlan.UsingScan -> scan(queryPlan, paginationRequest)
        }

        return PaginatedDataAccessResult(items.map { it.toAccountAttributes() }.toList(), cursor)
    }

    override fun getCountBy(activeAccountsOnly: Boolean, filterRequest: AttributesFiltering?): Long {
        validateGetAllByRequest(COUNTBY_OPERATION, null, filterRequest)

        return when (val queryPlan = buildQueryPlan(filterRequest, activeAccountsOnly)) {
            is QueryPlan.UsingQueries -> queryCount(queryPlan)
            is QueryPlan.UsingScan -> scanCount(queryPlan)
        }
    }

    private fun validateGetAllByRequest(
        operation: String,
        sortRequest: AttributesSorting?,
        filterRequest: AttributesFiltering?,
    ) {
        checkGetAllBySupported(operation)

        QueryHelper.validateRequest(
            filterRequest.useScan(), _configuration.getAllowTableScans(),
            AccountsTable.queryCapabilities, filterRequest, sortRequest
        )

        if (filterRequest?.filter != null && filterRequest.filter.length < INITIAL_LENGTH) {
            throw DataSourceCapabilityException(
                TableCapability.FILTERING_FILTER_TOO_SMALL,
                TableCapability.FILTERING_FILTER_TOO_SMALL.unsupportedMessage
            )
        }
    }

    private fun checkGetAllBySupported(operation: String) {
        val featureId = DynamoDBSecondaryIndexFeatureCheck.buildFeatureId(
            AccountsTable.name(_configuration),
            AccountsTable.userNameInitialUserNameIndex
        )
        if (!_dynamoDBClient.supportsFeature(featureId)) {
            _logger.info(
                "User account data source '{}' does not support '{}' because feature {} is missing",
                this, operation, featureId
            )
            throw UnsupportedOperationException(operation)
        }
    }

    private fun AttributesFiltering?.useScan(): Boolean =
        this == null || this.filterBy.isNullOrEmpty() || this.filter.isNullOrEmpty()

    private fun buildQueryPlan(
        filterRequest: AttributesFiltering?, activeAccountsOnly: Boolean
    ): QueryPlan {
        val activeAccountsOnlyProduct = if (activeAccountsOnly) {
            DisjunctiveNormalForm(
                setOf(
                    Product.of(
                        BinaryAttributeExpression(
                            AccountsTable.active,
                            BinaryAttributeOperator.Eq,
                            true // only active accounts
                        )
                    )
                )
            )
        } else {
            DisjunctiveNormalForm(setOf())
        }

        if (filterRequest == null || filterRequest.useScan()) {
            return QueryPlan.UsingScan(activeAccountsOnlyProduct)
        }

        val filterAttribute = AccountsTable.queryCapabilities.attributeMap[filterRequest.filterBy]
            ?: throw IllegalArgumentException(
                "Filtering using the ${filterRequest.filterBy} attribute is not supported in DynamoDB."
            )
        // Search for an index implementing the StartsWith pattern for this filterBy attribute.
        val index = AccountsTable.queryCapabilities.indexes.asSequence()
            .filter {
                it.partitionAttribute is StartsWithStringAttribute
                        && it.partitionAttribute.canBeUsedOnQueryTo(filterAttribute)
            }
            .first()
        val startsWithAttribute = index.partitionAttribute as StartsWithStringAttribute
        val keyCondition = QueryPlan.KeyCondition(
            index,
            BinaryAttributeExpression(
                startsWithAttribute, BinaryAttributeOperator.Eq,
                startsWithAttribute.toInitials(_tenantId, filterRequest.filter)
            ),
            RangeCondition.Binary(
                BinaryAttributeExpression(
                    startsWithAttribute.fullAttribute, BinaryAttributeOperator.Sw,
                    startsWithAttribute.fullAttribute.toAttrValue(filterRequest.filter).s()
                )
            )
        )

        return QueryPlan.UsingQueries(mapOf(keyCondition to activeAccountsOnlyProduct.products.toList()))
    }

    private fun tryDelete(accountId: String): TransactionAttemptResult<Unit> {
        _logger.debug(MASK_MARKER, "Received request to delete account with accountId: {}", accountId)

        // Deleting an account requires the deletion of the main item and all the secondary items.
        // A `getItem` is needed to obtain the `userName`, `email`, and `phone` required to compute the
        // secondary item keys.
        val getItemResponse = _dynamoDBClient.getItem(
            GetItemRequest.builder()
                .tableName(tableName)
                .key(AccountsTable.keyFromAccountId(accountId, _tenantId))
                .build()
        )

        if (!getItemResponse.hasItem() || getItemResponse.item().isEmpty()) {
            return TransactionAttemptResult.Success(Unit)
        }

        val item = getItemResponse.item()
        val version =
            AccountsTable.version.optionalFrom(item)
                ?: throw SchemaErrorException(AccountsTable, AccountsTable.version)
        val userName =
            AccountsTable.userName.optionalFrom(item) ?: throw SchemaErrorException(
                AccountsTable,
                AccountsTable.userName
            )
        // email and phone may not exist
        val email = AccountsTable.email.optionalFrom(item)
        val phone = AccountsTable.phone.optionalFrom(item)

        // Create a transaction with all the items (main and secondary) deletions,
        // conditioned to the version not having changed - optimistic concurrency.
        val transactionItems = mutableListOf<TransactWriteItem>()

        val conditionExpression = versionAndAccountIdConditionExpression(version, accountId)

        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(tableName)
                    it.key(AccountsTable.keyFromAccountId(accountId, _tenantId))
                    it.conditionExpression(conditionExpression)
                }
                .build()
        )
        transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(tableName)
                    it.key(AccountsTable.keyFrom(AccountsTable.userName, _tenantId, userName))
                    it.conditionExpression(conditionExpression)
                }
                .build()
        )
        if (email != null) {
            transactionItems.add(
                TransactWriteItem.builder()
                    .delete {
                        it.tableName(tableName)
                        it.key(AccountsTable.keyFrom(AccountsTable.email, _tenantId, email))
                        it.conditionExpression(conditionExpression)
                    }
                    .build()
            )
        }
        if ((phone != null) && UNIQUE_ACCOUNT_PHONE_NUMBER) {
            transactionItems.add(
                TransactWriteItem.builder()
                    .delete {
                        it.tableName(tableName)
                        it.key(AccountsTable.keyFrom(AccountsTable.phone, _tenantId, phone))
                        it.conditionExpression(conditionExpression)
                    }
                    .build()
            )
        }

        val request = TransactWriteItemsRequest.builder()
            .transactItems(transactionItems)
            .build()

        try {
            _dynamoDBClient.transactionWriteItems(request)
            return TransactionAttemptResult.Success(Unit)
        } catch (ex: Exception) {
            if (ex.isTransactionCancelledDueToConditionFailure()) {
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
    ): ResourceAttributes<*>? {
        _logger.debug(MASK_MARKER, "Received request to update account with data : {}", accountAttributes)

        val id = accountAttributes.id
        updateAccount(id, accountAttributes)

        // TODO is this really required - the JDBC DAP does it
        return getById(id, attributesEnumeration)
    }

    override fun update(
        accountId: String, map: Map<String, Any>,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>? {
        _logger.debug(MASK_MARKER, "Received request to update account with id:{} and data : {}", accountId, map)

        updateAccount(accountId, AccountAttributes.fromMap(map))

        // TODO is this really required - the JDBC DAP does it
        return getById(accountId, attributesEnumeration)
    }

    private fun updateAccount(accountId: String, accountAttributes: AccountAttributes) =
        retry("updateAccount", N_OF_ATTEMPTS) {
            val observedItem = getItemByAccountId(accountId) ?: return@retry TransactionAttemptResult.Success(null)
            tryUpdateAccount(accountId, accountAttributes, observedItem)
        }

    private fun tryUpdateAccount(accountId: String, accountAttributes: AccountAttributes, observedItem: DynamoDBItem)
            : TransactionAttemptResult<Unit> {
        val observedVersion = observedItem.version()
        val newVersion = observedVersion + 1

        // The created attribute is not updated
        val created = AccountsTable.created.from(observedItem)
        val updated = Instant.now().epochSecond
        val commonItem = accountAttributes.toItem(accountId, created, updated)
        commonItem.addAttr(AccountsTable.version, newVersion)

        // The password attribute is not updated
        commonItem.remove(AccountsTable.password.name)
        // Preserve credential data, if any
        val maybePassword = AccountsTable.password.optionalFrom(observedItem)
        if (maybePassword != null) {
            commonItem[AccountsTable.password.name] = AccountsTable.password.toAttrValue(maybePassword)
        }
        val maybeCredentialAttributes = AccountsTable.credentialAttributes.optionalFrom(observedItem)
        if (maybeCredentialAttributes != null) {
            commonItem[AccountsTable.credentialAttributes.name] =
                AccountsTable.credentialAttributes.toAttrValue(maybeCredentialAttributes)
        }

        val request = buildAccountUpdateRequest(accountId, observedItem, commonItem)
        try {
            _dynamoDBClient.transactionWriteItems(request)
            return TransactionAttemptResult.Success(Unit)
        } catch (ex: Exception) {
            if (ex.isTransactionCancelledDueToConditionFailure()) {
                val exceptionCause = ex.cause
                if (exceptionCause is TransactionCanceledException) {
                    ex.validateKnownUniqueConstraintsForAccountMutations(
                        exceptionCause.cancellationReasons(),
                        request.transactItems()
                    )
                }

                return TransactionAttemptResult.Failure(ex)
            }
            throw ex
        }
    }

    private fun buildAccountUpdateRequest(
        accountId: String,
        observedItem: DynamoDBItem,
        commonItem: MutableMap<String, AttributeValue>,
    ): TransactWriteItemsRequest {
        val observedVersion = observedItem.version()
        val newVersion = observedVersion + 1

        commonItem.addAttr(AccountsTable.version, newVersion)
        commonItem.addAttr(AccountsTable.updated, Instant.now().epochSecond)

        // Fill the initials for userName and email (if present) only for the main item,
        // to build a partial Global Secondary Index on these attributes.
        val mainItemAttributes = mutableMapOf(
            AccountsTable.userNameInitial.uniqueKeyEntryFor(_tenantId, AccountsTable.userName.from(commonItem))
        )
        AccountsTable.email.optionalFrom(commonItem)?.also {
            AccountsTable.emailInitial.uniqueKeyEntryFor(_tenantId, it).also { pair ->
                mainItemAttributes[pair.first] = pair.second
            }
        }

        val updateBuilder = UpdateBuilderWithMultipleUniquenessConstraints(
            _configuration,
            AccountsTable,
            commonItem,
            AccountsTable.pk,
            versionAndAccountIdConditionExpression(observedVersion, accountId)
        )

        updateBuilder.handleUniqueAttribute(
            AccountsTable.accountId,
            accountId,
            accountId,
            mainItemAttributes
        )

        updateBuilder.handleUniqueAttribute(
            AccountsTable.userName,
            AccountsTable.userName.optionalFrom(observedItem),
            AccountsTable.userName.optionalFrom(commonItem),
        )

        updateBuilder.handleUniqueAttribute(
            AccountsTable.email,
            AccountsTable.email.optionalFrom(observedItem),
            AccountsTable.email.optionalFrom(commonItem),
        )

        if (UNIQUE_ACCOUNT_PHONE_NUMBER) {
            updateBuilder.handleUniqueAttribute(
                AccountsTable.phone,
                AccountsTable.phone.optionalFrom(observedItem),
                AccountsTable.phone.optionalFrom(commonItem),
            )
        }

        return updateBuilder.build()
    }

    override fun patch(
        accountId: String, attributeUpdate: AttributeUpdate,
        attributesEnumeration: ResourceQuery.AttributesEnumeration
    ): ResourceAttributes<*>? {
        retry("updateAccount", N_OF_ATTEMPTS)
        {
            val observedItem = getItemByAccountId(accountId) ?: return@retry TransactionAttemptResult.Success(null)
            val observedAttributes = observedItem.toAccountAttributes()

            val newAttributes = attributeUpdate.applyOn(observedAttributes)
                .with(Attribute.of(ResourceAttributes.ID, accountId))
                .removeAttribute(ResourceAttributes.META)

            tryUpdateAccount(accountId, fromAttributes(newAttributes), observedItem)
        }

        // TODO is this really required - the JDBC DAP does it
        return getById(accountId, attributesEnumeration)
    }

    // Start CredentialStoringDataAccessProvider implementation

    override fun store(subjectAttributes: SubjectAttributes, passwordData: String) {
        if (!storeCredential(subjectAttributes, passwordData, null)) {
            _logger.debug(
                MASK_MARKER, "Unable to update password because there isn't an account with username {}",
                subjectAttributes.subject
            )
            throw IllegalStateException("Account not found")
        }
    }

    override fun store(
        subjectAttributes: SubjectAttributes,
        passwordData: String,
        credentialAttributes: CredentialAttributes
    ) {
        if (!storeCredential(subjectAttributes, passwordData, credentialAttributes)) {
            _logger.debug(
                MASK_MARKER, "Unable to update password because there isn't an account with username {}",
                subjectAttributes.subject
            )
            throw IllegalStateException("Account not found")
        }
    }

    override fun store(subjectAttributes: SubjectAttributes, credentialAttributes: CredentialAttributes): Boolean {
        return storeCredential(subjectAttributes, null, credentialAttributes)
    }

    private fun storeCredential(
        subjectAttributes: SubjectAttributes,
        passwordData: String?,
        credentialAttributes: CredentialAttributes?
    ): Boolean {
        val username = subjectAttributes.subject
        _logger.debug(
            MASK_MARKER, "Received request to update credential for username: {}. Password? {}, attributes? {}.",
            username,
            passwordData != null,
            credentialAttributes != null
        )

        return retry("storeCredential", N_OF_ATTEMPTS)
        {
            val observedItem = getItemByUsername(username)
            if (observedItem == null) {
                return@retry TransactionAttemptResult.Success(false)
            }

            val update = listOfNotNull(
                passwordData?.let {
                    AccountsTable.password.toNameValuePair(it)
                },
                credentialAttributes?.let {
                    AccountsTable.credentialAttributes.toNameValuePair(jsonHandler.fromAttributes(it))
                }
            ).toMap()

            val accountId = observedItem.accountId()
            val commonItem = observedItem.toMutableMap().apply { putAll(update) }

            try {
                _dynamoDBClient.transactionWriteItems(buildAccountUpdateRequest(accountId, observedItem, commonItem))
                return@retry TransactionAttemptResult.Success(true)
            } catch (ex: Exception) {
                if (ex.isTransactionCancelledDueToConditionFailure()) {
                    return@retry TransactionAttemptResult.Failure(ex)
                }
                throw ex
            }
        }
    }

    override fun get(subjectAttributes: SubjectAttributes): GetResult? {
        val username = subjectAttributes.subject
        _logger.debug(MASK_MARKER, "Received request to verify password for username : {}", username)

        val request = GetItemRequest.builder()
            .tableName(tableName)
            // 'password' is not a DynamoDB reserved word
            .key(AccountsTable.keyFrom(AccountsTable.userName, _tenantId, username))
            .projectionExpression(
                "${AccountsTable.accountId.name}, ${AccountsTable.userName.name}, " +
                        "${AccountsTable.password.name}, ${AccountsTable.credentialAttributes.name}, ${AccountsTable.active.name}"
            )
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem()) {
            return null
        }

        val item = response.item()
        val active = AccountsTable.active.optionalFrom(item) ?: false

        if (!active) {
            return null
        }

        val passwordData = AccountsTable.password.optionalFrom(item) ?: return null
        val credentialAttributes = AccountsTable.credentialAttributes.optionalFrom(item)?.let {
            CredentialAttributes.of(jsonHandler.toAttributes(it))
        } ?: CredentialAttributes.empty()

        val outputSubjectAttributes = SubjectAttributes.of(
            username,
            Attributes.of(
                Attribute.of("accountId", AccountsTable.accountId.optionalFrom(item)),
                Attribute.of("userName", AccountsTable.userName.optionalFrom(item))
            )
        )
        return GetResult(outputSubjectAttributes, credentialAttributes, passwordData)
    }

    override fun delete(subjectAttributes: SubjectAttributes): Boolean {
        // The only usage of this method so far in the product is when deleting an account, so here we assume that is
        // the case and avoid the extra work/traffic.
        _logger.debug(
            "DynamoDB data-source doesn't support deleting credentials because they are stored with accounts. " +
                    "This request will be ignored, but the credential will be removed if the account is deleted."
        )
        return true
    }

    override fun updateSubject(subject: SubjectAttributes, newSubject: String): Boolean {
        // The only usage of this method so far in the product is when updating an account, so here we assume that is
        // the case and avoid the extra work/traffic.
        _logger.debug(
            "DynamoDB data-source doesn't support updating credential subjects because they are stored with accounts. " +
                    "This request will be ignored, but the credential will be up-to-date if the account was updated."
        )
        return true
    }

    // End CredentialStoringDataAccessProvider implementation

    private fun getItemByAccountId(accountId: String): DynamoDBItem? {
        val requestBuilder = GetItemRequest.builder()
            .tableName(tableName)
            .key(AccountsTable.keyFromAccountId(accountId, _tenantId))
            .consistentRead(true)

        val request = requestBuilder.build()

        val response = _dynamoDBClient.getItem(request)
        return if (response.hasItem()) response.item() else null
    }

    private fun getItemByUsername(userName: String): DynamoDBItem? {
        val requestBuilder = GetItemRequest.builder()
            .tableName(tableName)
            .key(AccountsTable.keyFrom(AccountsTable.userName, _tenantId, userName))
            .consistentRead(true)

        val request = requestBuilder.build()

        val response = _dynamoDBClient.getItem(request)
        return if (response.hasItem()) response.item() else null
    }

    override fun link(
        linkingAccountManager: String,
        localAccountId: String,
        foreignDomainName: String,
        foreignUserName: String
    ) {
        val request = PutItemRequest.builder()
            .tableName(LinksTable.name(_configuration))
            .item(
                LinksTable.createItem(
                    _tenantId,
                    linkingAccountManager,
                    localAccountId,
                    foreignDomainName,
                    foreignUserName
                )
            )
            .build()

        _dynamoDBClient.putItem(request)
    }

    override fun listLinks(linkingAccountManager: String, localAccountId: String): Collection<LinkedAccount> {
        val request = QueryRequest.builder()
            .tableName(LinksTable.name(_configuration))
            .indexName(LinksTable.listLinksIndex.name)
            .keyConditionExpression(LinksTable.listLinksIndex.keyConditionExpression)
            .filterExpression(LinksTable.listLinksIndex.filterExpression(_tenantId))
            .expressionAttributeValues(
                LinksTable.listLinksIndex.expressionValueMap(
                    _tenantId,
                    localAccountId,
                    linkingAccountManager
                )
            )
            .expressionAttributeNames(LinksTable.listLinksIndex.expressionNameMap)
            .build()

        return querySequence(request, _dynamoDBClient)
            .map { item ->
                LinkedAccount.of(
                    LinksTable.linkedAccountDomainName.from(item),
                    LinksTable.linkedAccountId.from(item),
                    NO_LINK_DESCRIPTION,
                    LinksTable.created.optionalFrom(item).toIsoInstantString()
                )
            }
            .toList()
    }

    override fun resolveLink(
        linkingAccountManager: String,
        foreignDomainName: String,
        foreignAccountId: String
    ): AccountAttributes? {
        val request = GetItemRequest.builder()
            .tableName(LinksTable.name(_configuration))
            .key(mapOf(LinksTable.pk.toNameValuePair(_tenantId, foreignAccountId, foreignDomainName)))
            .consistentRead(true)
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        val item = response.item()

        val itemAccountManager = LinksTable.linkingAccountManager.optionalFrom(item)
            ?: throw SchemaErrorException(LinksTable, LinksTable.linkingAccountManager)

        if (itemAccountManager != linkingAccountManager) {
            return null
        }

        val localAccountId = LinksTable.localAccountId.optionalFrom(item)
            ?: throw SchemaErrorException(LinksTable, LinksTable.localAccountId)

        return getById(localAccountId)
    }

    override fun deleteLink(
        linkingAccountManager: String,
        localAccountId: String,
        foreignDomainName: String,
        foreignAccountId: String
    ): Boolean {
        val request = DeleteItemRequest.builder()
            .tableName(LinksTable.name(_configuration))
            .key(mapOf(LinksTable.pk.toNameValuePair(_tenantId, foreignAccountId, foreignDomainName)))
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

        return try {
            val response = _dynamoDBClient.deleteItem(request)
            response.sdkHttpResponse().isSuccessful
        } catch (_: ConditionalCheckFailedException) {
            _logger.trace("Conditional check failed when removing link, meaning that link does not exist")
            false
        }

    }

    override fun getAll(resourceQuery: ResourceQuery): ResourceQueryResult = try {
        val comparator = getComparatorFor(resourceQuery)

        val queryPlan = if (resourceQuery.filter != null) {
            QueryPlanner(AccountsTable.queryCapabilities, _tenantId).build(resourceQuery.filter)
        } else {
            QueryPlan.UsingScan.fullScan()
        }

        val values = when (queryPlan) {
            is QueryPlan.UsingQueries -> query(queryPlan)
            is QueryPlan.UsingScan -> scan(queryPlan)
        }

        // FIXME avoid the toList
        val sortedValues = if (comparator != null) {
            values.sortedWith(
                if (resourceQuery.sorting.sortOrder == ResourceQuery.Sorting.SortOrder.ASCENDING) {
                    comparator
                } else {
                    comparator.reversed()
                }
            ).toList()
        } else {
            values.toList()
        }

        val validatedStartIndex = resourceQuery.pagination.startIndex.toIntOrThrow("pagination.startIndex")
        val validatedCount = resourceQuery.pagination.count.toIntOrThrow("pagination.count")

        val finalValues = sortedValues
            .drop(validatedStartIndex)
            .take(validatedCount)
            .map { it.toAccountAttributes(resourceQuery.attributesEnumeration) }
            .toList()

        ResourceQueryResult(
            finalValues,
            sortedValues.size.toLong(),
            resourceQuery.pagination.startIndex,
            resourceQuery.pagination.count
        )
    } catch (e: UnsupportedQueryException) {
        _logger.debug("Unable to process query. Reason is '{}'", e.message)
        _logger.debug(MASK_MARKER, "Unable to process query = '{}", resourceQuery)

        throw _configuration.getExceptionFactory().externalServiceException(e.message)
    }

    private fun scan(queryPlan: QueryPlan.UsingScan?): Sequence<DynamoDBItem> {
        val scanRequestBuilder = ScanRequest.builder()
            .tableName(tableName)

        return if (queryPlan != null && queryPlan.expression.products.isNotEmpty()) {
            val dynamoDBScan = DynamoDBQueryBuilder.buildScan(queryPlan.expression).addPkFiltering(_tenantId)
            scanRequestBuilder.configureWith(dynamoDBScan)
            scanSequence(scanRequestBuilder.build(), _dynamoDBClient)
                .filterWith(queryPlan.expression.products)
        } else {
            scanRequestBuilder.configureWith(getFilterForItemsWithAccountIdPk(_tenantId))
            scanSequence(scanRequestBuilder.build(), _dynamoDBClient)
        }
    }

    private fun scan(
        queryPlan: QueryPlan.UsingScan?,
        paginationRequest: PaginationRequest?
    ): Pair<List<Map<String, AttributeValue>>, String?> {
        val exclusiveStartKey = if (!paginationRequest?.cursor.isNullOrBlank()) {
            paginationRequest?.let { QueryHelper.getExclusiveStartKey(jsonHandler, it.cursor) }
        } else {
            null
        }
        val limit = paginationRequest?.count ?: DEFAULT_PAGE_SIZE
        val scanRequestBuilder = ScanRequest.builder().tableName(tableName)

        val result = if (queryPlan != null && queryPlan.expression.products.isNotEmpty()) {
            val dynamoDBScan = DynamoDBQueryBuilder.buildScan(queryPlan.expression).addPkFiltering(_tenantId)
            scanRequestBuilder.configureWith(dynamoDBScan)
            scanWithPagination(_dynamoDBClient, scanRequestBuilder, limit, exclusiveStartKey, ::toLastEvaluatedKey)
        } else {
            scanRequestBuilder.configureWith(getFilterForItemsWithAccountIdPk(_tenantId))
            scanWithPagination(_dynamoDBClient, scanRequestBuilder, limit, exclusiveStartKey, ::toLastEvaluatedKey)
        }

        return result.items to QueryHelper.getEncodedCursor(jsonHandler, result.lastEvaluationKey)
    }

    private fun toLastEvaluatedKey(item: Map<String, AttributeValue>): Map<String, AttributeValue> =
        mapOf(
            AccountsTable.primaryKey.partitionAttribute.name to AccountsTable.primaryKey.partitionAttribute.attributeValueFrom(
                item
            )
        )

    private fun scanCount(queryPlan: QueryPlan.UsingScan?): Long {
        val scanRequestBuilder = ScanRequest.builder()
            .tableName(tableName)

        return if (queryPlan != null && queryPlan.expression.products.isNotEmpty()) {
            val dynamoDBScan = DynamoDBQueryBuilder.buildScan(queryPlan.expression).addPkFiltering(_tenantId)
            scanRequestBuilder.configureWith(dynamoDBScan).select(Select.COUNT)
            count(scanRequestBuilder.build(), _dynamoDBClient)
        } else {
            scanRequestBuilder.configureWith(getFilterForItemsWithAccountIdPk(_tenantId)).select(Select.COUNT)
            count(scanRequestBuilder.build(), _dynamoDBClient)
        }
    }

    private fun DynamoDBScan.addPkFiltering(tenantId: TenantId): DynamoDBScan {
        val pkScan = getFilterForItemsWithAccountIdPk(tenantId)
        return DynamoDBScan(
            filterExpression = "${pkScan.filterExpression} AND (${this.filterExpression})",
            valueMap = pkScan.valueMap + this.valueMap,
            nameMap = pkScan.nameMap + this.nameMap
        )
    }

    private fun query(queryPlan: QueryPlan.UsingQueries): Sequence<DynamoDBItem> {
        val nOfQueries = queryPlan.queries.entries.size
        if (nOfQueries > MAX_QUERIES) {
            throw UnsupportedQueryException.QueryRequiresTooManyOperations(nOfQueries, MAX_QUERIES)
        }
        val result = linkedMapOf<String, Map<String, AttributeValue>>()
        queryPlan.queries.forEach { query ->
            val dynamoDBQuery = DynamoDBQueryBuilder.buildQuery(query.key, query.value)

            val queryRequest = QueryRequest.builder()
                .tableName(tableName)
                .configureWith(dynamoDBQuery)
                .build()

            querySequence(queryRequest, _dynamoDBClient)
                .filterWith(query.value)
                .forEach {
                    result[AccountsTable.accountId.from(it)] = it
                }
        }
        return result.values.asSequence()
    }

    private fun query(
        queryPlan: QueryPlan.UsingQueries,
        sortRequest: AttributesSorting?,
        paginationRequest: PaginationRequest?
    ): Pair<List<Map<String, AttributeValue>>, String?> {
        val nOfQueries = queryPlan.queries.entries.size
        if (nOfQueries > 1) {
            throw UnsupportedQueryException.QueryRequiresTooManyOperations(nOfQueries, 1)
        }

        val exclusiveStartKey = if (!paginationRequest?.cursor.isNullOrBlank()) {
            paginationRequest?.let { QueryHelper.getExclusiveStartKey(jsonHandler, it.cursor) }
        } else {
            null
        }
        val limit = paginationRequest?.count ?: DEFAULT_PAGE_SIZE
        val query = queryPlan.queries.entries.first()
        val dynamoDBQuery = DynamoDBQueryBuilder.buildQuery(query.key, query.value, sortRequest?.sortOrder)

        val queryRequestBuilder = QueryRequest.builder()
            .tableName(AccountsTable.name(_configuration))
            .configureWith(dynamoDBQuery)

        val result =
            queryWithPagination(queryRequestBuilder, limit, exclusiveStartKey, _dynamoDBClient) {
                query.key.index.toIndexPrimaryKey(it, AccountsTable.primaryKey)
            }
        return result.items to QueryHelper.getEncodedCursor(jsonHandler, result.lastEvaluationKey)
    }

    private fun queryCount(queryPlan: QueryPlan.UsingQueries): Long {
        val nOfQueries = queryPlan.queries.entries.size
        if (nOfQueries > 1) {
            throw UnsupportedQueryException.QueryRequiresTooManyOperations(nOfQueries, 1)
        }

        val query = queryPlan.queries.entries.first()
        val dynamoDBQuery = DynamoDBQueryBuilder.buildQuery(query.key, query.value)

        val queryRequest = QueryRequest.builder()
            .tableName(tableName)
            .configureWith(dynamoDBQuery)
            .select(Select.COUNT)
            .build()

        return count(queryRequest, _dynamoDBClient)
    }

    private fun getComparatorFor(resourceQuery: ResourceQuery): Comparator<Map<String, AttributeValue>>? {
        return if (resourceQuery.sorting != null && resourceQuery.sorting.sortBy != null) {
            AccountsTable.queryCapabilities.attributeMap[resourceQuery.sorting.sortBy]
                ?.let { attribute ->
                    attribute.comparator()
                        ?: throw UnsupportedQueryException.UnsupportedSortAttribute(resourceQuery.sorting.sortBy)
                }
                ?: throw UnsupportedQueryException.UnknownSortAttribute(resourceQuery.sorting.sortBy)
        } else {
            null
        }
    }

    override fun getAll(startIndex: Long, count: Long): ResourceQueryResult {
        val validatedStartIndex = startIndex.toIntOrThrow("pagination.startIndex")
        val validatedCount = count.toIntOrThrow("pagination.count")

        val allItems = scan(null).toList()

        val paginatedItems = allItems
            .drop(validatedStartIndex)
            .take(validatedCount)
            .map { it.toAccountAttributes() }
            .toList()

        return ResourceQueryResult(paginatedItems, allItems.count().toLong(), startIndex, count)
    }

    private fun AccountAttributes.toItem(
        accountId: String,
        created: Long,
        updated: Long
    ): MutableMap<String, AttributeValue> {
        val item = mutableMapOf<String, AttributeValue>()
        item.addAttr(AccountsTable.userName, userName)
        item.addAttr(AccountsTable.active, isActive)

        item.addAttr(AccountsTable.accountId, accountId)
        item.addAttr(AccountsTable.created, created)
        item.addAttr(AccountsTable.updated, updated)

        _tenantId.tenantId?.let {
            item.addAttr(AccountsTable.tenantId, it)
        }

        if (!password.isNullOrEmpty()) {
            item.addAttr(AccountsTable.password, password)
        }

        if (emails != null) {
            val email = emails.primaryOrFirst
            if (email != null && !email.isEmpty) {
                item.addAttr(AccountsTable.email, email.significantValue)
            }
        }

        if (phoneNumbers != null) {
            val phone = phoneNumbers.primaryOrFirst
            if (phone != null && !phone.isEmpty) {
                item.addAttr(AccountsTable.phone, phone.significantValue)
            }
        }

        serialize(this)?.let {
            item.addAttr(AccountsTable.attributes, it)
        }

        return item
    }

    private fun serialize(accountAttributes: AccountAttributes): String? {
        val filteredAttributes =
            Attributes.of(
                removeLinkedAccounts(accountAttributes)
                    .filter {
                        !attributesToRemove.contains(it.name.value)
                    }
            )

        return if (!filteredAttributes.isEmpty) {
            jsonHandler.fromAttributes(filteredAttributes)
        } else {
            null
        }
    }

    private fun Map<String, AttributeValue>.toAccountAttributes(): AccountAttributes = toAccountAttributes(null)
    private fun Map<String, AttributeValue>.toAccountAttributes(attributesEnumeration: ResourceQuery.AttributesEnumeration?): AccountAttributes {
        val map = mutableMapOf<String, Any?>()

        map["id"] = AccountsTable.accountId.optionalFrom(this)

        forEach { (key, value) ->
            when (key) {
                AccountsTable.pk.name -> { /*ignore*/
                }

                AccountsTable.tenantId.name -> { /*ignore*/
                }

                AccountsTable.version.name -> { /*ignore*/
                }

                AccountsTable.accountId.name -> { /*ignore: already in the 'id' attribute*/
                }

                AccountsTable.active.name -> map["active"] = value.bool()
                AccountsTable.email.name -> {
                } // skip, emails are in attributes
                AccountsTable.phone.name -> {
                } // skip, phones are in attributes
                AccountsTable.attributes.name -> map.putAll(
                    jsonHandler.fromJson(AccountsTable.attributes.optionalFrom(this)) ?: emptyMap<String, Any>()
                )

                AccountsTable.created.name -> {
                } // skip, this goes to meta
                AccountsTable.updated.name -> {
                } // skip, this goes to meta
                AccountsTable.password.name -> {
                } // do not return passwords
                AccountsTable.userNameInitial.name -> {
                } // Ignore: attribute specific to DynamoDB
                AccountsTable.emailInitial.name -> {
                } // Ignore: attribute specific to DynamoDB
                else -> map[key] = value.s()
            }
        }

        if (attributesEnumeration.includeMeta()) {
            val zoneId = if (map["timezone"] != null) ZoneId.of(map["timezone"].toString()) else ZoneId.of("UTC")

            map["meta"] = mapOf(
                Meta.RESOURCE_TYPE to AccountAttributes.RESOURCE_TYPE,
                "created" to formatAsStringAttributeValue(
                    ZonedDateTime.ofInstant(
                        Instant.ofEpochSecond(AccountsTable.created.optionalFrom(this) ?: -1L),
                        zoneId
                    )
                ),
                "lastModified" to formatAsStringAttributeValue(
                    ZonedDateTime.ofInstant(
                        Instant.ofEpochSecond(AccountsTable.updated.optionalFrom(this) ?: -1L),
                        zoneId
                    )
                ),
                Meta.TIME_ZONE_ID to (map[AccountAttributes.TIMEZONE] ?: "UTC")
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
        this == null || isNeutral || (this is ResourceQuery.Inclusions && attributes.contains("meta")) || (this is ResourceQuery.Exclusions && !attributes.contains(
            "meta"
        ))

    object AccountsTable : Table("curity-accounts") {
        val pk = KeyStringAttribute("pk")
        val tenantId = StringAttribute("tenantId")
        val accountId = TenantAwareUniqueStringAttribute("accountId", "ai", "#")
        val version = NumberLongAttribute("version")
        val userName = TenantAwareUniqueStringAttribute("userName", "un", "#")
        val userNameInitial = StartsWithStringAttribute("userNameInitial", userName, INITIAL_LENGTH)
        val email = TenantAwareUniqueStringAttribute("email", "em", "#")
        val emailInitial = StartsWithStringAttribute("emailInitial", email, INITIAL_LENGTH)

        // Used only if UNIQUE_ACCOUNT_PHONE_NUMBER is true
        val phone = TenantAwareUniqueStringAttribute("phone", "pn", "#")
        val password = StringAttribute("password")
        val credentialAttributes = StringAttribute("credentialAttributes")
        val active = BooleanAttribute("active")
        val attributes = StringAttribute("attributes")
        val created = NumberLongAttribute("created")
        val updated = NumberLongAttribute("updated")

        val primaryKey = PartitionKey(pk)
        val userNameInitialUserNameIndex =
            PartitionAndSortIndex("userNameInitial-userName-index", userNameInitial, userName)
        private val emailInitialEmailIndex =
            PartitionAndSortIndex("emailInitial-email-index", emailInitial, email)

        fun keyFromAccountId(accountIdValue: String, tenantId: TenantId) = keyFrom(accountId, tenantId, accountIdValue)

        fun keyFrom(uniqueAttribute: TenantAwareUniqueAttribute, tenantId: TenantId, value: String) = mapOf(
            pk.name to pk.toAttrValue(uniqueAttribute.uniquenessValueFrom(tenantId, value))
        )

        val queryCapabilities = TableQueryCapabilities(
            indexes = buildList {
                add(Index.from(PartitionKey(UniquenessBasedIndexStringAttribute(pk, accountId))))
                add(Index.from(PartitionKey(UniquenessBasedIndexStringAttribute(pk, userName))))
                add(Index.from(userNameInitialUserNameIndex))
                add(Index.from(PartitionKey(UniquenessBasedIndexStringAttribute(pk, email))))
                add(Index.from(emailInitialEmailIndex))
                if (UNIQUE_ACCOUNT_PHONE_NUMBER) {
                    add(Index.from(PartitionKey(UniquenessBasedIndexStringAttribute(pk, phone))))
                }
            },
            attributeMap = buildMap {
                put(AccountAttributes.USER_NAME, userName)
                put(userNameInitial.name, userNameInitial)
                put(email.name, email)
                put(AccountAttributes.EMAILS, email)
                put(AccountAttributes.EMAILS + ".value", email)
                put(emailInitial.name, emailInitial)
                put(AccountAttributes.ACTIVE, active)
                put(AccountAttributes.PHONE_NUMBERS, phone)
                put(AccountAttributes.PHONE_NUMBERS + ".value", phone)
            },
            setOf(TableCapability.SORTING),
        )
    }

    object LinksTable : Table("curity-links") {
        val pk =
            StringCompositeAttribute2("linkedAccountId_linkedAccountDomainName") { tenantId, linkedAccountId, linkedAccountDomainName ->
                val tenantSuffix = if (tenantId?.tenantId != null) ".${tenantId.tenantId}" else ""
                // e.g. abc@facebook or abc@facebook.tenant1
                "$linkedAccountId@$linkedAccountDomainName$tenantSuffix"
            }
        val tenantId = StringAttribute("tenantId")
        val localAccountId = StringAttribute("accountId")
        val linkedAccountId = StringAttribute("linkedAccountId")
        val linkedAccountDomainName = StringAttribute("linkedAccountDomainName")
        val linkingAccountManager = StringAttribute("linkingAccountManager")
        val created = NumberLongAttribute("created")

        val listLinksIndex = TenantAwarePartitionAndSortIndex(
            "list-links-index",
            localAccountId,
            linkingAccountManager,
            tenantId
        )

        fun createItem(
            tenantIdValue: TenantId,
            linkingAccountManagerValue: String,
            localAccountIdValue: String,
            foreignDomainNameValue: String,
            foreignUserNameValue: String
        ): Map<String, AttributeValue> = buildMap {
            put(pk.name, pk.toAttrValue(tenantIdValue, foreignUserNameValue to foreignDomainNameValue))
            addAttr(localAccountId, localAccountIdValue)
            addAttr(linkedAccountId, foreignUserNameValue)
            addAttr(linkedAccountDomainName, foreignDomainNameValue)
            addAttr(linkingAccountManager, linkingAccountManagerValue)
            addAttr(created, Instant.now().epochSecond)

            tenantIdValue.tenantId?.let {
                addAttr(tenantId, it)
            }
        }
    }

    private fun versionAndAccountIdConditionExpression(version: Long, accountId: String) = object : Expression(
        _conditionExpressionBuilder
    ) {
        override val values = mapOf(
            ":oldVersion" to AccountsTable.version.toAttrValue(version),
            AccountsTable.accountId.toExpressionNameValuePair(accountId)
        )
    }
}
