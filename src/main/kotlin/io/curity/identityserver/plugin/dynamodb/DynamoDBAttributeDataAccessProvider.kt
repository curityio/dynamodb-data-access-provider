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

import se.curity.identityserver.sdk.attribute.AttributeTableView
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.datasource.AttributeDataAccessProvider
import software.amazon.awssdk.services.dynamodb.model.QueryRequest

class DynamoDBAttributeDataAccessProvider(private val dynamoDBClient: DynamoDBClient) : AttributeDataAccessProvider
{
    override fun getAttributes(subject: String): AttributeTableView
    {
        val accountQuery = QueryRequest.builder()
            .tableName(AccountsTable.name)
            .indexName(AccountsTable.userNameIndex.name)
            .keyConditionExpression("${AccountsTable.userName.name} = ${AccountsTable.userName.colonName}")
            .expressionAttributeValues(mapOf(AccountsTable.userName.toExpressionNameValuePair(subject)))
            .build()

        val accountQueryResult = dynamoDBClient.query(accountQuery)
        if (!accountQueryResult.hasItems() || accountQueryResult.items().isEmpty())
        {
            return AttributeTableView.empty()
        }
        val accountQueryItem = accountQueryResult.items()[0]
        val accountId = AccountsTable.accountId.optionalFrom(accountQueryItem)
            ?: throw SchemaErrorException(AccountsTable, AccountsTable.accountId)

        val linksQueryRequest = QueryRequest.builder()
            .tableName(LinksTable.name)
            .indexName(LinksTable.listLinksIndex.name)
            .keyConditionExpression("${LinksTable.localAccountId.name} = ${LinksTable.localAccountId.colonName}")
            .expressionAttributeValues(mapOf(LinksTable.localAccountId.toExpressionNameValuePair(accountId)))
            .build()

        val items = querySequence(linksQueryRequest, dynamoDBClient)

        return AttributeTableView.ofAttributes(
            items
                .map { item ->
                    Attributes.fromMap(
                        item
                            .map { entry -> (dynamoNamesToJdbcNames[entry.key] ?: entry.key) to entry.value.s() }
                            .toMap()
                    )
                }
                .toList()
        )
    }

    companion object
    {
        private val AccountsTable = DynamoDBUserAccountDataAccessProvider.AccountsTable
        private val LinksTable = DynamoDBUserAccountDataAccessProvider.LinksTable

        // Maps the internal DynamoDB column names to the canonical (JDBC) names
        private val dynamoNamesToJdbcNames = mapOf(
            LinksTable.localAccountId.name to "account_id",
            LinksTable.linkingAccountManager.name to "linking_account_manager",
            LinksTable.linkedAccountDomainName.name to "linked_account_domain_name",
            LinksTable.linkedAccountId.name to "linked_account_id"
        )
    }
}
