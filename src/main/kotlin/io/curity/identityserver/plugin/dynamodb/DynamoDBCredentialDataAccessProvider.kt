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

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.attribute.AccountAttributes
import se.curity.identityserver.sdk.attribute.Attribute
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.AuthenticationAttributes
import se.curity.identityserver.sdk.attribute.ContextAttributes
import se.curity.identityserver.sdk.attribute.SubjectAttributes
import se.curity.identityserver.sdk.datasource.CredentialDataAccessProvider
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

class DynamoDBCredentialDataAccessProvider(private val dynamoDBClient: DynamoDBClient) : CredentialDataAccessProvider
{
    override fun updatePassword(accountAttributes: AccountAttributes)
    {
        val username = accountAttributes.userName

        logger.debug("Received request to update password for username : {}", username)

        val newPassword: String? = accountAttributes.password

        if (newPassword == null)
        {
            logger.debug("Cannot update account password for {}, missing password value.", username)
            return
        }

        val request = UpdateItemRequest.builder()
            .tableName(AccountsTable.name)
            .key(AccountsTable.key(accountAttributes.id))
            // 'password' is not a DynamoDB reserved word
            .updateExpression("SET ${AccountsTable.password.name} = ${AccountsTable.password.colonName}")
            .expressionAttributeValues(mapOf(AccountsTable.password.toExpressionNameValuePair(newPassword)))
            .build()

        dynamoDBClient.updateItem(request)
    }

    override fun verifyPassword(userName: String, password: String): AuthenticationAttributes?
    {
        logger.debug("Received request to verify password for username : {}", userName)

        val request = QueryRequest.builder()
            .tableName(AccountsTable.name)
            .indexName(AccountsTable.userNameIndex.name)
            // 'password' is not a DynamoDB reserved word
            .keyConditionExpression("${AccountsTable.userName.name} = ${AccountsTable.userName.colonName}")
            .expressionAttributeValues(mapOf(AccountsTable.userName.toExpressionNameValuePair(userName)))
            .projectionExpression(
                "${AccountsTable.accountId.name}, ${AccountsTable.userName.name}, " +
                        "${AccountsTable.password.name}, ${AccountsTable.active.name}")
            .build()

        val response = dynamoDBClient.query(request)

        if (!response.hasItems() || response.items().isEmpty())
        {
            return null
        }

        val item = response.items()[0]
        val active = AccountsTable.active.fromOpt(item) ?: false

        if (!active)
        {
            return null
        }

        // Password is not verified by this DAP, which is aligned with customQueryVerifiesPassword returning false
        return AuthenticationAttributes.of(
            SubjectAttributes.of(
                userName,
                Attributes.of(
                    Attribute.of("password", AccountsTable.password.fromOpt(item)),
                    Attribute.of("accountId", AccountsTable.accountId.fromOpt(item)),
                    Attribute.of("userName", AccountsTable.userName.fromOpt(item))
                )
            ),
            ContextAttributes.empty()
        )
    }

    override fun customQueryVerifiesPassword(): Boolean
    {
        return false
    }

    companion object
    {
        private val AccountsTable = DynamoDBUserAccountDataAccessProvider.AccountsTable
        private val logger: Logger = LoggerFactory.getLogger(DynamoDBCredentialDataAccessProvider::class.java)
    }
}
