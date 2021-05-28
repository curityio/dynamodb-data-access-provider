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
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant

class DynamoDBCredentialDataAccessProvider(
    private val _dynamoDBClient: DynamoDBClient
) : CredentialDataAccessProvider
{
    override fun updatePassword(accountAttributes: AccountAttributes)
    {
        val username = accountAttributes.userName

        _logger.debug("Received request to update password for username : {}", username)

        val newPassword: String? = accountAttributes.password

        if (newPassword == null)
        {
            _logger.debug("Cannot update account password for {}, missing password value.", username)
            return
        }

        val now = Instant.now().epochSecond
        val request = UpdateItemRequest.builder()
            .tableName(AccountsTable.name)
            .key(AccountsTable.key(accountAttributes.id))
            .conditionExpression("attribute_exists(${AccountsTable.accountId})")
            // 'password' is not a DynamoDB reserved word
            .updateExpression("SET ${AccountsTable.password.name} = ${AccountsTable.password.colonName}, " +
                    "${AccountsTable.updated.name} = ${AccountsTable.updated.colonName}")
            .expressionAttributeValues(mapOf(
                AccountsTable.password.toExpressionNameValuePair(newPassword),
                AccountsTable.updated.toExpressionNameValuePair(now)
            ))
            .build()

        try
        {
            _dynamoDBClient.updateItem(request)
        } catch (_: ConditionalCheckFailedException)
        {
            // This means the condition failed, i.e, the entry does not exist,
            // which should not be signalled with an exception
            _logger.debug("Cannot update account password for {}, since that used does not exist", username)
        }
    }

    override fun verifyPassword(userName: String, password: String): AuthenticationAttributes?
    {
        _logger.debug("Received request to verify password for username : {}", userName)

        val request = QueryRequest.builder()
            .tableName(AccountsTable.name)
            .indexName(AccountsTable.userNameIndex.name)
            // 'password' is not a DynamoDB reserved word
            .keyConditionExpression("${AccountsTable.userName.name} = ${AccountsTable.userName.colonName}")
            .expressionAttributeValues(mapOf(AccountsTable.userName.toExpressionNameValuePair(userName)))
            .projectionExpression(
                "${AccountsTable.accountId.name}, ${AccountsTable.userName.name}, " +
                        "${AccountsTable.password.name}, ${AccountsTable.active.name}"
            )
            .build()

        val response = _dynamoDBClient.query(request)

        if (!response.hasItems() || response.items().isEmpty())
        {
            return null
        }

        val item = response.items()[0]
        val active = AccountsTable.active.optionalFrom(item) ?: false

        if (!active)
        {
            return null
        }

        // Password is not verified by this DAP, which is aligned with customQueryVerifiesPassword returning false
        return AuthenticationAttributes.of(
            SubjectAttributes.of(
                userName,
                Attributes.of(
                    Attribute.of("password", AccountsTable.password.optionalFrom(item)),
                    Attribute.of("accountId", AccountsTable.accountId.optionalFrom(item)),
                    Attribute.of("userName", AccountsTable.userName.optionalFrom(item))
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
        private val _logger: Logger = LoggerFactory.getLogger(DynamoDBCredentialDataAccessProvider::class.java)
    }
}
