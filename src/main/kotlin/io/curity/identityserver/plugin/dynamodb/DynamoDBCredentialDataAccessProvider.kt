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
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

class DynamoDBCredentialDataAccessProvider(private val dynamoDBClient: DynamoDBClient): CredentialDataAccessProvider
{
    override fun updatePassword(accountAttributes: AccountAttributes)
    {
        val username = accountAttributes.userName

        logger.debug("Received request to update password for username : {}", username)

        val newPassword: String? = accountAttributes.password

        if (newPassword == null) {
            logger.debug("Cannot update account password for {}, missing password value.", username)
            return
        }

        val request = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(username.toKey("userName"))
                .expressionAttributeValues(mapOf<String, AttributeValue>(Pair(":password", AttributeValue.builder().s(newPassword).build())))
                .updateExpression("SET password = :password")
                .build()

        dynamoDBClient.updateItem(request)
    }

    override fun verifyPassword(userName: String, password: String): AuthenticationAttributes?
    {
        logger.debug("Received request to verify password for username : {}", userName)

        val request = GetItemRequest.builder()
                .tableName(tableName)
                .key(userName.toKey("userName"))
                .attributesToGet("userName", "password", "active")
                .build()

        val response = dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        val item = response.item()

        if (!item["active"]!!.bool()) {
            return null
        }

        return AuthenticationAttributes.of(
            SubjectAttributes.of(userName,
                Attributes.of(
                    Attribute.of("password", item["password"]?.s()),
                    Attribute.of("accountId", item["userName"]?.s()),
                    Attribute.of("userName", item["userName"]?.s()))
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
        private const val tableName = "curity-accounts"
        private val logger: Logger = LoggerFactory.getLogger(DynamoDBCredentialDataAccessProvider::class.java)
    }
}
