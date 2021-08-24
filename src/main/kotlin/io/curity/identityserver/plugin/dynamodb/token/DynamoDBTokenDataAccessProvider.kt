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
package io.curity.identityserver.plugin.dynamodb.token

import io.curity.identityserver.plugin.dynamodb.DynamoDBClient
import io.curity.identityserver.plugin.dynamodb.DynamoDBItem
import io.curity.identityserver.plugin.dynamodb.ListStringAttribute
import io.curity.identityserver.plugin.dynamodb.NumberLongAttribute
import io.curity.identityserver.plugin.dynamodb.StringAttribute
import io.curity.identityserver.plugin.dynamodb.Table
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import se.curity.identityserver.sdk.data.authorization.Token
import se.curity.identityserver.sdk.data.authorization.TokenStatus
import se.curity.identityserver.sdk.data.tokens.DefaultStringOrArray
import se.curity.identityserver.sdk.datasource.TokenDataAccessProvider
import se.curity.identityserver.sdk.errors.ConflictException
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

class DynamoDBTokenDataAccessProvider(
    private val _configuration: DynamoDBDataAccessProviderConfiguration,
    private val _dynamoDBClient: DynamoDBClient
) : TokenDataAccessProvider {
    private val _jsonHandler = _configuration.getJsonHandler()

    object TokenTable : Table("curity-tokens") {
        val tokenHash = StringAttribute("tokenHash")

        val id = StringAttribute("id")
        val scope = StringAttribute("scope")

        val delegationsId = StringAttribute("delegationsId")
        val purpose = StringAttribute("purpose")
        val usage = StringAttribute("usage")
        val format = StringAttribute("format")
        val status = StringAttribute("status")
        val issuer = StringAttribute("issuer")
        val subject = StringAttribute("subject")

        val audience = ListStringAttribute("audience")
        val tokenData = StringAttribute("tokenData")

        val created = NumberLongAttribute("created")
        val expires = NumberLongAttribute("expires")
        val deletableAt = NumberLongAttribute("deletableAt")
        val notBefore = NumberLongAttribute("notBefore")

        fun keyFromHash(hash: String) = mapOf(
            tokenHash.toNameValuePair(hash)
        )

    }

    private fun Token.toItem() = mutableMapOf<String, AttributeValue>().also { item ->
        TokenTable.tokenHash.addTo(item, tokenHash)

        TokenTable.id.addToNullable(item, id)
        TokenTable.scope.addToNullable(item, scope)

        TokenTable.delegationsId.addTo(item, delegationsId)
        TokenTable.purpose.addTo(item, purpose)
        TokenTable.usage.addTo(item, usage)
        TokenTable.format.addTo(item, format)
        TokenTable.status.addTo(item, status.name)
        TokenTable.issuer.addTo(item, issuer)
        TokenTable.subject.addTo(item, subject)

        TokenTable.audience.addTo(item, audience.values)
        TokenTable.tokenData.addTo(item, _jsonHandler.toJson(data))

        TokenTable.created.addTo(item, created)
        TokenTable.expires.addTo(item, expires)
        TokenTable.notBefore.addTo(item, notBefore)
        TokenTable.deletableAt.addTo(item, expires + _configuration.getTokensTtlRetainDuration())
    }

    private fun DynamoDBItem.toToken(): Token {
        val item = this
        return TokenTable.run {

            DynamoDBToken(
                tokenHash = tokenHash.from(item),
                id = id.optionalFrom(item),
                scope = scope.optionalFrom(item),
                delegationsId = delegationsId.from(item),
                purpose = purpose.from(item),
                usage = usage.from(item),
                format = format.from(item),
                status = TokenStatus.valueOf(status.from(item)),
                issuer = issuer.from(item),
                subject = subject.from(item),
                created = created.from(item),
                expires = expires.from(item),
                notBefore = notBefore.from(item),
                audience = DefaultStringOrArray.of(audience.from(item)),
                data = _jsonHandler.fromJson(tokenData.from(item))
            )
        }
    }

    override fun getByHash(hash: String): Token? {
        val request = GetItemRequest.builder()
            .tableName(TokenTable.name(_configuration))
            .key(TokenTable.keyFromHash(hash))
            .consistentRead(true)
            .build()

        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        return response.item().toToken()
    }

    override fun create(token: Token) {
        val request = PutItemRequest.builder()
            .tableName(TokenTable.name(_configuration))
            .conditionExpression("attribute_not_exists(${TokenTable.tokenHash.name})")
            .item(token.toItem())
            .build()

        try {
            _dynamoDBClient.putItem(request)
        } catch (_: ConditionalCheckFailedException) {
            throw ConflictException("Token with same hash already exists")
        }
    }

    override fun getStatus(tokenHash: String): String? {
        val token = getByHash(tokenHash)

        return token?.status?.toString()
    }

    override fun setStatusByTokenHash(tokenHash: String, newStatus: TokenStatus): Long {
        val request = UpdateItemRequest.builder()
            .tableName(TokenTable.name(_configuration))
            .key(TokenTable.keyFromHash(tokenHash))
            .conditionExpression("attribute_exists(${TokenTable.tokenHash})")
            .updateExpression("SET ${TokenTable.status.hashName} = ${TokenTable.status.colonName}")
            .expressionAttributeNames(mapOf(TokenTable.status.toNamePair()))
            .expressionAttributeValues(mapOf(TokenTable.status.toExpressionNameValuePair(newStatus.name)))
            .returnValues(ReturnValue.UPDATED_NEW)
            .build()

        try {
            val response = _dynamoDBClient.updateItem(request)
            return if (response.hasAttributes() && response.attributes().isNotEmpty()) {
                1
            } else {
                0
            }
        } catch (_: ConditionalCheckFailedException) {
            // this exceptions means the entry does not exists, which isn't an error
            return 0
        }
    }

    override fun setStatus(tokenId: String, newStatus: TokenStatus): Long {
        // This method is not implemented because it isn't used and will be deprecated.
        throw UnsupportedOperationException()
    }
}
