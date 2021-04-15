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

import se.curity.identityserver.sdk.data.StringOrArray
import se.curity.identityserver.sdk.data.authorization.Token
import se.curity.identityserver.sdk.data.authorization.TokenStatus
import se.curity.identityserver.sdk.data.tokens.DefaultStringOrArray
import se.curity.identityserver.sdk.service.Json
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class TokenData(
        private val tokenHash: AttributeValue?,
        private val id: AttributeValue?,
        private val delegationsId: AttributeValue?,
        private val purpose: AttributeValue?,
        private val usage: AttributeValue?,
        private val format: AttributeValue?,
        private val created: AttributeValue?,
        private val expires: AttributeValue?,
        private val scope: AttributeValue?,
        private val status: AttributeValue?,
        private val issuer: AttributeValue?,
        private val subject: AttributeValue?,
        private val serializedAudience: AttributeValue?,
        private val notBefore: AttributeValue?,
        private val serializedTokenData: AttributeValue?,
        private val jsonHandler: Json
): Token
{
    
    override fun getEnumActiveStatus(): TokenStatus = TokenStatus.issued

    override fun getStatus(): TokenStatus = if (status != null ) TokenStatus.valueOf(status.s()) else TokenStatus.revoked

    override fun getExpires(): Long = expires?.s()?.toLong() ?: -1

    override fun getTokenHash(): String? = tokenHash?.s()

    override fun getId(): String? = id?.s()

    override fun getDelegationsId(): String? = delegationsId?.s()

    override fun getPurpose(): String? = purpose?.s()

    override fun getUsage(): String? = usage?.s()

    override fun getFormat(): String? = format?.s()

    override fun getScope(): String? = scope?.s()

    override fun getCreated(): Long = created?.s()?.toLong() ?: -1

    override fun getIssuer(): String? = issuer?.s()

    override fun getSubject(): String? = subject?.s()

    override fun getAudience(): StringOrArray? = DefaultStringOrArray.of(jsonHandler.fromJsonArray(serializedAudience?.s()) as Collection<String>)

    override fun getNotBefore(): Long = notBefore?.s()?.toLong() ?: Long.MAX_VALUE

    override fun getData(): MutableMap<String, Any> = jsonHandler.fromJson(serializedTokenData?.s())

}
