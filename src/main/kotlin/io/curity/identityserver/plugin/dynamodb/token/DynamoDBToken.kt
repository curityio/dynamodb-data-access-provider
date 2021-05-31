/*
 *  Copyright 2021 Curity AB
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

class DynamoDBToken(
    private val tokenHash: String,

    private val id: String?,
    private val scope: String?,

    private val delegationsId: String,
    private val purpose: String,
    private val usage: String,
    private val format: String,
    private val status: TokenStatus,
    private val issuer: String,
    private val subject: String,

    private val created: Long,
    private val expires: Long,
    private val notBefore: Long,

    private val audience: StringOrArray,
    private val data: Map<String, Any>

) : Token
{
    override fun getTokenHash() = tokenHash

    override fun getId() = id
    override fun getScope() = scope

    override fun getEnumActiveStatus() = TokenStatus.issued

    override fun getDelegationsId() = delegationsId
    override fun getPurpose() = purpose
    override fun getUsage() = usage
    override fun getFormat() = format
    override fun getStatus() = status
    override fun getIssuer() = issuer
    override fun getSubject() = subject

    override fun getCreated() = created
    override fun getExpires() = expires
    override fun getNotBefore() = notBefore

    override fun getAudience() = audience
    override fun getData() = data
}

