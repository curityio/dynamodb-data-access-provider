/*
 * Copyright (C) 2021 Curity AB. All rights reserved.
 *
 * The contents of this file are the property of Curity AB.
 * You may not copy or use this file, in either source code
 * or executable form, except in compliance with terms
 * set by Curity AB.
 *
 * For further information, please contact Curity AB.
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

