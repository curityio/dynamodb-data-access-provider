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

import se.curity.identityserver.sdk.attribute.AuthenticationAttributes
import se.curity.identityserver.sdk.data.authorization.Delegation
import se.curity.identityserver.sdk.data.authorization.DelegationConsentResult
import se.curity.identityserver.sdk.data.authorization.DelegationStatus
import se.curity.identityserver.sdk.data.authorization.ScopeClaim

class DynamoDBDelegation(
    val version: String,
    private val id: String,
    private val status: DelegationStatus,
    private val owner: String,
    private val created: Long,
    private val expires: Long,
    private val clientId: String,
    private val redirectUri: String?,
    private val authorizationCodeHash: String?,
    private val authenticationAttributes: AuthenticationAttributes,
    private val consentResult: DelegationConsentResult?,
    private val scope: String,
    private val claimMap: Map<String, Any>,
    private val customClaimValues: Map<String, Any>,
    private val claims: Map<String, Any>,
    private val mtlsClientCertificate: String?,
    private val mtlsClientCertificateX5TS256: String?,
    private val mtlsClientCertificateDN: String?

) : Delegation
{
    override fun getId(): String = id
    override fun getStatus(): DelegationStatus = status
    override fun getOwner(): String = owner
    override fun getCreated(): Long = created
    override fun getExpires(): Long = expires
    override fun getClientId(): String = clientId
    override fun getRedirectUri(): String? = redirectUri
    override fun getAuthorizationCodeHash(): String? = authorizationCodeHash
    override fun getScope(): String = scope

    override fun getAuthenticationAttributes(): AuthenticationAttributes = authenticationAttributes
    override fun getConsentResult(): DelegationConsentResult? = consentResult
    override fun getCustomClaimValues(): Map<String, Any> = customClaimValues
    override fun getClaims(): Map<String, Any> = claims
    override fun getClaimMap(): Map<String, Any> = claimMap

    override fun getMtlsClientCertificate(): String? = mtlsClientCertificate
    override fun getMtlsClientCertificateX5TS256(): String? = mtlsClientCertificateX5TS256
    override fun getMtlsClientCertificateDN(): String? = mtlsClientCertificateDN

    // Empty set because it property is deprecated
    override fun getScopeClaims(): Set<ScopeClaim> = setOf()

    override fun getEnumActiveStatus() = DelegationStatus.issued
}