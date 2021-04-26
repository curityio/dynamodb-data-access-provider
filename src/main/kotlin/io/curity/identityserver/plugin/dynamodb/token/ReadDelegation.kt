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

class ReadDelegation(
    private val id: String,
    private val owner: String,
    private val created: Long,
    private val scope: String,
    private val scopeClaims: Set<ScopeClaim>,
    private val claimMap: Map<String, Any>,
    private val clientId: String,
    private val redirectUri: String?,
    private val authorizationCodeHash: String?,
    private val authenticationAttributes: AuthenticationAttributes,
    private val customClaimValues: Map<String, Any>,
    private val expires: Long,
    private val status: DelegationStatus,
    private val enumActiveStatus: DelegationStatus,
    private val mtlsClientCertificate: String?,
    private val mtlsClientCertificateX5TS256: String?,
    private val mtlsClientCertificateDN: String?,
    private val consentResult: DelegationConsentResult,
    private val claims: Map<String, Any>
) : Delegation
{
    override fun getEnumActiveStatus() = enumActiveStatus

    override fun getStatus() = status

    override fun getExpires() = expires

    override fun getId() = id

    override fun getOwner() = owner

    override fun getCreated() = created

    override fun getScope() = scope

    override fun getScopeClaims()= scopeClaims

    override fun getClaimMap() = claimMap

    override fun getClientId() = clientId

    override fun getRedirectUri() = redirectUri

    override fun getAuthorizationCodeHash() = authorizationCodeHash

    override fun getAuthenticationAttributes() = authenticationAttributes

    override fun getCustomClaimValues() = customClaimValues

    override fun getMtlsClientCertificate() = mtlsClientCertificate

    override fun getMtlsClientCertificateX5TS256() = mtlsClientCertificateX5TS256

    override fun getMtlsClientCertificateDN() = mtlsClientCertificateDN

    override fun getConsentResult() = consentResult

    override fun getClaims() = claims
}
