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

import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.AuthenticationAttributes
import se.curity.identityserver.sdk.data.authorization.Delegation
import se.curity.identityserver.sdk.data.authorization.DelegationConsentResult
import se.curity.identityserver.sdk.data.authorization.DelegationStatus
import se.curity.identityserver.sdk.data.authorization.ScopeClaim
import se.curity.identityserver.sdk.service.Json
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class DelegationData(
        private val id: AttributeValue?,
        private val owner: AttributeValue?,
        private val created: AttributeValue?,
        private val expires: AttributeValue?,
        private val scope: AttributeValue?,
        private val scopeClaims: AttributeValue?,
        private val clientId: AttributeValue?,
        private val redirectUri: AttributeValue?,
        private val status: AttributeValue?,
        private val claims: AttributeValue?,
        private val claimMap: AttributeValue?,
        private val customClaimValues: AttributeValue?,
        private val authenticationAttributes: AttributeValue?,
        private val authorizationCodeHash: AttributeValue?,
        private val mtlsClientCertificate: AttributeValue?,
        private val mtlsClientCertificateDN: AttributeValue?,
        private val mtlsClientCertificateX5TS256: AttributeValue?,
        private val consentResult: AttributeValue?,
        private val jsonHandler: Json
): Delegation
{
    override fun getEnumActiveStatus(): DelegationStatus = DelegationStatus.issued

    override fun getStatus(): DelegationStatus = if (status != null) DelegationStatus.valueOf(status.s()) else DelegationStatus.revoked

    override fun getExpires(): Long = expires?.s()?.toLong() ?: -1L

    override fun getId(): String? = id?.s()

    override fun getOwner(): String? = owner?.s()

    override fun getCreated(): Long = created?.s()?.toLong() ?: -1L

    override fun getScope(): String? = scope?.s()

    override fun getScopeClaims(): MutableSet<DynamoDBScopeClaim?> = jsonHandler.fromJsonArray(scopeClaims?.s()).map { item -> DynamoDBScopeClaim.fromMap(item) }.toMutableSet()

    override fun getClaimMap(): MutableMap<String, Any> = jsonHandler.fromJson(claimMap?.s())

    override fun getClientId(): String? = clientId?.s()

    override fun getRedirectUri(): String? = redirectUri?.s()

    override fun getAuthorizationCodeHash(): String? = authorizationCodeHash?.s()

    override fun getAuthenticationAttributes(): AuthenticationAttributes = AuthenticationAttributes.fromAttributes(Attributes.fromMap(jsonHandler.fromJson(authenticationAttributes?.s())))

    override fun getCustomClaimValues(): MutableMap<String, Any> = jsonHandler.fromJson(customClaimValues?.s())

    override fun getMtlsClientCertificate(): String? = mtlsClientCertificate?.s()

    override fun getMtlsClientCertificateX5TS256(): String? = mtlsClientCertificateX5TS256?.s()

    override fun getMtlsClientCertificateDN(): String? = mtlsClientCertificateDN?.s()

    override fun getConsentResult(): DelegationConsentResult? = DelegationConsentResult.fromMap(jsonHandler.fromJson(consentResult?.s()))

    override fun getClaims(): MutableMap<String, Any> = jsonHandler.fromJson(claims?.s())
}

class DynamoDBScopeClaim(private val scope: String, private val name: String, private val required: Boolean): ScopeClaim {
    override fun asMap(): MutableMap<String, Any> = mutableMapOf(
            Pair("scope", scope),
            Pair("name", name),
            Pair("required", required)
    )

    override fun getScope(): String = scope

    override fun getName(): String = name

    override fun isRequired(): Boolean = required

    companion object
    {
        fun fromMap(map: Any?): DynamoDBScopeClaim?
        {
            if (map == null || map !is Map<*, *>) {
                return null
            }

            return DynamoDBScopeClaim(
                map["scope"] as String,
                map["name"] as String,
                map["required"] as Boolean
            )
        }
    }
}
