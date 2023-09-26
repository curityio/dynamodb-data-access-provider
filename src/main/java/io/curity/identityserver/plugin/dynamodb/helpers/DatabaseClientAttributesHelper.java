/*
 * Copyright (C) 2023 Curity AB. All rights reserved.
 *
 * The contents of this file are the property of Curity AB.
 * You may not copy or use this file, in either source code
 * or executable form, except in compliance with terms
 * set by Curity AB.
 *
 * For further information, please contact Curity AB.
 */

package io.curity.identityserver.plugin.dynamodb.helpers;

import se.curity.identityserver.sdk.Nullable;
import se.curity.identityserver.sdk.attribute.Attribute;
import se.curity.identityserver.sdk.attribute.AttributeCollector;
import se.curity.identityserver.sdk.attribute.Attributes;
import se.curity.identityserver.sdk.attribute.MapAttributeValue;
import se.curity.identityserver.sdk.attribute.UserAuthenticationConfigAttributes;
import se.curity.identityserver.sdk.attribute.client.database.ClientAttestationAttributes;
import se.curity.identityserver.sdk.attribute.client.database.ClientAuthenticationVerifier;
import se.curity.identityserver.sdk.attribute.client.database.ClientCapabilitiesAttributes;
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes;
import se.curity.identityserver.sdk.attribute.client.database.JwksUri;
import se.curity.identityserver.sdk.attribute.client.database.JwtAssertionAttributes;
import se.curity.identityserver.sdk.attribute.client.database.JwtSigningAttributes;
import se.curity.identityserver.sdk.attribute.client.database.UserConsentAttributes;
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes;
import se.curity.identityserver.sdk.authentication.mutualtls.MutualTlsAttributes;
import se.curity.identityserver.sdk.data.query.ResourceQuery;
import se.curity.identityserver.sdk.service.Json;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static se.curity.identityserver.sdk.attribute.UserAuthenticationConfigAttributes.AUTHENTICATOR_FILTERS;
import static se.curity.identityserver.sdk.attribute.UserAuthenticationConfigAttributes.REQUIRED_CLAIMS;
import static se.curity.identityserver.sdk.attribute.UserAuthenticationConfigAttributes.TEMPLATE_AREA;
import static se.curity.identityserver.sdk.attribute.client.database.ClientCapabilitiesAttributes.BackchannelCapability.AUTHENTICATORS;
import static se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.AUDIENCES;
import static se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.CLAIM_MAPPER_ID;
import static se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.CLIENT_ID;
import static se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.META;
import static se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.NAME;
import static se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.REDIRECT_URI_VALIDATION_POLICY_ID;
import static se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.SCOPES;
import static se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.STATUS;
import static se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.SUBJECT_TYPE;
import static se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.USERINFO_SIGNED_ISSUER_ID;
import static se.curity.identityserver.sdk.attribute.client.database.UserConsentAttributes.CONSENTORS;

public final class DatabaseClientAttributesHelper
{
    // Constants reused from DatabaseClientsTableColumnMapping
    public static final String CLIENT_NAME_COLUMN = "client_name";
    public static final String CREATED_DATE_COLUMN = "created";
    public static final String UPDATED_COLUMN = "updated";
    public static final String ID_TOKEN_JWE_ENCRYPTION_KEY_ID = "id_token_jwe_encryption_key_id";
    public static final String REQUEST_OBJECT_BY_REFERENCE_HTTP_CLIENT_ID = "request_object_by_reference_http_client_id";
    public static final String SECONDARY_CLIENT_AUTHENTICATION = "secondary_client_authentication";
    public static final String SECONDARY_CREDENTIAL_MANAGER_ID = "secondary_credential_manager_id";
    public static final String SECONDARY_ASYMMETRIC_KEY_ID = "secondary_asymmetric_key_id";
    public static final String SECONDARY_MUTUAL_TLS_CLIENT_CERTIFICATE_ID = "secondary_mutual_tls_client_certificate_id";
    public static final String PRIMARY_CLIENT_AUTHENTICATION = "primary_client_authentication";
    public static final String PRIMARY_CREDENTIAL_MANAGER_ID = "primary_credential_manager_id";
    public static final String PRIMARY_ASYMMETRIC_KEY_ID = "primary_asymmetric_key_id";
    public static final String PRIMARY_MUTUAL_TLS_CLIENT_CERTIFICATE_ID = "primary_mutual_tls_client_certificate_id";
    public static final String HAAPI_POLICY_ID = "haapi_policy_id";
    public static final String JWT_ASSERTION_JWKS_URI_CLIENT_ID = "jwt_assertion_jwks_uri_http_client_id";
    public static final String JWT_ASSERTION_ASYMMETRIC_KEY_ID = "jwt_assertion_asymmetric_key_id";
    public static final String ROPC_CREDENTIAL_MANAGER_ID = "resource_owner_password_credentials_credential_manager_id";
    public static final String BACKCHANNEL_LOGOUT_HTTP_CLIENT_ID = "backchannel_logout_http_client_id";
    public static final String TAGS = "tags";
    public static final String ATTRIBUTES = "attributes";
    public static final String PROFILE_ID = "profile_id";
    public static final String CONFIGURATION_REFERENCES = "configuration_references";

    /**
     * When persisting to the database, collection of attributes which should not be persisted into the ATTRIBUTES json
     * blob.
     */
    public static final Set<String> DATABASE_CLIENT_SEEDING_ATTRIBUTES = Set.of(
            CLIENT_ID, PROFILE_ID, NAME, ATTRIBUTES, STATUS, TAGS, META, ResourceAttributes.SCHEMAS
    );
    /**
     * When fetching from the database, collection of attributes that are used to fully populate a DatabaseClientAttributes.
     * They must be discarded from the Attributes source given to the DatabaseClient constructor.
     */
    public static final Set<String> DATABASE_CLIENT_INTERNAL_ATTRIBUTES = Set.of(
            PROFILE_ID, ATTRIBUTES, CONFIGURATION_REFERENCES, CREATED_DATE_COLUMN, UPDATED_COLUMN
    );

    private DatabaseClientAttributesHelper()
    {
    }

    // Reused & adapted from AttributesSqlHelper.databaseClientAttributesToParameters
    public static String configurationReferencesToJson(
            DatabaseClientAttributes clientAttributes,
            Json json)
    {
        Map<String, Object> configurationReferences = new HashMap<>(30);
        configurationReferences.put(SCOPES, clientAttributes.getScopes());
        configurationReferences.put(AUDIENCES, clientAttributes.getAudiences());
        configurationReferences.put(SUBJECT_TYPE, clientAttributes.getSubjectType());
        configurationReferences.put(CLAIM_MAPPER_ID, clientAttributes.getClaimMapperId());
        configurationReferences.put(USERINFO_SIGNED_ISSUER_ID, clientAttributes.getUserInfoSignedIssuerId());
        configurationReferences.put(REDIRECT_URI_VALIDATION_POLICY_ID, clientAttributes.getRedirectUriValidationPolicyId());

        @Nullable
        UserAuthenticationConfigAttributes userAuthenticationConfigAttributes = clientAttributes.getUserAuthentication();
        configurationReferences.put(REQUIRED_CLAIMS, NullUtils.map(
                userAuthenticationConfigAttributes, UserAuthenticationConfigAttributes::getRequiredClaims));
        configurationReferences.put(TEMPLATE_AREA,
                NullUtils.map(userAuthenticationConfigAttributes, u -> u.getTemplateArea().orElse(null)));
        configurationReferences.put(AUTHENTICATOR_FILTERS, NullUtils.map(
                userAuthenticationConfigAttributes, UserAuthenticationConfigAttributes::getAuthenticatorFilters));
        configurationReferences.put(BACKCHANNEL_LOGOUT_HTTP_CLIENT_ID, NullUtils.map(
                userAuthenticationConfigAttributes, u -> u.getHttpClient().orElse(null)));
        configurationReferences.put(CONSENTORS, Optional.ofNullable(userAuthenticationConfigAttributes)
                .flatMap(UserAuthenticationConfigAttributes::getConsent)
                .map(UserConsentAttributes::getConsentors)
                .orElse(null));

        ClientCapabilitiesAttributes capabilities = clientAttributes.getCapabilities();
        assert capabilities != null : "Database client has been previously validated and capabilities cannot be null here.";
        configurationReferences.put(ROPC_CREDENTIAL_MANAGER_ID, NullUtils.map(
                capabilities.getRopcCapability(), ClientCapabilitiesAttributes.RopcCapability::getCredentialManagerId));
        configurationReferences.put(AUTHENTICATORS, NullUtils.map(
                capabilities.getBackchannelCapability(), ClientCapabilitiesAttributes.BackchannelCapability::getAllowedAuthenticators));
        JwtAssertionAttributes jwtAssertionAttributes =
                capabilities.getAssertionCapability() == null || capabilities.getAssertionCapability().getJwtAssertion() == null
                        ? null : capabilities.getAssertionCapability().getJwtAssertion();
        JwtSigningAttributes jwtSigningAttributes = jwtAssertionAttributes != null ? jwtAssertionAttributes.getJwtSigning() : null;
        NullUtils.ifNotNull(jwtSigningAttributes, s -> s.matchAll(
                symmetricKey -> {
                }, // The symmetric key is not a reference but the encrypted actual encryption key.
                asymmetricKey -> configurationReferences.put(JWT_ASSERTION_ASYMMETRIC_KEY_ID, asymmetricKey),
                jwksUri -> configurationReferences.put(JWT_ASSERTION_JWKS_URI_CLIENT_ID,
                        NullUtils.map(jwksUri, JwksUri::httpClientId))));
        configurationReferences.put(HAAPI_POLICY_ID, NullUtils.map(capabilities.getHaapiCapability(), c -> c.getAttestation() == null
                ? null : c.getAttestation().getOptionalValue(ClientAttestationAttributes.POLICY_ID)));

        var method = clientAttributes.getClientAuthentication() == null || clientAttributes.getClientAuthentication().getPrimaryVerifier() == null
                ? null : clientAttributes.getClientAuthentication().getPrimaryVerifier().getClientAuthenticationMethod();
        configurationReferences.put(PRIMARY_CLIENT_AUTHENTICATION, NullUtils.map(method,
                m -> m.getClientAuthenticationType().name()));
        configurationReferences.put(PRIMARY_MUTUAL_TLS_CLIENT_CERTIFICATE_ID, NullUtils.map(method,
                m -> m instanceof ClientAuthenticationVerifier.MutualTlsVerifier mutualTls
                        && mutualTls.mutualTls().getMutualTls() instanceof MutualTlsAttributes.PinnedCertificate pinnedCertificate
                        ? pinnedCertificate.clientCertificate() : null));
        configurationReferences.put(PRIMARY_ASYMMETRIC_KEY_ID, NullUtils.map(method,
                m -> m instanceof ClientAuthenticationVerifier.AsymmetricKey asymmetricKey ? asymmetricKey.keyId() : null));
        configurationReferences.put(PRIMARY_CREDENTIAL_MANAGER_ID, NullUtils.map(method,
                m -> m instanceof ClientAuthenticationVerifier.CredentialManager credentialManager
                        ? credentialManager.credentialManagerId() : null));

        method = clientAttributes.getClientAuthentication() == null || clientAttributes.getClientAuthentication().getSecondaryVerifier() == null
                ? null : clientAttributes.getClientAuthentication().getSecondaryVerifier().getClientAuthenticationMethod();
        configurationReferences.put(SECONDARY_CLIENT_AUTHENTICATION, NullUtils.map(method,
                m -> m.getClientAuthenticationType().name()));
        configurationReferences.put(SECONDARY_MUTUAL_TLS_CLIENT_CERTIFICATE_ID, NullUtils.map(method,
                m -> m instanceof ClientAuthenticationVerifier.MutualTlsVerifier mutualTls
                        && mutualTls.mutualTls().getMutualTls() instanceof MutualTlsAttributes.PinnedCertificate pinnedCertificate
                        ? pinnedCertificate.clientCertificate() : null));
        configurationReferences.put(SECONDARY_ASYMMETRIC_KEY_ID, NullUtils.map(method,
                m -> m instanceof ClientAuthenticationVerifier.AsymmetricKey asymmetricKey ? asymmetricKey.keyId() : null));
        configurationReferences.put(SECONDARY_CREDENTIAL_MANAGER_ID, NullUtils.map(method,
                m -> m instanceof ClientAuthenticationVerifier.CredentialManager credentialManager
                        ? credentialManager.credentialManagerId() : null));

        configurationReferences.put(REQUEST_OBJECT_BY_REFERENCE_HTTP_CLIENT_ID,
                NullUtils.map(clientAttributes.getRequestObjectConfig(), r -> r.getByRefRequestObjectConfig() == null
                        ? null : r.getByRefRequestObjectConfig().getHttpClientId()));
        configurationReferences.put(ID_TOKEN_JWE_ENCRYPTION_KEY_ID,
                NullUtils.map(clientAttributes.getIdTokenConfig(), i -> i.getJweEncryptionConfig() == null
                        ? null : i.getJweEncryptionConfig().getEncryptionKeyId()));

        return json.toJson(configurationReferences);
    }

    // Reused & adapted from JdbcDatabaseClientDataAccessProvider.toResource
    public static DatabaseClientAttributes toResource(Attributes attributes,
                                                      ResourceQuery.AttributesEnumeration attributesEnumeration,
                                                      Json json)
    {
        Collection<String> multiValuedAttributes = Collections.emptySet();

        // All persistable attributes, are stored into the ATTRIBUTES attribute, non persistable attributes
        // are stored in dedicated columns and are not duplicated in ATTRIBUTES. The non persistable attributes are
        // the source attributes to inflate a DatabaseClient:
        // 1. Discard all attributes duplicated in ATTRIBUTES attribute.
        Attributes parsedAttributes = toResource(
                attributes,
                multiValuedAttributes,
                json,
                ATTRIBUTES);
        // 2. Promote all attributes nested in ATTRIBUTES property at top level.
        Attribute nestedAttributes = parsedAttributes.get(ATTRIBUTES);
        // The ATTRIBUTES attribute is known to be a MapAttributeValue and was parsed by the toResource(...) call above.
        //noinspection unchecked
        Attributes allAttributes = nestedAttributes == null
                ? parsedAttributes : parsedAttributes.with((Iterable<Attribute>) nestedAttributes.getAttributeValue());
        // 3. Add the "meta" attribute if it is enumerated.
        allAttributes = AttributesHelper.withMetaIfEnumerated(
                allAttributes,
                attributesEnumeration,
                DatabaseClientAttributes.RESOURCE_TYPE);
        // 4. Remove all persistence related attributes which are not needed anymore.
        allAttributes = allAttributes.removeAttributes(DATABASE_CLIENT_INTERNAL_ATTRIBUTES);

        return DatabaseClientAttributes.of(allAttributes);
    }

    // Reused & adapted from JdbcAttributesQueryHelper.toResource()
    public static Attributes toResource(Attributes attributes,
                                        Collection<String> multiValuedAttributes,
                                        Json json,
                                        String... extraAttributesHolderName)
    {
        Attributes extendedAttributes = attributes;

        for (String attributeName : extraAttributesHolderName)
        {
            @Nullable
            Attribute extraAttributesHolder = attributes.get(attributeName);
            if (extraAttributesHolder != null)
            {
                Attributes extraAttributes = json.toAttributes(extraAttributesHolder.getValueOfType(String.class));
                extendedAttributes = extendedAttributes.with(Attribute.of(attributeName, MapAttributeValue.of(extraAttributes)));
            }
        }

        return extendedAttributes.append(multiValuedAttributes.stream()
                .map(extendedAttributes::get)
                .filter(Objects::nonNull)
                .map(AttributesHelper::spaceSeparatedValuesToListAttributeValue)
                .collect(AttributeCollector.toAttributes()));
    }
}
