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

package io.curity.identityserver.plugin.dynamodb.helpers

import io.curity.identityserver.plugin.dynamodb.helpers.AttributesHelper.withMetaIfEnumerated
import se.curity.identityserver.sdk.attribute.Attribute
import se.curity.identityserver.sdk.attribute.AttributeCollector
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.MapAttributeValue
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes
import se.curity.identityserver.sdk.data.query.ResourceQuery.AttributesEnumeration
import se.curity.identityserver.sdk.service.Json
import java.util.Objects

object DatabaseClientAttributesHelper {

    const val CLIENT_NAME_COLUMN = "client_name"
    private const val CREATED_DATE_COLUMN = "created"
    private const val UPDATED_COLUMN = "updated"
    const val ID_TOKEN_JWE_ENCRYPTION_KEY_ID = "id_token_jwe_encryption_key_id"
    const val REQUEST_OBJECT_BY_REFERENCE_HTTP_CLIENT_ID = "request_object_by_reference_http_client_id"
    const val SECONDARY_CLIENT_AUTHENTICATION = "secondary_client_authentication"
    const val SECONDARY_CREDENTIAL_MANAGER_ID = "secondary_credential_manager_id"
    const val SECONDARY_ASYMMETRIC_KEY_ID = "secondary_asymmetric_key_id"
    const val SECONDARY_MUTUAL_TLS_CLIENT_CERTIFICATE_ID = "secondary_mutual_tls_client_certificate_id"
    const val PRIMARY_CLIENT_AUTHENTICATION = "primary_client_authentication"
    const val PRIMARY_CREDENTIAL_MANAGER_ID = "primary_credential_manager_id"
    const val PRIMARY_ASYMMETRIC_KEY_ID = "primary_asymmetric_key_id"
    const val PRIMARY_MUTUAL_TLS_CLIENT_CERTIFICATE_ID = "primary_mutual_tls_client_certificate_id"
    const val HAAPI_POLICY_ID = "haapi_policy_id"
    const val JWT_ASSERTION_JWKS_URI_CLIENT_ID = "jwt_assertion_jwks_uri_http_client_id"
    const val JWT_ASSERTION_ASYMMETRIC_KEY_ID = "jwt_assertion_asymmetric_key_id"
    const val ROPC_CREDENTIAL_MANAGER_ID = "resource_owner_password_credentials_credential_manager_id"
    const val BACKCHANNEL_LOGOUT_HTTP_CLIENT_ID = "backchannel_logout_http_client_id"
    const val CONFIGURATION_REFERENCES = "configuration_references"
    private const val TAGS = "tags"
    const val ATTRIBUTES = "attributes"
    const val PROFILE_ID = "profile_id"

    /**
     * When persisting to the database, collection of attributes which should not be persisted into the ATTRIBUTES json
     * blob.
     */
    val DATABASE_CLIENT_SEEDING_ATTRIBUTES = setOf(
        DatabaseClientAttributeKeys.CLIENT_ID,
        PROFILE_ID,
        DatabaseClientAttributeKeys.NAME,
        ATTRIBUTES,
        DatabaseClientAttributeKeys.STATUS,
        TAGS,
        DatabaseClientAttributeKeys.META,
        ResourceAttributes.SCHEMAS
    )

    /**
     * When fetching from the database, collection of attributes that are used to fully populate a DatabaseClientAttributes.
     * They must be discarded from the Attributes source given to the DatabaseClient constructor.
     */
    val DATABASE_CLIENT_INTERNAL_ATTRIBUTES = setOf(
        PROFILE_ID, ATTRIBUTES, CREATED_DATE_COLUMN, UPDATED_COLUMN
    )

    fun toResource(
        attributes: Attributes,
        attributesEnumeration: AttributesEnumeration,
        json: Json
    ): DatabaseClientAttributes {
        val multiValuedAttributes: Collection<String> = emptySet<String>()

        // All persistable attributes, are stored into the ATTRIBUTES attribute, non persistable attributes
        // are stored in dedicated columns and are not duplicated in ATTRIBUTES. The non persistable attributes are
        // the source attributes to inflate a DatabaseClient:
        // 1. Discard all attributes duplicated in ATTRIBUTES attribute.
        val parsedAttributes = toResource(attributes, multiValuedAttributes, json, ATTRIBUTES)
        // 2. Promote all attributes nested in ATTRIBUTES property at top level.
        val nestedAttributes = parsedAttributes[ATTRIBUTES]
        // The ATTRIBUTES attribute is known to be a MapAttributeValue and was parsed by the toResource(...) call above.
        var allAttributes = if (nestedAttributes == null) {
            parsedAttributes
        } else {
            assert(nestedAttributes.attributeValue is Iterable<*>)
            parsedAttributes.with(nestedAttributes.attributeValue as Iterable<Attribute>)
        }
        // 3. Add the "meta" attribute if it is enumerated.
        allAttributes =
            withMetaIfEnumerated(allAttributes, attributesEnumeration, DatabaseClientAttributes.RESOURCE_TYPE)
        // 4. Remove all persistence related attributes which are not needed anymore.
        allAttributes =
            allAttributes.removeAttributes(DATABASE_CLIENT_INTERNAL_ATTRIBUTES)
        return DatabaseClientAttributes.of(allAttributes)
    }

    private fun toResource(
        attributes: Attributes,
        multiValuedAttributes: Collection<String>,
        json: Json,
        vararg extraAttributesHolderName: String
    ): Attributes {
        var extendedAttributes = attributes
        for (attributeName in extraAttributesHolderName) {
            val extraAttributesHolder: Attribute? = attributes[attributeName]
            if (extraAttributesHolder != null) {
                val extraAttributes = json.toAttributes(extraAttributesHolder.getValueOfType(String::class.java))
                extendedAttributes =
                    extendedAttributes.with(Attribute.of(attributeName, MapAttributeValue.of(extraAttributes)))
            }
        }
        return extendedAttributes.append(multiValuedAttributes.stream()
            .map { name: String -> extendedAttributes[name] }
            .filter { obj: Attribute -> Objects.nonNull(obj) }
            .map { obj: Attribute -> AttributesHelper.spaceSeparatedValuesToListAttributeValue(obj) }
            .collect(AttributeCollector.toAttributes())
        )
    }
}