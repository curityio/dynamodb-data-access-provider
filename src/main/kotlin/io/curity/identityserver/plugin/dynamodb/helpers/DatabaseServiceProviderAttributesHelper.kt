/*
 *  Copyright 2025 Curity AB
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

package io.curity.identityserver.plugin.dynamodb.helpers

import io.curity.identityserver.plugin.dynamodb.helpers.AttributesHelper.withMetaIfEnumerated
import se.curity.identityserver.sdk.attribute.Attribute
import se.curity.identityserver.sdk.attribute.AttributeCollector
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.MapAttributeValue
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes
import se.curity.identityserver.sdk.attribute.serviceprovider.database.DatabaseServiceProviderAttributes
import se.curity.identityserver.sdk.attribute.serviceprovider.database.DatabaseServiceProviderAttributes.DatabaseServiceProviderAttributeKeys
import se.curity.identityserver.sdk.data.query.ResourceQuery.AttributesEnumeration
import se.curity.identityserver.sdk.service.Json
import java.util.Objects

object DatabaseServiceProviderAttributesHelper {

    const val SERVICE_PROVIDER_NAME_COLUMN = "service_provider_name"
    private const val CREATED_DATE_COLUMN = "created"
    private const val UPDATED_COLUMN = "updated"
    private const val TAGS = "tags"
    const val ATTRIBUTES = "attributes"
    const val PROFILE_ID = "profile_id"

    /**
     * When persisting to the database, collection of attributes which should not be persisted into the ATTRIBUTES json
     * blob.
     */
    val DATABASE_SERVICE_PROVIDER_SEEDING_ATTRIBUTES = setOf(
        DatabaseServiceProviderAttributeKeys.ID,
        PROFILE_ID,
        DatabaseServiceProviderAttributeKeys.NAME,
        ATTRIBUTES,
        DatabaseServiceProviderAttributeKeys.ENABLED,
        TAGS,
        DatabaseServiceProviderAttributeKeys.META,
        ResourceAttributes.SCHEMAS
    )

    /**
     * When fetching from the database, collection of attributes that are used to fully populate a DatabaseServiceProviderAttributes.
     * They must be discarded from the Attributes source given to the DatabaseServiceProvider constructor.
     */
    val DATABASE_SERVICE_PROVIDER_INTERNAL_ATTRIBUTES = setOf(
        PROFILE_ID, ATTRIBUTES, CREATED_DATE_COLUMN, UPDATED_COLUMN
    )

    fun toResource(
        attributes: Attributes,
        attributesEnumeration: AttributesEnumeration,
        json: Json
    ): DatabaseServiceProviderAttributes {
        val multiValuedAttributes: Collection<String> = emptySet()

        // All persistable attributes, are stored into the ATTRIBUTES attribute, non persistable attributes
        // are stored in dedicated columns and are not duplicated in ATTRIBUTES. The non persistable attributes are
        // the source attributes to inflate a DatabaseServiceProvider:
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
            withMetaIfEnumerated(allAttributes, attributesEnumeration, DatabaseServiceProviderAttributes.RESOURCE_TYPE)
        // 4. Remove all persistence related attributes which are not needed anymore.
        allAttributes =
            allAttributes.removeAttributes(DATABASE_SERVICE_PROVIDER_INTERNAL_ATTRIBUTES)
        return DatabaseServiceProviderAttributes.of(allAttributes)
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