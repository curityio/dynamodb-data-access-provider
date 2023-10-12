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

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.Nullable
import se.curity.identityserver.sdk.attribute.Attribute
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.attribute.ListAttributeValue
import se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.META
import se.curity.identityserver.sdk.attribute.scim.v2.Meta
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes
import se.curity.identityserver.sdk.data.query.ResourceQuery.AttributesEnumeration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter


object AttributesHelper {
    private val _logger: Logger = LoggerFactory.getLogger(AttributesHelper::class.java)

    @JvmStatic
    fun withMetaIfEnumerated(
        attributes: Attributes,
        attributesEnumeration: AttributesEnumeration,
        resourceType: String
    ): Attributes {
        var returnAttributes: Attributes = attributes
        if (attributesEnumeration.keepAttribute(ResourceAttributes.META)) {
            @Nullable var created: Instant? = null
            @Nullable var lastModified: Instant? = null
            if (attributes.contains(Meta.CREATED)) {
                val metaCreated = java.lang.String.format("%s.%s", META, Meta.CREATED)
                created = parseInstant(metaCreated, attributes.get(Meta.CREATED).value)
            }
            if (attributes.contains(Meta.LAST_MODIFIED)) {
                val metaLastModified = java.lang.String.format("%s.%s", META, Meta.LAST_MODIFIED)
                lastModified = parseInstant(metaLastModified, attributes.get(Meta.LAST_MODIFIED).value)
            }
            if (created != null || lastModified != null) {
                returnAttributes = returnAttributes.append(
                    Attribute.of(ResourceAttributes.META, Meta.of(resourceType, created, lastModified))
                )
            }
        }
        return returnAttributes.removeAttributes(setOf(Meta.CREATED, Meta.LAST_MODIFIED))
    }

    private fun parseInstant(name: String, dateTimeValue: Any): Instant? {
        // database date-time values are normally stored as a TIMESTAMP, but we convert that to a
        // String in io.curity.identityserver.plugin.data.access.jdbc.jdbi.mappers.ResultSetAttributesMapper.toAttributeValue
        // because SCIM represents date-time as a String value.
        // Hence, we should get a String here... but for cases where the DAP decides to store epochSeconds (Long),
        // which is common in OAuth and derived specs, we handle that case too.
        if (dateTimeValue is String) {
            return LocalDateTime.parse(dateTimeValue, DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC)
        } else if (dateTimeValue is Number) {
            return Instant.ofEpochSecond(dateTimeValue.toLong())
        }
        _logger.debug(
            "Attributes {} contained value of a type that cannot be converted to timestamp: '{}'",
            name, dateTimeValue
        )

        // ignore unknown types
        return null
    }

    @JvmStatic
    fun spaceSeparatedValuesToListAttributeValue(attribute: Attribute): Attribute {
        val valueAsString: String? = attribute.getOptionalValueOfType(String::class.java)
        if (valueAsString != null) {
            val trimmedValueAsString = valueAsString.trim { it <= ' ' }

            // The empty string is an empty list, not a list containing the empty string.
            if (trimmedValueAsString.isEmpty()) {
                return attribute.withValue(ListAttributeValue.of(emptyList<Any>()))
            }
            val values = listOf(*trimmedValueAsString.split("\\s+".toRegex()).dropLastWhile { it.isEmpty() }
                .toTypedArray())
            return attribute.withValue(ListAttributeValue.of(values))
        }
        return attribute
    }
}

