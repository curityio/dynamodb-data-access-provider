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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.curity.identityserver.sdk.Nullable;
import se.curity.identityserver.sdk.attribute.Attribute;
import se.curity.identityserver.sdk.attribute.Attributes;
import se.curity.identityserver.sdk.attribute.ListAttributeValue;
import se.curity.identityserver.sdk.attribute.scim.v2.Meta;
import se.curity.identityserver.sdk.attribute.scim.v2.ResourceAttributes;
import se.curity.identityserver.sdk.data.query.ResourceQuery.AttributesEnumeration;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static se.curity.identityserver.sdk.attribute.client.database.DatabaseClientAttributes.DatabaseClientAttributeKeys.META;

public final class AttributesHelper
{
    private static final Logger _logger = LoggerFactory.getLogger(AttributesHelper.class);

    private AttributesHelper()
    {
    }

    // Reused from AttributesSqlHelper
    public static Attributes withMetaIfEnumerated(Attributes attributes,
                                                  AttributesEnumeration attributesEnumeration,
                                                  String resourceType)
    {
        Attributes returnAttributes = attributes;

        if (attributesEnumeration.keepAttribute(ResourceAttributes.META))
        {
            @Nullable
            Instant created = null, lastModified = null;

            if (attributes.contains(Meta.CREATED))
            {
                var metaCreated = String.format("%s.%s", META, Meta.CREATED);
                created = parseInstant(metaCreated, attributes.get(Meta.CREATED).getValue());
            }
            if (attributes.contains(Meta.LAST_MODIFIED))
            {
                var metaLastModified = String.format("%s.%s", META, Meta.LAST_MODIFIED);
                lastModified = parseInstant(metaLastModified, attributes.get(Meta.LAST_MODIFIED).getValue());
            }

            if (created != null || lastModified != null)
            {
                returnAttributes = returnAttributes.append(Attribute.of(ResourceAttributes.META,
                        Meta.of(resourceType, created, lastModified)));
            }
        }

        return returnAttributes.removeAttributes(Set.of(Meta.CREATED, Meta.LAST_MODIFIED));
    }

    // Reused from AttributesSqlHelper
    @Nullable
    private static Instant parseInstant(String name, Object dateTimeValue)
    {
        // database date-time values are normally stored as a TIMESTAMP, but we convert that to a
        // String in io.curity.identityserver.plugin.data.access.jdbc.jdbi.mappers.ResultSetAttributesMapper.toAttributeValue
        // because SCIM represents date-time as a String value.
        // Hence, we should get a String here... but for cases where the DAP decides to store epochSeconds (Long),
        // which is common in OAuth and derived specs, we handle that case too.
        if (dateTimeValue instanceof String str)
        {
            return LocalDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC);
        }
        else if (dateTimeValue instanceof Number number)
        {
            return Instant.ofEpochSecond(number.longValue());
        }

        _logger.debug("Attributes {} contained value of a type that cannot be converted to timestamp: '{}'",
                name, dateTimeValue);

        // ignore unknown types
        return null;
    }

    // Reused from AttributesSqlHelper
    static Attribute spaceSeparatedValuesToListAttributeValue(Attribute attribute)
    {
        @Nullable
        String valueAsString = attribute.getOptionalValueOfType(String.class);
        if (valueAsString != null)
        {
            String trimmedValueAsString = valueAsString.trim();

            // The empty string is an empty list, not a list containing the empty string.
            if (trimmedValueAsString.isEmpty())
            {
                return attribute.withValue(ListAttributeValue.of(Collections.emptyList()));
            }

            List<String> values = Arrays.asList(trimmedValueAsString.split("\\s+"));

            return attribute.withValue(ListAttributeValue.of(values));
        }

        return attribute;
    }
}
