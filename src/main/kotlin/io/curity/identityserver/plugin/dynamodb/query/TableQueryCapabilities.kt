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

package io.curity.identityserver.plugin.dynamodb.query

import io.curity.identityserver.plugin.dynamodb.DynamoDBAttribute

class TableQueryCapabilities(
    val indexes: List<Index>,
    val attributeMap: Map<String, DynamoDBAttribute<*>>
)
{
    companion object
    {
        fun attributeMapFrom(vararg attributes: DynamoDBAttribute<*>) =
            attributes.asSequence()
                .map{ it.name to it}
                .toMap()

    }
}
