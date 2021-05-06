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

sealed class Expression
{
    data class Attribute(
        val attribute: DynamoDBAttribute<*>,
        val operator: AttributeOperator,
        val value: Any
    ) : Expression()

    data class Logical(
        val left: Expression,
        val operator: LogicalOperator,
        val right: Expression
    ) : Expression()

    data class Negation(
        val inner: Expression
    ) : Expression()
}