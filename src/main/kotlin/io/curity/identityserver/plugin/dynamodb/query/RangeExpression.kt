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

sealed class RangeExpression
{
    data class Binary(val attributeExpression: Expression.Attribute) : RangeExpression()
    data class Between(val attribute: DynamoDBAttribute<*>, val lower: Any, val higher: Any) : RangeExpression()
}

