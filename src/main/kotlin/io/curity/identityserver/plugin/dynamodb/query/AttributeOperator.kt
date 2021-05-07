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

import se.curity.identityserver.sdk.data.query.Filter

sealed class AttributeOperator
{
    abstract fun negate(): AttributeOperator
    abstract fun toDynamoOperator(left: String, right: String): String

    object Eq : AttributeOperator()
    {
        override fun toString() = "Eq"
        override fun negate() = Ne
        override fun toDynamoOperator(left: String, right: String) = "$left = $right"
    }

    object Ne : AttributeOperator()
    {
        override fun toString() = "Ne"
        override fun negate() = Eq
        override fun toDynamoOperator(left: String, right: String) = "$left <> $right"
    }

    object Co : AttributeOperator()
    {
        override fun toString() = "Co"
        override fun negate() = NotCo
        override fun toDynamoOperator(left: String, right: String) = "contains($left,$right)"
    }

    object NotCo : AttributeOperator()
    {
        override fun toString() = "NotCo"
        override fun negate() = Co
        override fun toDynamoOperator(left: String, right: String) = "NOT ${Co.toDynamoOperator(left, right)}"
    }

    object Sw : AttributeOperator()
    {
        override fun toString() = "Sw"
        override fun negate() = NotSw
        override fun toDynamoOperator(left: String, right: String) = "begins_with($left, $right)"
    }

    object NotSw : AttributeOperator()
    {
        override fun toString() = "NotSw"
        override fun negate() = Sw
        override fun toDynamoOperator(left: String, right: String) = "NOT ${Sw.toDynamoOperator(left, right)}"
    }

    object Pr : AttributeOperator()
    {
        override fun toString() = "Pr"
        override fun negate() = NotPr
        override fun toDynamoOperator(left: String, right: String) = "attribute_exists($left)"
    }

    object NotPr : AttributeOperator()
    {
        override fun toString() = "NotPr"
        override fun negate() = Pr
        override fun toDynamoOperator(left: String, right: String) = "attribute_not_exists($left)"
    }

    object Gt : AttributeOperator()
    {
        override fun toString() = "Gt"
        override fun negate() = Le
        override fun toDynamoOperator(left: String, right: String) = "$left > $right"
    }

    object Ge : AttributeOperator()
    {
        override fun toString() = "Ge"
        override fun negate() = Lt
        override fun toDynamoOperator(left: String, right: String) = "$left >= $right"
    }

    object Lt : AttributeOperator()
    {
        override fun toString() = "Lt"
        override fun negate() = Ge
        override fun toDynamoOperator(left: String, right: String) = "$left < $right"
    }

    object Le : AttributeOperator()
    {
        override fun toString() = "Le"
        override fun negate() = Gt
        override fun toDynamoOperator(left: String, right: String) = "$left <= $right"
    }

    companion object
    {
        fun isUsableOnSortIndex(operator: AttributeOperator) = sortOperators.contains(operator)

        fun from(filterOperator: Filter.AttributeOperator) = when (filterOperator)
        {
            Filter.AttributeOperator.EQ -> Eq
            Filter.AttributeOperator.NE -> Ne
            Filter.AttributeOperator.CO -> Co
            Filter.AttributeOperator.SW -> Sw
            Filter.AttributeOperator.EW -> throw UnsupportedQueryException.UnsupportedOperator(Filter.AttributeOperator.EW)
            Filter.AttributeOperator.PR -> Pr
            Filter.AttributeOperator.GT -> Gt
            Filter.AttributeOperator.GE -> Ge
            Filter.AttributeOperator.LT -> Lt
            Filter.AttributeOperator.LE -> Le
        }
    }
}

private val sortOperators = setOf(
    AttributeOperator.Eq,
    AttributeOperator.Gt,
    AttributeOperator.Ge,
    AttributeOperator.Lt,
    AttributeOperator.Le,
    AttributeOperator.Sw
)
