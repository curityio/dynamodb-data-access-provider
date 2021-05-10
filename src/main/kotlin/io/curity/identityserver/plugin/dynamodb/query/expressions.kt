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
import se.curity.identityserver.sdk.data.query.Filter

/*
 * Classes and functions to represent and operate on expressions
 */

/**
 * Class hierarchy with the supported DynamoDB attribute operators.
 */
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

/**
 * Class hierarchy with the supported DynamoDB binary logical operators.
 */
sealed class LogicalOperator
{
    object And : LogicalOperator()
    {
        override fun toString() = "AND"
    }

    object Or : LogicalOperator()
    {
        override fun toString() = "OR"
    }

    companion object
    {
        fun from(operator: Filter.LogicalOperator) = when (operator)
        {
            Filter.LogicalOperator.AND -> And
            Filter.LogicalOperator.OR -> Or
        }
    }
}

/**
 * Class hierarchy to represent expression.
 */
sealed class Expression

data class AttributeExpression(
    val attribute: DynamoDBAttribute<*>,
    val operator: AttributeOperator,
    val value: Any
) : Expression()

data class LogicalExpression(
    val left: Expression,
    val operator: LogicalOperator,
    val right: Expression
) : Expression()

data class NegationExpression(
    val inner: Expression
) : Expression()

/**
 * Maps an SDK filter expression into a [Expression]
 */
class ExpressionMapper(
    /** The map to use when mapping from attribute names to [DynamoDBAttribute] instances */
    private val attributeMap: Map<String, DynamoDBAttribute<*>>
)
{
    fun from(filter: Filter): Expression = when (filter)
    {
        is Filter.AttributeExpression -> lookupAttribute(filter.attributeName).let { attribute ->
            AttributeExpression(
                attribute,
                AttributeOperator.from(filter.operator),
                validateValue(attribute, filter.value)
            )
        }
        is Filter.LogicalExpression -> LogicalExpression(
            from(filter.leftHandFilter),
            LogicalOperator.from(filter.operator),
            from(filter.rightHandFilter)

        )
        is Filter.NotExpression -> NegationExpression(from(filter.filter))
        else -> throw UnsupportedQueryException.UnsupportedFilterType(filter)
    }

    private fun validateValue(attribute: DynamoDBAttribute<*>, value: Any) =
        if (attribute.isValueCompatible(value))
        {
            value
        } else
        {
            throw UnsupportedQueryException.InvalidValue(attribute.name, value)
        }

    private fun lookupAttribute(name: String): DynamoDBAttribute<*> =
        attributeMap[name] ?: throw UnsupportedQueryException.UnknownAttribute(name)
}

/**
 * A product is an AND of multiple terms, where each term is an [AttributeExpression]
 */
data class Product(val terms: Set<AttributeExpression>)
{
    override fun toString() = terms.joinToString(".") { it.toString() }

    companion object
    {
        fun of(vararg terms: AttributeExpression) = Product(setOf(*terms))
    }
}

/**
 * Canonical representation via a Disjunctive Normal Form,
 * - i.e. a sum of products.
 * - i.e. an OR of ANDs.
 */
data class DisjunctiveNormalForm(val products: Set<Product>)

/*
 * Boolean operators over generic expressions, products, and normal forms
 */
fun and(left: Product, right: Product) = Product(left.terms + right.terms)
fun and(left: Product, right: DisjunctiveNormalForm) =
    DisjunctiveNormalForm(right.products.map { and(it, left) }.toSet())

fun and(left: DisjunctiveNormalForm, right: DisjunctiveNormalForm) = or(left.products.map { and(it, right) })
fun and(left: Expression, right: Expression) = LogicalExpression(left, LogicalOperator.And, right)

fun or(left: Product, right: Product) = DisjunctiveNormalForm(setOf(left, right))
fun or(left: DisjunctiveNormalForm, right: DisjunctiveNormalForm) =
    DisjunctiveNormalForm(left.products + right.products)

fun or(normalExpressions: List<DisjunctiveNormalForm>) =
    DisjunctiveNormalForm(normalExpressions.flatMap { it.products }.toSet())

fun or(left: Expression, right: Expression) = LogicalExpression(left, LogicalOperator.Or, right)

/**
 * Converts an [Expression] into the equivalent [DisjunctiveNormalForm]
 */
fun normalize(expr: Expression): DisjunctiveNormalForm = when (expr)
{
    is AttributeExpression -> DisjunctiveNormalForm(setOf(Product(setOf(expr))))
    is LogicalExpression ->
    {
        val (l, o, r) = expr
        when (o)
        {
            is LogicalOperator.Or -> or(normalize(l), normalize(r))
            is LogicalOperator.And -> and(normalize(l), normalize(r))
        }
    }
    is NegationExpression -> normalize(negate(expr.inner))
}

/**
 * Negates an [Expression]
 */
fun negate(expr: Expression): Expression = when (expr)
{
    is AttributeExpression ->
    {
        val (attr, operation, value) = expr
        AttributeExpression(attr, operation.negate(), value)
    }
    is LogicalExpression ->
    {
        val (l, o, r) = expr
        when (o)
        {
            is LogicalOperator.Or -> and(negate(l), negate(r))
            is LogicalOperator.And -> or(negate(l), negate(r))
        }
    }
    is NegationExpression -> expr.inner
}

private val sortOperators = setOf(
    AttributeOperator.Eq,
    AttributeOperator.Gt,
    AttributeOperator.Ge,
    AttributeOperator.Lt,
    AttributeOperator.Le,
    AttributeOperator.Sw
)