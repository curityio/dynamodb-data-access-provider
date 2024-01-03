/*
 *  Copyright 2021 Curity AB
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

package io.curity.identityserver.plugin.dynamodb.query

import io.curity.identityserver.plugin.dynamodb.DynamoDBAttribute
import io.curity.identityserver.plugin.dynamodb.DynamoDBItem
import se.curity.identityserver.sdk.data.query.Filter

/*
 * Classes and functions to represent and operate on expressions
 */

/**
 * Class hierarchy with the supported DynamoDB attribute operators.
 */

sealed class AttributeOperator {
    open val requiresPostQueryEvaluation = false
}

sealed class BinaryAttributeOperator : AttributeOperator() {
    abstract fun negate(): BinaryAttributeOperator
    abstract fun toDynamoOperator(left: String, right: String): String
    abstract fun eval(left: Any?, right: Any): Boolean

    object Eq : BinaryAttributeOperator() {
        override fun toString() = "Eq"
        override fun negate() = Ne
        override fun toDynamoOperator(left: String, right: String) = "$left = $right"
        override fun eval(left: Any?, right: Any) = left == right
    }

    object Ne : BinaryAttributeOperator() {
        override fun toString() = "Ne"
        override fun negate() = Eq
        override fun toDynamoOperator(left: String, right: String) = "$left <> $right"
        override fun eval(left: Any?, right: Any) = left != right
    }

    object Co : BinaryAttributeOperator() {
        override fun toString() = "Co"
        override fun negate() = NotCo
        override fun toDynamoOperator(left: String, right: String) = "contains($left,$right)"
        override fun eval(left: Any?, right: Any) = left is String && right is String && left.contains(right)
    }

    object NotCo : BinaryAttributeOperator() {
        override fun toString() = "NotCo"
        override fun negate() = Co
        override fun toDynamoOperator(left: String, right: String) = "NOT ${Co.toDynamoOperator(left, right)}"
        override fun eval(left: Any?, right: Any) = !Co.eval(left, right)
    }

    object Sw : BinaryAttributeOperator() {
        override fun toString() = "Sw"
        override fun negate() = NotSw
        override fun toDynamoOperator(left: String, right: String) = "begins_with($left, $right)"
        override fun eval(left: Any?, right: Any) = left is String && right is String && left.startsWith(right)
    }

    object NotSw : BinaryAttributeOperator() {
        override fun toString() = "NotSw"
        override fun negate() = Sw
        override fun toDynamoOperator(left: String, right: String) = "NOT ${Sw.toDynamoOperator(left, right)}"
        override fun eval(left: Any?, right: Any) = !Sw.eval(left, right)
    }

    object Ew : BinaryAttributeOperator() {
        override fun toString() = "Ew"
        override fun negate() = NotEw
        override fun toDynamoOperator(left: String, right: String) = "contains($left, $right)"
        override fun eval(left: Any?, right: Any) = left is String && right is String && left.endsWith(right)
        override val requiresPostQueryEvaluation = true
    }

    object NotEw : BinaryAttributeOperator() {
        override fun toString() = "NotEw"
        override fun negate() = Ew
        override fun toDynamoOperator(left: String, right: String) = "NOT ${Ew.toDynamoOperator(left, right)}"
        override fun eval(left: Any?, right: Any) = !Sw.eval(left, right)
        override val requiresPostQueryEvaluation = true
    }

    object Gt : BinaryAttributeOperator() {
        override fun toString() = "Gt"
        override fun negate() = Le
        override fun toDynamoOperator(left: String, right: String) = "$left > $right"
        override fun eval(left: Any?, right: Any) = compare(this, left, right) > 0
    }

    object Ge : BinaryAttributeOperator() {
        override fun toString() = "Ge"
        override fun negate() = Lt
        override fun toDynamoOperator(left: String, right: String) = "$left >= $right"
        override fun eval(left: Any?, right: Any) = compare(this, left, right) >= 0
    }

    object Lt : BinaryAttributeOperator() {
        override fun toString() = "Lt"
        override fun negate() = Ge
        override fun toDynamoOperator(left: String, right: String) = "$left < $right"
        override fun eval(left: Any?, right: Any) = compare(this, left, right) < 0
    }

    object Le : BinaryAttributeOperator() {
        override fun toString() = "Le"
        override fun negate() = Gt
        override fun toDynamoOperator(left: String, right: String) = "$left <= $right"
        override fun eval(left: Any?, right: Any) = compare(this, left, right) <= 0
    }
}

sealed class UnaryAttributeOperator : AttributeOperator() {
    abstract fun negate(): UnaryAttributeOperator
    abstract fun toDynamoOperator(left: String): String
    abstract fun eval(left: Any?): Boolean

    object Pr : UnaryAttributeOperator() {
        override fun toString() = "Pr"
        override fun negate() = NotPr
        override fun toDynamoOperator(left: String) = "attribute_exists($left)"
        override fun eval(left: Any?) = left != null
    }

    object NotPr : UnaryAttributeOperator() {
        override fun toString() = "NotPr"
        override fun negate() = Pr
        override fun toDynamoOperator(left: String) = "attribute_not_exists($left)"
        override fun eval(left: Any?) = !Pr.eval(left)
    }
}

fun operatorFrom(filterOperator: Filter.AttributeOperator) = when (filterOperator) {
    Filter.AttributeOperator.EQ -> BinaryAttributeOperator.Eq
    Filter.AttributeOperator.NE -> BinaryAttributeOperator.Ne
    Filter.AttributeOperator.CO -> BinaryAttributeOperator.Co
    Filter.AttributeOperator.SW -> BinaryAttributeOperator.Sw
    Filter.AttributeOperator.EW -> BinaryAttributeOperator.Ew
    Filter.AttributeOperator.PR -> UnaryAttributeOperator.Pr
    Filter.AttributeOperator.NP -> UnaryAttributeOperator.NotPr
    Filter.AttributeOperator.GT -> BinaryAttributeOperator.Gt
    Filter.AttributeOperator.GE -> BinaryAttributeOperator.Ge
    Filter.AttributeOperator.LT -> BinaryAttributeOperator.Lt
    Filter.AttributeOperator.LE -> BinaryAttributeOperator.Le
}

fun isUsableOnSortIndex(operator: AttributeOperator) = sortOperators.contains(operator)

fun compare(operator: AttributeOperator, left: Any?, right: Any) =
    if (left is String && right is String) {
        compareValues(left, right)
    } else if (left is Long && right is Long) {
        compareValues(left, right)
    } else {
        throw UnsupportedQueryException.InvalidOperandTypes(operator, left, right)
    }

/**
 * Class hierarchy with the supported DynamoDB binary logical operators.
 */
sealed class LogicalOperator {
    object And : LogicalOperator() {
        override fun toString() = "AND"
    }

    object Or : LogicalOperator() {
        override fun toString() = "OR"
    }

    companion object {
        fun from(operator: Filter.LogicalOperator) = when (operator) {
            Filter.LogicalOperator.AND -> And
            Filter.LogicalOperator.OR -> Or
        }
    }
}

/**
 * Class hierarchy to represent expression.
 */
sealed class Expression

sealed class AttributeExpression(
    open val attribute: DynamoDBAttribute<*>,
    open val operator: AttributeOperator
) : Expression() {
    abstract fun match(item: DynamoDBItem): Boolean
}

data class BinaryAttributeExpression(
    override val attribute: DynamoDBAttribute<*>,
    override val operator: BinaryAttributeOperator,
    val value: Any
) : AttributeExpression(attribute, operator) {
    override fun match(item: DynamoDBItem) = operator.eval(attribute.optionalFrom(item), value)
}

data class UnaryAttributeExpression(
    override val attribute: DynamoDBAttribute<*>,
    override val operator: UnaryAttributeOperator
) : AttributeExpression(attribute, operator) {
    override fun match(item: DynamoDBItem) = operator.eval(attribute.optionalFrom(item))
}

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
) {
    fun from(filter: Filter): Expression = when (filter) {
        is Filter.AttributeExpression -> lookupAttribute(filter.attributeName).let { attribute ->
            when (val operator = operatorFrom(filter.operator)) {
                is UnaryAttributeOperator -> UnaryAttributeExpression(attribute, operator)
                is BinaryAttributeOperator -> BinaryAttributeExpression(
                    attribute,
                    operator,
                    validateValue(attribute, filter.value)
                )
            }
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
        if (attribute.isValueCompatible(value)) {
            value
        } else {
            throw UnsupportedQueryException.InvalidValue(attribute.name)
        }

    private fun lookupAttribute(name: String): DynamoDBAttribute<*> =
        attributeMap[name] ?: throw UnsupportedQueryException.UnknownAttribute(name)
}

/**
 * A product is an AND of multiple terms, where each term is an [AttributeExpression]
 */
data class Product(val terms: Set<AttributeExpression>) {
    override fun toString() = terms.joinToString(".") { it.toString() }

    fun match(item: DynamoDBItem) = terms.all { it.match(item) }

    companion object {
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

fun requiresPostQueryEvaluation(prod: Product) = prod.terms.any { it.operator.requiresPostQueryEvaluation }
fun requiresPostQueryEvaluation(prods: Iterable<Product>) = prods.any { requiresPostQueryEvaluation(it) }

fun DynamoDBItem.matches(attributeExpression: AttributeExpression) = attributeExpression.match(this)
fun DynamoDBItem.matches(product: Product) = product.terms.all { this.matches(it) }
fun DynamoDBItem.matches(products: Iterable<Product>) = products.any { this.matches(it) }

fun Sequence<DynamoDBItem>.filterWith(products: Iterable<Product>) =
    if (requiresPostQueryEvaluation(products)) {
        this.filter { it.matches(products) }
    } else {
        this
    }

/**
 * Converts an [Expression] into the equivalent [DisjunctiveNormalForm]
 */

fun normalize(expr: Expression): DisjunctiveNormalForm = when (expr) {
    is AttributeExpression -> DisjunctiveNormalForm(setOf(Product(setOf(expr))))
    is LogicalExpression -> {
        val (l, o, r) = expr
        when (o) {
            is LogicalOperator.Or -> or(normalize(l), normalize(r))
            is LogicalOperator.And -> and(normalize(l), normalize(r))
        }
    }
    is NegationExpression -> normalize(negate(expr.inner))
}

/**
 * Negates an [Expression]
 */
fun negate(expr: Expression): Expression = when (expr) {
    is UnaryAttributeExpression -> {
        val (attr, operation) = expr
        UnaryAttributeExpression(attr, operation.negate())
    }
    is BinaryAttributeExpression -> {
        val (attr, operation, value) = expr
        BinaryAttributeExpression(attr, operation.negate(), value)
    }
    is LogicalExpression -> {
        val (l, o, r) = expr
        when (o) {
            is LogicalOperator.Or -> and(negate(l), negate(r))
            is LogicalOperator.And -> or(negate(l), negate(r))
        }
    }
    is NegationExpression -> expr.inner
}

private val sortOperators = setOf(
    BinaryAttributeOperator.Eq,
    BinaryAttributeOperator.Gt,
    BinaryAttributeOperator.Ge,
    BinaryAttributeOperator.Lt,
    BinaryAttributeOperator.Le,
    BinaryAttributeOperator.Sw
)