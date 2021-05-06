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

fun and(left: Expression, right: Expression) = Expression.Logical(left, LogicalOperator.And, right)
fun or(left: Expression, right: Expression) = Expression.Logical(left, LogicalOperator.Or, right)

fun normalize(expr: Expression): Normal = when (expr)
{
    is Expression.Attribute -> Normal(setOf(Product(setOf(expr))))
    is Expression.Logical ->
    {
        val (l, o, r) = expr
        when (o)
        {
            is LogicalOperator.Or -> or(normalize(l), normalize(r))
            is LogicalOperator.And -> and(normalize(l), normalize(r))
        }
    }
    is Expression.Negation -> normalize(negate(expr.inner))
}

fun negate(expr: Expression): Expression = when (expr)
{
    is Expression.Attribute ->
    {
        val (attr, operation, value) = expr
        Expression.Attribute(attr, operation.negate(), value)
    }
    is Expression.Logical ->
    {
        val (l, o, r) = expr
        when (o)
        {
            is LogicalOperator.Or -> and(negate(l), negate(r))
            is LogicalOperator.And -> or(negate(l), negate(r))
        }
    }
    is Expression.Negation -> expr.inner
}

