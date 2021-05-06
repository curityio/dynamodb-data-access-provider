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

data class Product(val terms: Set<Expression.Attribute>) {
    override fun toString() = terms.joinToString(".") { it.toString() }
}

fun and(left: Product, right: Product) = Product(left.terms + right.terms)
fun or(left: Product, right: Product) = Normal(setOf(left, right))

fun productOf(vararg terms: Expression.Attribute) = Product(setOf(*terms))
