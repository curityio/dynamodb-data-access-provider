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

data class Normal(val products: Set<Product>)

fun or(left: Normal, right: Normal) = Normal(left.products + right.products)
fun or(normalExpressions: List<Normal>) = Normal(normalExpressions.flatMap { it.products }.toSet())
fun and(left: Product, right: Normal) = Normal(right.products.map { and(it, left) }.toSet())
fun and(left: Normal, right: Normal) = or(left.products.map { and(it, right) })

fun normalOf(vararg products: Product) = Normal(setOf(*products))