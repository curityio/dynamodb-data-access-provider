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

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(Parameterized::class)
class NormalizationTests(
    private val description:String,
    private val inputExpression: Expression,
    private val expectedNormalizedExpression: DisjunctiveNormalForm)
{

    @Test
    fun testNormalization(){
        assertEquals(expectedNormalizedExpression, normalize(inputExpression))
    }

    companion object {
        @JvmStatic
        @Parameterized.Parameters(name="{index}: {0}")
        fun where() = listOf(
            arrayOf(
                "(A || B) && C",
                and(
                    or(
                        BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                        BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice")
                    ),
                    BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "valid")
                ),
                DisjunctiveNormalForm(setOf(
                    Product.of(
                        BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "valid")
                    ),
                    Product.of(
                        BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "valid")
                    )
                ))
            ),

            arrayOf(
                "(A || B) && !C",
                and(
                    or(
                        BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                        BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice")
                    ),
                    negate(BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "valid"))
                ),
                DisjunctiveNormalForm(setOf(
                    Product.of(
                        BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Ne, "valid")
                    ),
                    Product.of(
                        BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Ne, "valid")
                    )
                ))
            ),
            arrayOf(
                "(A || B) && (C || D)",
                and(
                    or(
                        BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                        BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice")
                    ),
                    or(
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "expired"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "revoked")
                    )
                ),
                DisjunctiveNormalForm(setOf(
                    Product.of(
                        BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "expired")
                    ),
                    Product.of(
                        BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "revoked")
                    ),
                    Product.of(
                        BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "expired")
                    ),
                    Product.of(
                        BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "revoked")
                    )
                ))
            ),

            arrayOf(
                "(A || B) && !(C || D)",
                and(
                    or(
                        BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                        BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice")
                    ),
                    negate(or(
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "expired"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "revoked")
                    ))
                ),
                DisjunctiveNormalForm(setOf(
                    Product.of(
                        BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Ne, "expired"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Ne, "revoked")
                    ),
                    Product.of(
                        BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Ne, "expired"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Ne, "revoked")
                    )
                ))
            )
        )
    }
}