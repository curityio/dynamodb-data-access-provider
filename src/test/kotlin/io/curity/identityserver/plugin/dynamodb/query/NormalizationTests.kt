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
                        AttributeExpression(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        AttributeExpression(USER_NAME, AttributeOperator.Eq, "alice")
                    ),
                    AttributeExpression(STATUS, AttributeOperator.Eq, "valid")
                ),
                DisjunctiveNormalForm(setOf(
                    Product.of(
                        AttributeExpression(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        AttributeExpression(STATUS, AttributeOperator.Eq, "valid")
                    ),
                    Product.of(
                        AttributeExpression(USER_NAME, AttributeOperator.Eq, "alice"),
                        AttributeExpression(STATUS, AttributeOperator.Eq, "valid")
                    )
                ))
            ),

            arrayOf(
                "(A || B) && !C",
                and(
                    or(
                        AttributeExpression(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        AttributeExpression(USER_NAME, AttributeOperator.Eq, "alice")
                    ),
                    negate(AttributeExpression(STATUS, AttributeOperator.Eq, "valid"))
                ),
                DisjunctiveNormalForm(setOf(
                    Product.of(
                        AttributeExpression(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        AttributeExpression(STATUS, AttributeOperator.Ne, "valid")
                    ),
                    Product.of(
                        AttributeExpression(USER_NAME, AttributeOperator.Eq, "alice"),
                        AttributeExpression(STATUS, AttributeOperator.Ne, "valid")
                    )
                ))
            ),
            arrayOf(
                "(A || B) && (C || D)",
                and(
                    or(
                        AttributeExpression(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        AttributeExpression(USER_NAME, AttributeOperator.Eq, "alice")
                    ),
                    or(
                        AttributeExpression(STATUS, AttributeOperator.Eq, "expired"),
                        AttributeExpression(STATUS, AttributeOperator.Eq, "revoked")
                    )
                ),
                DisjunctiveNormalForm(setOf(
                    Product.of(
                        AttributeExpression(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        AttributeExpression(STATUS, AttributeOperator.Eq, "expired")
                    ),
                    Product.of(
                        AttributeExpression(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        AttributeExpression(STATUS, AttributeOperator.Eq, "revoked")
                    ),
                    Product.of(
                        AttributeExpression(USER_NAME, AttributeOperator.Eq, "alice"),
                        AttributeExpression(STATUS, AttributeOperator.Eq, "expired")
                    ),
                    Product.of(
                        AttributeExpression(USER_NAME, AttributeOperator.Eq, "alice"),
                        AttributeExpression(STATUS, AttributeOperator.Eq, "revoked")
                    )
                ))
            ),

            arrayOf(
                "(A || B) && !(C || D)",
                and(
                    or(
                        AttributeExpression(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        AttributeExpression(USER_NAME, AttributeOperator.Eq, "alice")
                    ),
                    negate(or(
                        AttributeExpression(STATUS, AttributeOperator.Eq, "expired"),
                        AttributeExpression(STATUS, AttributeOperator.Eq, "revoked")
                    ))
                ),
                DisjunctiveNormalForm(setOf(
                    Product.of(
                        AttributeExpression(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        AttributeExpression(STATUS, AttributeOperator.Ne, "expired"),
                        AttributeExpression(STATUS, AttributeOperator.Ne, "revoked")
                    ),
                    Product.of(
                        AttributeExpression(USER_NAME, AttributeOperator.Eq, "alice"),
                        AttributeExpression(STATUS, AttributeOperator.Ne, "expired"),
                        AttributeExpression(STATUS, AttributeOperator.Ne, "revoked")
                    )
                ))
            )
        )
    }
}