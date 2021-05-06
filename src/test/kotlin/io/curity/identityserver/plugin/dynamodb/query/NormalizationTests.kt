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
    private val expectedNormalizedExpression: Normal)
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
                        Expression.Attribute(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        Expression.Attribute(USER_NAME, AttributeOperator.Eq, "alice")
                    ),
                    Expression.Attribute(STATUS, AttributeOperator.Eq, "valid")
                ),
                Normal(setOf(
                    productOf(
                        Expression.Attribute(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        Expression.Attribute(STATUS, AttributeOperator.Eq, "valid")
                    ),
                    productOf(
                        Expression.Attribute(USER_NAME, AttributeOperator.Eq, "alice"),
                        Expression.Attribute(STATUS, AttributeOperator.Eq, "valid")
                    )
                ))
            ),

            arrayOf(
                "(A || B) && !C",
                and(
                    or(
                        Expression.Attribute(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        Expression.Attribute(USER_NAME, AttributeOperator.Eq, "alice")
                    ),
                    negate(Expression.Attribute(STATUS, AttributeOperator.Eq, "valid"))
                ),
                Normal(setOf(
                    productOf(
                        Expression.Attribute(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        Expression.Attribute(STATUS, AttributeOperator.Ne, "valid")
                    ),
                    productOf(
                        Expression.Attribute(USER_NAME, AttributeOperator.Eq, "alice"),
                        Expression.Attribute(STATUS, AttributeOperator.Ne, "valid")
                    )
                ))
            ),
            arrayOf(
                "(A || B) && (C || D)",
                and(
                    or(
                        Expression.Attribute(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        Expression.Attribute(USER_NAME, AttributeOperator.Eq, "alice")
                    ),
                    or(
                        Expression.Attribute(STATUS, AttributeOperator.Eq, "expired"),
                        Expression.Attribute(STATUS, AttributeOperator.Eq, "revoked")
                    )
                ),
                Normal(setOf(
                    productOf(
                        Expression.Attribute(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        Expression.Attribute(STATUS, AttributeOperator.Eq, "expired")
                    ),
                    productOf(
                        Expression.Attribute(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        Expression.Attribute(STATUS, AttributeOperator.Eq, "revoked")
                    ),
                    productOf(
                        Expression.Attribute(USER_NAME, AttributeOperator.Eq, "alice"),
                        Expression.Attribute(STATUS, AttributeOperator.Eq, "expired")
                    ),
                    productOf(
                        Expression.Attribute(USER_NAME, AttributeOperator.Eq, "alice"),
                        Expression.Attribute(STATUS, AttributeOperator.Eq, "revoked")
                    )
                ))
            ),

            arrayOf(
                "(A || B) && !(C || D)",
                and(
                    or(
                        Expression.Attribute(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        Expression.Attribute(USER_NAME, AttributeOperator.Eq, "alice")
                    ),
                    negate(or(
                        Expression.Attribute(STATUS, AttributeOperator.Eq, "expired"),
                        Expression.Attribute(STATUS, AttributeOperator.Eq, "revoked")
                    ))
                ),
                Normal(setOf(
                    productOf(
                        Expression.Attribute(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        Expression.Attribute(STATUS, AttributeOperator.Ne, "expired"),
                        Expression.Attribute(STATUS, AttributeOperator.Ne, "revoked")
                    ),
                    productOf(
                        Expression.Attribute(USER_NAME, AttributeOperator.Eq, "alice"),
                        Expression.Attribute(STATUS, AttributeOperator.Ne, "expired"),
                        Expression.Attribute(STATUS, AttributeOperator.Ne, "revoked")
                    )
                ))
            )
        )
    }
}