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

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(Parameterized::class)
class NormalizationTests(
    private val description: String,
    private val inputExpression: Expression,
    private val expectedNormalizedExpression: DisjunctiveNormalForm
) {

    @Test
    fun testNormalization() {
        assertEquals(expectedNormalizedExpression, normalize(inputExpression))
    }

    companion object {
        @JvmStatic
        @Parameterized.Parameters(name = "{index}: {0}")
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
                DisjunctiveNormalForm(
                    setOf(
                        Product.of(
                            BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                            BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "valid")
                        ),
                        Product.of(
                            BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice"),
                            BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "valid")
                        )
                    )
                )
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
                DisjunctiveNormalForm(
                    setOf(
                        Product.of(
                            BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                            BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Ne, "valid")
                        ),
                        Product.of(
                            BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice"),
                            BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Ne, "valid")
                        )
                    )
                )
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
                DisjunctiveNormalForm(
                    setOf(
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
                    )
                )
            ),

            arrayOf(
                "(A || B) && !(C || D)",
                and(
                    or(
                        BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                        BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice")
                    ),
                    negate(
                        or(
                            BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "expired"),
                            BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "revoked")
                        )
                    )
                ),
                DisjunctiveNormalForm(
                    setOf(
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
                    )
                )
            )
        )
    }
}