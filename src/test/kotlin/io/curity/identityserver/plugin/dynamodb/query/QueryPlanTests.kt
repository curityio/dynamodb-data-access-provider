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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

class QueryPlanTests
{
    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("where")
    fun testQueryPlan(
        description: String,
        indexes: List<Index>,
        inputExpression: Expression,
        expectedQueryPlan: QueryPlan
    ) {
        val queryPlanner = QueryPlanner(TableQueryCapabilities(indexes, mapOf()))
        assertEquals(expectedQueryPlan, queryPlanner.build(inputExpression))
    }

    companion object {
        private val emailIndex = Index("email-index", EMAIL)
        private val userNameIndex = Index("userName-index", USER_NAME)

        @JvmStatic
        fun where() = listOf(
            arrayOf(
                "(A || B) && C",
                listOf(emailIndex, userNameIndex),
                and(
                    or(
                        BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                        BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice")
                    ),
                    BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "valid")
                ),
                QueryPlan.UsingQueries(
                    mapOf(
                        QueryPlan.KeyCondition(
                            emailIndex,
                            BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com")
                        )
                                to listOf(
                            Product.of(
                                BinaryAttributeExpression(
                                    STATUS,
                                    BinaryAttributeOperator.Eq,
                                    "valid"
                                )
                            )
                        ),
                        QueryPlan.KeyCondition(
                            userNameIndex,
                            BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice")
                        )
                                to listOf(
                            Product.of(
                                BinaryAttributeExpression(
                                    STATUS,
                                    BinaryAttributeOperator.Eq,
                                    "valid"
                                )
                            )
                        )

                    )
                )
            ),
            arrayOf(
                "(A || B) && (C || D)",
                listOf(emailIndex, userNameIndex),
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
                QueryPlan.UsingQueries(
                    mapOf(
                        QueryPlan.KeyCondition(
                            emailIndex,
                            BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com")
                        )
                                to listOf(
                            Product.of(BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "expired")),
                            Product.of(BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "revoked"))
                        ),
                        QueryPlan.KeyCondition(
                            userNameIndex,
                            BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice")
                        )
                                to listOf(
                            Product.of(BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "expired")),
                            Product.of(BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "revoked"))
                        )

                    )
                )
            )
        )
    }
}
