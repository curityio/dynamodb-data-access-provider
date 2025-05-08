/*
 *  Copyright 2025 Curity AB
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

package io.curity.identityserver.plugin.dynamodb

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class AttributeValueUtilsTests {

    @Test
    fun `can map strings, numbers, booleans, null, lists, and maps to AttributeValue`() {
        // given:
        val valueToMap = mapOf(
            "string" to "value",
            "number" to 42,
            "boolean" to true,
            "null" to null,
            "list" to listOf(
                "string",
                42,
                false,
                null,
                listOf(
                    "string",
                    42,
                    mapOf(
                        "string" to "string"
                    )
                )
            ),
            "map" to mapOf(
                "string" to "value",
                "number" to 42,
                "boolean" to true,
                "null" to null,
                "list" to listOf(
                    "string",
                    42,
                    false,
                    null,
                    listOf(
                        "string",
                        42,
                        mapOf(
                            "string" to "string"
                        )
                    )
                ),
            )
        )

        // when:
        val attributeValue = mapToAttributeValue(valueToMap)

        // then:
        val expected = AttributeValue.fromM(
            mapOf(
                "string" to AttributeValue.fromS("value"),
                "number" to AttributeValue.fromN("42"),
                "boolean" to AttributeValue.fromBool(true),
                "null" to AttributeValue.fromNul(true),
                "list" to AttributeValue.builder().l(
                    listOf(
                        AttributeValue.fromS("string"),
                        AttributeValue.fromN("42"),
                        AttributeValue.fromBool(false),
                        AttributeValue.fromNul(true),
                        AttributeValue.builder().l(
                            listOf(
                                AttributeValue.fromS("string"),
                                AttributeValue.fromN("42"),
                                AttributeValue.builder().m(
                                    mapOf(
                                        "string" to AttributeValue.fromS("string")
                                    )
                                ).build()
                            )
                        ).build()
                    )
                ).build(),
                "map" to AttributeValue.builder().m(
                    mapOf(
                        "string" to AttributeValue.fromS("value"),
                        "number" to AttributeValue.fromN("42"),
                        "boolean" to AttributeValue.fromBool(true),
                        "null" to AttributeValue.fromNul(true),
                        "list" to AttributeValue.builder().l(
                            listOf(
                                AttributeValue.fromS("string"),
                                AttributeValue.fromN("42"),
                                AttributeValue.fromBool(false),
                                AttributeValue.fromNul(true),
                                AttributeValue.builder().l(
                                    listOf(
                                        AttributeValue.fromS("string"),
                                        AttributeValue.fromN("42"),
                                        AttributeValue.builder().m(
                                            mapOf(
                                                "string" to AttributeValue.fromS("string")
                                            )
                                        ).build()
                                    )
                                ).build()
                            )
                        ).build()
                    )
                ).build()
            )
        )
        assertEquals(expected, attributeValue)
    }

    @Test
    fun `can index using a map into nested maps`() {
        // given: a nested map
        val valueToMap = mapOf(
            "string" to "value",
            "number" to 42,
            "boolean" to true,
            "null" to null,
            "map" to mapOf(
                "string" to "value",
                "number" to 42,
                "boolean" to true,
                "null" to null,
                "list" to listOf(
                    "string",
                    42,
                    false,
                ),
                "nestedMap" to mapOf(
                    "number" to 42
                )
            )
        )

        // when: retrieving the value for an existing path, then: the value is the expected one
        assertEquals("value", valueToMap.getMapValueForPath(listOf("string")))
        assertEquals(42, valueToMap.getMapValueForPath(listOf("number")))
        assertEquals(true, valueToMap.getMapValueForPath(listOf("boolean")))
        assertEquals(null, valueToMap.getMapValueForPath(listOf("null")))

        assertEquals("value", valueToMap.getMapValueForPath(listOf("map", "string")))
        assertEquals(42, valueToMap.getMapValueForPath(listOf("map", "number")))
        assertEquals(true, valueToMap.getMapValueForPath(listOf("map", "boolean")))
        assertEquals(listOf("string", 42, false), valueToMap.getMapValueForPath(listOf("map", "list")))
        assertEquals(42, valueToMap.getMapValueForPath(listOf("map", "nestedMap", "number")))

        // when: retrieving the value for a non-existing path, then: the value is the expected one
        assertEquals(null, valueToMap.getMapValueForPath(listOf("")))
        assertEquals(null, valueToMap.getMapValueForPath(listOf("", "")))
        assertEquals(null, valueToMap.getMapValueForPath(listOf()))
        assertEquals(null, valueToMap.getMapValueForPath(listOf("map", "")))
        assertEquals(null, valueToMap.getMapValueForPath(listOf("map", "number", "foo")))
        assertEquals(null, valueToMap.getMapValueForPath(listOf("map", ".")))
        assertEquals(null, valueToMap.getMapValueForPath(listOf("map", "does-not-exist")))
    }
}