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

import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

/**
 * Map any value to a DynamoDB [AttributeValue]
 *
 * @param value the value to map into a DynamoDB [AttributeValue]
 * @return the mapped [AttributeValue] of `null` if such mapping is not possible
 */
fun mapToAttributeValue(value: Any?): AttributeValue? =
    when (value) {
        is String -> AttributeValue.fromS(value)
        is Number -> AttributeValue.fromN(value.toString())
        is Boolean -> AttributeValue.fromBool(value)
        null -> AttributeValue.fromNul(true)
        is Collection<*> -> {
            val list = value.mapNotNull { item ->
                val attribute = mapToAttributeValue(item)
                    ?: return@mapNotNull null.also {
                        logger.debug("Unsupported value: {}", item?.javaClass?.simpleName)
                    }
                attribute
            }
            AttributeValue.builder().l(list).build()
        }

        is Map<*, *> -> {
            val map = value.mapNotNull { (k, v) ->
                val key = k as? String
                    ?: return@mapNotNull null.also {
                        logger.debug("Unsupported key: {}", k?.javaClass?.simpleName)
                    }
                val attribute = mapToAttributeValue(v)
                    ?: return@mapNotNull null.also {
                        logger.debug("Unsupported value: {}", v?.javaClass?.simpleName)
                    }
                key to attribute
            }.toMap()
            AttributeValue.builder().m(map).build()
        }

        else -> {
            logger.debug("Unsupported value: {}", value.javaClass.simpleName)
            null
        }
    }

/**
 * Given a [Map] and a path defined as a list of [String], uses the path to recursively index inside the map
 * and retrieve the indexed value. Returns `null` if there isn't any value with the provided path.
 */
fun Map<String, *>.getMapValueForPath(pathNames: List<String>): Any? {
    if (pathNames.isEmpty()) {
        logger.debug("Trying to index with an empty path, that's probably an error")
        return null
    }
    return pathNames.fold<String, Any>(this) { acc, name ->
        val map = acc as? Map<*, *>
            ?: return null.also {
                logger.debug("Cannot retrieve map entry for key '{}' because target is not a map", name)
            }
        val entry = map[name]
            ?: return null.also {
                logger.debug("Cannot retrieve map entry for key '{}' because target does not have it", name)
            }
        entry
    }
}

private val logger = LoggerFactory.getLogger("AttributeValueUtils")