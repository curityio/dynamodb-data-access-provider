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

import io.curity.identityserver.plugin.dynamodb.DynamoDBAttribute
import io.curity.identityserver.plugin.dynamodb.DynamoDBDialect
import se.curity.identityserver.sdk.datasource.db.TableCapabilities
import se.curity.identityserver.sdk.datasource.db.TableCapabilities.TableCapability

open class TableQueryCapabilities(
    val indexes: List<Index>,
    val attributeMap: Map<String, DynamoDBAttribute<*>>,
    unsupportedCapabilities: Set<TableCapability> = emptySet()
) : TableCapabilities {

    val dialect = DynamoDBDialect()
    private val unsupportedCapabilities = dialect.unsupportedCapabilities + unsupportedCapabilities

    override fun getUnsupported(): Set<TableCapability> = unsupportedCapabilities

    // -1 as 'indexes' includes also the table's main index
    open fun getGsiCount() = indexes.size - 1

    open fun getLsiCount() = 0

    open fun getMappedAttributeName(attributeName: String) = attributeMap[attributeName]?.name
}
