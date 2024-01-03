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

package io.curity.identityserver.plugin.dynamodb

import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import io.curity.identityserver.plugin.dynamodb.query.TableQueryCapabilities
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.Delete
import software.amazon.awssdk.services.dynamodb.model.Put
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

/*
 * A DynamoDB item (i.e. a table row) is represented by a Map
 * - from attribute name string
 * - to attribute value
 */
typealias DynamoDBItem = Map<String, AttributeValue>
typealias MutableDynamoDBItem = MutableMap<String, AttributeValue>

/*
 * Set of types and functions to represent and work with tables and table attributes
 */

enum class AttributeType(val typeName: String) {
    S("S"),
    N("N"),
    L("L")
}

// A DynamoDB table
abstract class Table(
    private val suffixName: String
) {
    fun name(configuration: DynamoDBDataAccessProviderConfiguration) = configuration.getTableNamePrefix()
        .map { prefix -> "$prefix$suffixName" }
        .orElse(suffixName)

    // toString is not overridden on purpose, so that it isn't mistakenly used as the table name
}

abstract class TableWithCapabilities(
    suffixName: String
) : Table(suffixName) {
    abstract fun queryCapabilities(): TableQueryCapabilities
}

// A DynamoDB attribute
interface DynamoDBAttribute<T> {
    val name: String
    val type: AttributeType
    fun toNamePair(): Pair<String, String>
    fun toNameValuePair(value: T): Pair<String, AttributeValue>
    fun toExpressionNameValuePair(value: T): Pair<String, AttributeValue>
    fun optionalFrom(map: Map<String, AttributeValue>): T?
    fun from(map: Map<String, AttributeValue>): T
    fun attributeValueFrom(map: Map<String, AttributeValue>): AttributeValue
    val hashName: String
    val colonName: String
    fun toAttrValue(value: T): AttributeValue
    fun from(attrValue: AttributeValue): T
    fun addTo(map: MutableMap<String, AttributeValue>, value: T)
    fun addToNullable(map: MutableMap<String, AttributeValue>, value: T?)
    fun addToAny(map: MutableMap<String, AttributeValue>, value: Any)
    fun cast(value: Any): T?
    fun isValueCompatible(value: Any): Boolean
    fun toNameAttributePair(): Pair<String, DynamoDBAttribute<T>>
    fun toAttrValueWithCast(value: Any): AttributeValue
    fun comparator(): Comparator<Map<String, AttributeValue>>?
    fun canBeUsedOnQueryTo(other: DynamoDBAttribute<*>): Boolean
}

// A DynamoDB attribute that must also be unique
// See [https://aws.amazon.com/blogs/database/simulating-amazon-dynamodb-unique-constraints-using-transactions/]
interface UniqueAttribute<T> : DynamoDBAttribute<T> {
    // The value to use on the partition key
    fun uniquenessValueFrom(value: T): String
}

abstract class BaseAttribute<T>(
    final override val name: String,
    override val type: AttributeType
) : DynamoDBAttribute<T> {
    override fun toNamePair() = "#${name}" to name
    override fun toString() = name
    override fun toNameValuePair(value: T) = name to toAttrValue(value)
    override fun toExpressionNameValuePair(value: T) = ":${name}" to toAttrValue(value)
    override fun optionalFrom(map: Map<String, AttributeValue>): T? = map[name]?.let { from(it) }
    override fun from(map: Map<String, AttributeValue>) = optionalFrom(map) ?: throw SchemaErrorException(this)
    override fun attributeValueFrom(map: Map<String, AttributeValue>) = map[name] ?: throw SchemaErrorException(this)
    override fun addTo(map: MutableMap<String, AttributeValue>, value: T) {
        map[name] = toAttrValue(value)
    }

    override fun addToNullable(map: MutableMap<String, AttributeValue>, value: T?) {
        if (value != null) {
            map[name] = toAttrValue(value)
        }
    }

    override fun addToAny(map: MutableMap<String, AttributeValue>, value: Any) {
        val castedValue = cast(value)
        if (castedValue != null) {
            addTo(map, castedValue)
        } else {
            throw RuntimeException("Unable to convert '$value' to '$name' attribute value")
        }
    }

    override val hashName = "#${name}"
    override val colonName = ":${name}"

    override fun isValueCompatible(value: Any) = cast(value) != null

    override fun toNameAttributePair() = name to this

    override fun toAttrValueWithCast(value: Any) =
        toAttrValue(cast(value) ?: throw RuntimeException("Unable to convert '$value' to '$name' attribute value"))

    override fun canBeUsedOnQueryTo(other: DynamoDBAttribute<*>) = this == other
}

private fun <T : Comparable<T>> compare(a: T?, b: T?) =
    if (a == null && b == null) {
        0
    } else if (a == null) {
        -1
    } else if (b == null) {
        1
    } else {
        a.compareTo(b)
    }

class StringAttribute(name: String) : BaseAttribute<String>(name, AttributeType.S) {
    override fun toAttrValue(value: String): AttributeValue = AttributeValue.builder().s(value).build()
    override fun from(attrValue: AttributeValue): String = attrValue.s()
    override fun cast(value: Any) = value as? String
    override fun comparator() = Comparator<DynamoDBItem> { a, b -> compare(optionalFrom(a), optionalFrom(b)) }
}

class EnumAttribute<T : Enum<T>>(
    name: String,
    private val enumClass: Class<T>,
    private val valuesMap: Map<String, T>
) : BaseAttribute<T>(name, AttributeType.S) {
    override fun toAttrValue(value: T): AttributeValue = AttributeValue.builder().s(value.name).build()
    override fun from(attrValue: AttributeValue): T = valuesMap[attrValue.s()]
        ?: throw SchemaErrorException(this, attrValue.s())

    override fun cast(value: Any) = if (enumClass.isInstance(value)) {
        enumClass.cast(value)
    } else {
        null
    }

    override fun comparator() = Comparator<DynamoDBItem> { a, b -> compare(optionalFrom(a), optionalFrom(b)) }

    companion object {
        inline fun <reified T : Enum<T>> of(name: String) = EnumAttribute(
            name,
            T::class.java,
            enumValues<T>().associateBy { it.name }
        )
    }
}

class ListStringAttribute(name: String) : BaseAttribute<Collection<String>>(name, AttributeType.L) {
    override fun toAttrValue(value: Collection<String>): AttributeValue = AttributeValue.builder()
        .l(value.map { AttributeValue.builder().s(it).build() })
        .build()

    override fun from(attrValue: AttributeValue): Collection<String> = attrValue.l()
        .map { it.s() }

    override fun cast(value: Any) = if (value is Collection<*> && value.all { it is String }) {
        value.map { it as String }
    } else {
        null
    }

    // Lists are not comparable
    override fun comparator() = null
}

// An attribute that is composed by two values
class StringCompositeAttribute2(name: String, private val template: (String, String) -> String) :
    BaseAttribute<Pair<String, String>>(name, AttributeType.S) {
    override fun toAttrValue(value: Pair<String, String>): AttributeValue =
        AttributeValue.builder().s(template(value.first, value.second)).build()

    fun toNameValuePair(first: String, second: String) = name to toAttrValue(Pair(first, second))
    override fun from(attrValue: AttributeValue) = throw UnsupportedOperationException("Cannot read a composite value")
    override fun cast(value: Any) = throw UnsupportedOperationException("Cannot cast a composite value")
    override fun comparator(): Comparator<Map<String, AttributeValue>>? = null
}

open class UniqueStringAttribute(name: String, val prefix: String) : BaseAttribute<String>(name, AttributeType.S),
    UniqueAttribute<String> {
    private fun getUniquePkValue(value: String) = "$prefix$value"
    override fun toAttrValue(value: String): AttributeValue = AttributeValue.builder().s(value).build()
    override fun from(attrValue: AttributeValue): String = attrValue.s()
    override fun uniquenessValueFrom(value: String) = getUniquePkValue(value)
    override fun cast(value: Any) = value as? String
    override fun comparator() = Comparator<DynamoDBItem> { a, b -> compare(optionalFrom(a), optionalFrom(b)) }
}

class KeyStringAttribute(name: String) : BaseAttribute<String>(name, AttributeType.S) {
    override fun toAttrValue(value: String): AttributeValue = AttributeValue.builder().s(value).build()
    override fun from(attrValue: AttributeValue): String = attrValue.s()

    fun <T> uniqueKeyEntryFor(uniqueAttribute: UniqueAttribute<T>, value: T) =
        toNameValuePair(uniqueAttribute.uniquenessValueFrom(value))

    override fun cast(value: Any) = value as? String

    override fun comparator() = Comparator<DynamoDBItem> { a, b -> compare(optionalFrom(a), optionalFrom(b)) }
}

class NumberLongAttribute(name: String) : BaseAttribute<Long>(name, AttributeType.N) {
    override fun toAttrValue(value: Long): AttributeValue = AttributeValue.builder().n(value.toString()).build()
    override fun from(attrValue: AttributeValue): Long = attrValue.n().toLong()
    override fun cast(value: Any) = value as? Long ?: (value as? Int)?.toLong()
    override fun comparator() = Comparator<DynamoDBItem> { a, b -> compare(optionalFrom(a), optionalFrom(b)) }
}

class BooleanAttribute(name: String) : BaseAttribute<Boolean>(name, AttributeType.N) {
    override fun toAttrValue(value: Boolean): AttributeValue = AttributeValue.builder().bool(value).build()
    override fun from(attrValue: AttributeValue): Boolean = attrValue.bool()
    override fun cast(value: Any) = value as? Boolean
    override fun comparator() = Comparator<DynamoDBItem> { a, b -> compare(optionalFrom(a), optionalFrom(b)) }
}

/**
 * This attribute type is used when building index objects that are based on duplicate item per entity (and not on
 * DynamoDB local or global secondary indexes).
 * This typically happens when a table has multiple items per entity, because of multiple uniqueness restrictions
 * or strong consistency read requirements.
 */
class UniquenessBasedIndexStringAttribute(
    // The attribute representing the primary key
    private val _primaryKeyAttribute: KeyStringAttribute,
    // The attribute that is added as primary key on a secondary item
    private val _uniqueAttribute: UniqueAttribute<String>
)
// the attribute name is the name of the primary key attribute
    : BaseAttribute<String>(_primaryKeyAttribute.name, AttributeType.S) {

    // the attribute name comes from the primary key, however the value uses the unique attribute mapping function
    override fun toAttrValue(value: String) = _primaryKeyAttribute.toAttrValue(
        _uniqueAttribute.uniquenessValueFrom(value)
    )

    override fun from(attrValue: AttributeValue) = _primaryKeyAttribute.from(attrValue)

    override fun cast(value: Any) = _uniqueAttribute.cast(value)

    override fun comparator() = _uniqueAttribute.comparator()

    override fun canBeUsedOnQueryTo(other: DynamoDBAttribute<*>) = _uniqueAttribute == other
}

class StartsWithStringAttribute(
    // The attribute's name storing the first letters of the full attribute.
    _initialsAttribute: String,
    // The attribute storing the full value which starts with _initialAttribute value.
    val fullAttribute: BaseAttribute<String>,
    // The length of the initials stored in the _initialsAttribute.
    private val _initialLength: Int
) : BaseAttribute<String>(_initialsAttribute, AttributeType.S) {
    private fun getInitials(value: String): String =
        if (value.length < _initialLength) value else value.substring(0, _initialLength)

    override fun from(attrValue: AttributeValue): String = attrValue.s()

    override fun cast(value: Any): String? = value as? String

    override fun comparator() = Comparator<DynamoDBItem> { a, b -> compare(optionalFrom(a), optionalFrom(b)) }

    override fun toAttrValue(value: String): AttributeValue = AttributeValue.builder().s(getInitials(value)).build()

    override fun canBeUsedOnQueryTo(other: DynamoDBAttribute<*>) = this == other || fullAttribute == other
}

class ExpressionBuilder(
    val expression: String, vararg attributes: DynamoDBAttribute<*>
) {
    val attributeNames = attributes.associate { it.toNamePair() }
}

abstract class Expression(
    builder: ExpressionBuilder
) {
    val expression = builder.expression
    val attributeNames = builder.attributeNames
    abstract val values: Map<String, AttributeValue>
}

// Extension functions that apply expressions to request builders
fun UpdateItemRequest.Builder.updateExpression(expression: Expression)
        : UpdateItemRequest.Builder {
    this.updateExpression(expression.expression)
    this.expressionAttributeNames(expression.attributeNames)
    this.expressionAttributeValues(expression.values)
    return this
}

fun Delete.Builder.conditionExpression(expression: Expression)
        : Delete.Builder {
    this.conditionExpression(expression.expression)
    this.expressionAttributeNames(expression.attributeNames)
    this.expressionAttributeValues(expression.values)
    return this
}

fun Put.Builder.conditionExpression(expression: Expression)
        : Put.Builder {
    this.conditionExpression(expression.expression)
    this.expressionAttributeNames(expression.attributeNames)
    this.expressionAttributeValues(expression.values)
    return this
}

sealed class PrimaryKey<T>(val partitionAttribute: DynamoDBAttribute<T>)

class PartitionKey<T>(
    partitionAttribute: DynamoDBAttribute<T>
): PrimaryKey<T>(partitionAttribute)

class CompositePrimaryKey<T1, T2>(
    partitionAttribute: DynamoDBAttribute<T1>,
    val sortAttribute: DynamoDBAttribute<T2>
): PrimaryKey<T1>(partitionAttribute)

// A DynamoDB index composed by a single column (partition key)
class PartitionOnlyIndex<T>(
    val name: String,
    val attribute: DynamoDBAttribute<T>
) {
    override fun toString() = name
    val expression = "${attribute.hashName} = ${attribute.colonName}"
    fun expressionValueMap(value: T) = mapOf(attribute.toExpressionNameValuePair(value))
    val expressionNameMap = mapOf(attribute.hashName to attribute.name)
}

fun <T> QueryRequest.Builder.useIndexAndKey(index: PartitionOnlyIndex<T>, value: T): QueryRequest.Builder {
    this.indexName(index.name)
    this.keyConditionExpression(index.expression)
    this.expressionAttributeNames(index.expressionNameMap)
    this.expressionAttributeValues(index.expressionValueMap(value))
    return this
}

// A DynamoDB index composed by two columns (partition key + sort key)
class PartitionAndSortIndex<T1, T2>(
    val name: String,
    val partitionAttribute: DynamoDBAttribute<T1>,
    val sortAttribute: DynamoDBAttribute<T2>
) {
    override fun toString() = name
    fun expressionValueMap(first: T1, second: T2) = mapOf(
        partitionAttribute.toExpressionNameValuePair(first),
        sortAttribute.toExpressionNameValuePair(second)
    )

    val expressionNameMap = mapOf(
        partitionAttribute.hashName to partitionAttribute.name,
        sortAttribute.hashName to sortAttribute.name
    )

    val keyConditionExpression =
        "${partitionAttribute.hashName} = ${partitionAttribute.colonName} AND ${sortAttribute.hashName} = ${sortAttribute.colonName}"
}

fun <T> MutableMap<String, AttributeValue>.addAttr(attribute: DynamoDBAttribute<T>, value: T) {
    this[attribute.name] = attribute.toAttrValue(value)
}