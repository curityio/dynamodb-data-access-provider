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

package io.curity.identityserver.plugin.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.Delete
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

enum class AttributeType(val typeName: String)
{
    S("S"),
    N("N"),
    L("L")
}

// A DynamoDB table
abstract class Table(val name: String)
{
    override fun toString() = name
}

// A DynamoDB attribute
interface DynamoDBAttribute<T>
{
    val name: String
    val type: AttributeType
    fun toNamePair(): Pair<String, String>
    fun toNameValuePair(value: T): Pair<String, AttributeValue>
    fun toExpressionNameValuePair(value: T): Pair<String, AttributeValue>
    fun fromOpt(map: Map<String, AttributeValue>): T?
    fun from(map: Map<String, AttributeValue>): T
    val hashName: String
    val colonName: String
    fun toAttrValue(value: T): AttributeValue
    fun from(attrValue: AttributeValue): T
    fun addTo(map: MutableMap<String, AttributeValue>, value: T)
    fun addToOpt(map: MutableMap<String, AttributeValue>, value: T?)
    fun addToAny(map: MutableMap<String, AttributeValue>, value: Any)
    fun cast(value: Any): T?
}

// A DynamoDB attribute that must also be unique
// See [https://aws.amazon.com/blogs/database/simulating-amazon-dynamodb-unique-constraints-using-transactions/]
interface UniqueAttribute<T> : DynamoDBAttribute<T>
{
    // The value to use on the partition key
    fun uniquenessValueFrom(value: T): String
}

abstract class BaseAttribute<T>(
    final override val name: String,
    override val type: AttributeType
) : DynamoDBAttribute<T>
{
    override fun toNamePair() = "#${name}" to name
    override fun toString() = name
    override fun toNameValuePair(value: T) = name to toAttrValue(value)
    override fun toExpressionNameValuePair(value: T) = ":${name}" to toAttrValue(value)
    override fun fromOpt(map: Map<String, AttributeValue>): T? = map[name]?.let { from(it) }
    override fun from(map: Map<String, AttributeValue>) = fromOpt(map) ?: throw SchemaErrorException(this)
    override fun addTo(map: MutableMap<String, AttributeValue>, value: T)
    {
        map[name] = toAttrValue(value)
    }

    override fun addToOpt(map: MutableMap<String, AttributeValue>, value: T?)
    {
        if (value != null)
        {
            map[name] = toAttrValue(value)
        }
    }

    override fun addToAny(map: MutableMap<String, AttributeValue>, value: Any) {
        val castedValue = cast(value)
        if(castedValue != null) {
            addTo(map, castedValue)
        } else {
            throw Exception("TODO")
        }
    }

    override val hashName = "#${name}"
    override val colonName = ":${name}"
}

class StringAttribute(name: String) : BaseAttribute<String>(name, AttributeType.S)
{
    override fun toAttrValue(value: String): AttributeValue = AttributeValue.builder().s(value).build()
    override fun from(attrValue: AttributeValue): String = attrValue.s()
    override fun cast(value: Any) = value as? String
}

class ListStringAttribute(name: String) : BaseAttribute<Collection<String>>(name, AttributeType.L)
{
    override fun toAttrValue(value: Collection<String>): AttributeValue = AttributeValue.builder()
        .l(value.map { AttributeValue.builder().s(it).build() })
        .build()

    override fun from(attrValue: AttributeValue): Collection<String> = attrValue.l()
        .map { it.s() }

    override fun cast(value: Any) = if (value is Collection<*> && value.all { it is String })
    {
        value.map { it as String }
    } else
    {
        null
    }
}

// An attribute that is composed by two values
class StringCompositeAttribute2(name: String, private val template: (String, String) -> String) :
    BaseAttribute<String>(name, AttributeType.S)
{
    override fun toAttrValue(value: String): AttributeValue =
        throw UnsupportedOperationException("Cannot create from a single value")

    fun toAttrValue2(first: String, second: String): AttributeValue =
        AttributeValue.builder().s(template(first, second)).build()

    fun toNameValuePair(first: String, second: String) = name to toAttrValue2(first, second)
    override fun from(attrValue: AttributeValue): String = attrValue.s()

    override fun cast(value: Any) = value as? String
}

class UniqueStringAttribute(name: String, val _f: (String) -> String) : BaseAttribute<String>(name, AttributeType.S),
    UniqueAttribute<String>
{
    override fun toAttrValue(value: String): AttributeValue = AttributeValue.builder().s(value).build()
    override fun from(attrValue: AttributeValue): String = attrValue.s()
    override fun uniquenessValueFrom(value: String) = _f(value)
    override fun cast(value: Any) = value as? String
}

class KeyStringAttribute(name: String) : BaseAttribute<String>(name, AttributeType.S)
{
    override fun toAttrValue(value: String): AttributeValue = AttributeValue.builder().s(value).build()
    override fun from(attrValue: AttributeValue): String = attrValue.s()

    fun <T> uniqueKeyEntryFor(uniqueAttribute: UniqueAttribute<T>, value: T) =
        toNameValuePair(uniqueAttribute.uniquenessValueFrom(value))

    override fun cast(value: Any) = value as? String
}

class NumberLongAttribute(name: String) : BaseAttribute<Long>(name, AttributeType.N)
{
    override fun toAttrValue(value: Long): AttributeValue = AttributeValue.builder().n(value.toString()).build()
    override fun from(attrValue: AttributeValue): Long = attrValue.n().toLong()
    override fun cast(value: Any) = value as? Long
}

class BooleanAttribute(name: String) : BaseAttribute<Boolean>(name, AttributeType.N)
{
    override fun toAttrValue(value: Boolean): AttributeValue = AttributeValue.builder().bool(value).build()
    override fun from(attrValue: AttributeValue): Boolean = attrValue.bool()
    override fun cast(value: Any) = value as? Boolean
}

class ExpressionBuilder(
    expr: String, vararg attributes: DynamoDBAttribute<*>
)
{
    val expression: String = expr
    val attributeNames = attributes
        .map {
            it.toNamePair()
        }
        .toMap()
}

abstract class Expression(
    builder: ExpressionBuilder
)
{
    val expression = builder.expression
    val attributeNames = builder.attributeNames
    abstract val values: Map<String, AttributeValue>
}

// Extension functions that apply expressions to request builders
fun UpdateItemRequest.Builder.updateExpression(expression: Expression)
        : UpdateItemRequest.Builder
{
    this.updateExpression(expression.expression)
    this.expressionAttributeNames(expression.attributeNames)
    this.expressionAttributeValues(expression.values)
    return this
}

fun Delete.Builder.conditionExpression(expression: Expression)
        : Delete.Builder
{
    this.conditionExpression(expression.expression)
    this.expressionAttributeNames(expression.attributeNames)
    this.expressionAttributeValues(expression.values)
    return this
}

// A DynamoDB index composed by a single column (partition key)
class Index<T>(val name: String, val attribute: DynamoDBAttribute<T>)
{
    override fun toString() = name
    val expression = "${attribute.hashName} = ${attribute.colonName}"
    fun expressionValueMap(value: T) = mapOf(attribute.toExpressionNameValuePair(value))
    val expressionNameMap = mapOf(attribute.hashName to attribute.name)
}

// A DynamoDB index composed by two columns (partition key + sort key)
class Index2<T>(val name: String, private val attribute1: DynamoDBAttribute<T>, private val attribute2: DynamoDBAttribute<T>)
{
    override fun toString() = name
    fun expressionValueMap(first: T, second: T) = mapOf(
        attribute1.toExpressionNameValuePair(first),
        attribute2.toExpressionNameValuePair(second)
    )

    val expressionNameMap = mapOf(
        attribute1.hashName to attribute1.name,
        attribute2.hashName to attribute2.name
    )

    val keyConditionExpression =
        "${attribute1.hashName} = ${attribute1.colonName} AND ${attribute2.hashName} = ${attribute2.colonName}"
}

fun <T> MutableMap<String, AttributeValue>.addAttr(attribute: DynamoDBAttribute<T>, value: T)
{
    this[attribute.name] = attribute.toAttrValue(value)
}