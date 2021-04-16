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
import software.amazon.awssdk.services.dynamodb.model.Update
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

enum class AttributeType(val typeName: String)
{
    S("S"),
    N("N")
}

abstract class Table(val name: String)
{
    override fun toString() = name
}

interface Attribute<T>
{
    val name: String
    val type: AttributeType
    fun toNamePair(): Pair<String, String>
    fun toNameValuePair(value: T): Pair<String, AttributeValue>
    fun toExpressionNameValuePair(value: T): Pair<String, AttributeValue>
    fun from(map: Map<String, AttributeValue>): T?
    val hashName: String
    val colonName: String
    fun toAttrValue(value: T): AttributeValue
    fun from(attrValue: AttributeValue): T
}

interface UniqueAttribute<T> : Attribute<T>
{
    fun uniquenessValueFrom(value: T): String
}

abstract class BaseAttribute<T>(
    override val name: String,
    override val type: AttributeType
) : Attribute<T>
{
    override fun toNamePair() = "#${name}" to name
    override fun toString() = name
    override fun toNameValuePair(value: T) = name to toAttrValue(value)
    override fun toExpressionNameValuePair(value: T) = ":${name}" to toAttrValue(value)
    override fun from(map: Map<String, AttributeValue>): T? = map[name]?.let { from(it) }

    override val hashName = "#${name}"
    override val colonName = ":${name}"
}

class StringAttribute(name: String) : BaseAttribute<String>(name, AttributeType.S)
{
    override fun toAttrValue(value: String): AttributeValue = AttributeValue.builder().s(value).build()
    override fun from(attrValue: AttributeValue): String = attrValue.s()
}

class UniqueStringAttribute(name: String, val _f: (String) -> String)
    : BaseAttribute<String>(name, AttributeType.S), UniqueAttribute<String>
{
    override fun toAttrValue(value: String): AttributeValue = AttributeValue.builder().s(value).build()
    override fun from(attrValue: AttributeValue): String = attrValue.s()
    override fun uniquenessValueFrom(value: String) = _f(value)
}

class KeyStringAttribute(name: String) : BaseAttribute<String>(name, AttributeType.S)
{
    override fun toAttrValue(value: String): AttributeValue = AttributeValue.builder().s(value).build()
    override fun from(attrValue: AttributeValue): String = attrValue.s()

    fun <T> uniqueKeyEntryFor(uniqueAttribute: UniqueAttribute<T>, value: T) =
        toNameValuePair(uniqueAttribute.uniquenessValueFrom(value))
}

class NumberLongAttribute(name: String) : BaseAttribute<Long>(name, AttributeType.N)
{
    override fun toAttrValue(value: Long): AttributeValue = AttributeValue.builder().n(value.toString()).build()
    override fun from(attrValue: AttributeValue): Long = attrValue.n().toLong()
}

class BooleanAttribute(name: String) : BaseAttribute<Boolean>(name, AttributeType.N)
{
    override fun toAttrValue(value: Boolean): AttributeValue = AttributeValue.builder().bool(value).build()
    override fun from(attrValue: AttributeValue): Boolean = attrValue.bool()
}

class ExpressionBuilder(
    expr: String, vararg attributes: Attribute<*>)
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

abstract class Index<T>(val name: String, val attribute: Attribute<T>)
{
    override fun toString() = name
    val expressionNameMap = mapOf(attribute.hashName to attribute.name)
}
