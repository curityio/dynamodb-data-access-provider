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

abstract class Attribute<T>(val name: String, val type: AttributeType)
{
    fun toNamePair() = "#${name}" to name
    override fun toString() = name
    fun toNameValuePair(value: T) = name to toAttrValue(value)
    fun toExpressionNameValuePair(value: T) = ":${name}" to toAttrValue(value)
    fun from(map: Map<String, AttributeValue>): T? = map[name]?.let { from(it) }

    abstract fun toAttrValue(value: T): AttributeValue
    abstract fun from(attrValue: AttributeValue): T
}

class StringAttribute(name: String) : Attribute<String>(name, AttributeType.S)
{
    override fun toAttrValue(value: String): AttributeValue = AttributeValue.builder().s(value).build()
    override fun from(attrValue: AttributeValue): String = attrValue.s()
}

class NumberLongAttribute(name: String) : Attribute<Long>(name, AttributeType.N)
{
    override fun toAttrValue(value: Long): AttributeValue = AttributeValue.builder().n(value.toString()).build()
    override fun from(attrValue: AttributeValue): Long = attrValue.n().toLong()
}

abstract class Expression(vararg val attributes: Attribute<*>)
{
    abstract val expression: String
    val attributeNames = attributes
        .map {
            it.toNamePair()
        }
        .toMap()
}

fun UpdateItemRequest.Builder.updateExpression(expression: Expression, values: Map<String, AttributeValue>)
: UpdateItemRequest.Builder
{
    this.updateExpression(expression.expression)
    this.expressionAttributeNames(expression.attributeNames)
    this.expressionAttributeValues(values)
    return this
}
