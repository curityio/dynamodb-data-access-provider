package io.curity.identityserver.plugin.dynamodb.query

import io.curity.identityserver.plugin.dynamodb.DynamoDBAttribute
import io.curity.identityserver.plugin.dynamodb.DynamoDBQuery
import io.curity.identityserver.plugin.dynamodb.DynamoDBScan
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class DynamoDBQueryBuilder
{
    companion object
    {
        fun buildQuery(keyCondition: KeyCondition, products: List<Product>): DynamoDBQuery
        {
            val builder = DynamoDBQueryBuilder()
            val filterExpression = builder.toDynamoExpression(products)
            val keyExpression = builder.toDynamoExpression(keyCondition)
            return DynamoDBQuery(
                keyCondition.index.name,
                keyExpression,
                filterExpression,
                builder.valueMap,
                builder.nameMap
            )
        }

        fun buildScan(products: List<Product>): DynamoDBScan
        {
            val builder = DynamoDBQueryBuilder()
            val filterExpression = builder.toDynamoExpression(products)
            return DynamoDBScan(
                filterExpression,
                builder.valueMap,
                builder.nameMap
            )
        }
    }

    private val valueMap = mutableMapOf<String, AttributeValue>()
    private val nameMap = mutableMapOf<String, String>()
    private val valueAliasCounter = mutableMapOf<String, Int>()

    private fun toDynamoExpression(keyCondition: KeyCondition): String
    {
        val partitionExpression = toDynamoExpression(keyCondition.partitionCondition)
        return if (keyCondition.sortCondition == null)
        {
            partitionExpression
        } else
        {
            "$partitionExpression AND (${toDynamoExpression(keyCondition.sortCondition)})"
        }
    }

    private fun toDynamoExpression(rangeExpression: RangeExpression) = when (rangeExpression)
    {
        is RangeExpression.Binary -> toDynamoExpression(rangeExpression.attributeExpression)
        is RangeExpression.Between ->
        {
            val hashName = hashNameFor(rangeExpression.attribute)
            val colonNameLower = colonNameFor(rangeExpression.attribute, rangeExpression.lower)
            val colonNameHigher = colonNameFor(rangeExpression.attribute, rangeExpression.higher)

            "$hashName BETWEEN $colonNameLower AND $colonNameHigher"
        }
    }

    private fun toDynamoExpression(products: List<Product>) =
        products.joinToString(" OR ") { "(${toDynamoExpression(it)})" }

    private fun toDynamoExpression(product: Product): String =
        product.terms.joinToString(" AND ") { "(${toDynamoExpression(it)})" }

    private fun toDynamoExpression(it: Expression.Attribute): String
    {
        val hashName = hashNameFor(it.attribute)
        val colonName = colonNameFor(it.attribute, it.value)
        return "(${it.operator.toDynamoOperator(hashName, colonName)})"
    }

    private fun hashNameFor(attribute: DynamoDBAttribute<*>): String
    {
        nameMap[attribute.name] = attribute.hashName
        return attribute.hashName
    }

    private fun colonNameFor(attribute: DynamoDBAttribute<*>, value: Any): String
    {
        val counter = valueAliasCounter.merge(attribute.name, 1) { old, new -> old + new } ?: 1
        val colonName = colonName(attribute.name, counter)
        // FIXME
        valueMap[colonName] = AttributeValue.builder().s(value.toString()).build()
        return colonName
    }

    private fun hashName(name: String) = "#$name"
    private fun colonName(name: String, counter: Int) = ":${name}_$counter"
}