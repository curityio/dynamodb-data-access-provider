/*
 *  Copyright 2020 Curity AB
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
package io.curity.dynamoDBDataAccessProvider.token

import se.curity.identityserver.sdk.data.query.Filter
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.util.EnumMap

class FilterParser(filter: Filter)
{
    val attributesNamesMap = mutableMapOf<String, String>()
    val attributeValues = mutableMapOf<String, AttributeValue>()
    val parsedFilter = parseFilter(filter)

    private fun parseFilter(filter: Filter): String  {
        if (filter is Filter.LogicalExpression){
            val left = parseFilter(filter.leftHandFilter)
            val right = parseFilter(filter.rightHandFilter)
            return "($left) ${filter.operator.name} ($right)"
        }

        if (filter is Filter.NotExpression){
            val parsedFilter = parseFilter(filter.filter)
            return "NOT ($parsedFilter)"
        }

        if (filter is Filter.AttributeExpression) {
            val name = getAttributeName(filter.attributeName)
            attributeValues[":$name"] = AttributeValue.builder().s(filter.value.toString()).build()
            return getFilterExpressionPartWithOperator(filter.operator, filter.attributeName, name)
        }

        return ""
    }

    private fun getFilterExpressionPartWithOperator(operator: Filter.AttributeOperator, attributeKey: String, attributeValueName: String): String {
        if (operator == Filter.AttributeOperator.EW) {
            // DynamoDB does not have an ends-with operator
            return ""
        }

        var sanitizedAttributeKey = attributeKey

        if (reservedAttributeKeyNames.contains(attributeKey)) {
            sanitizedAttributeKey = "#$attributeKey"
            attributesNamesMap[sanitizedAttributeKey] = attributeKey
        }

        return operatorToDynamoDB[operator]?.replace("_attribute_", sanitizedAttributeKey)?.replace("_attributeValue_", attributeValueName) ?: ""
    }

    private fun getAttributeName(attributeName: String): String {
        var name = attributeName
        var index = 0
        while (attributeValues[name] != null) {
            name = "$attributeName-$index"
            index++
        }

        return name
    }

    companion object {
        private val operatorToDynamoDB = initializeOperatorMap()

        private fun initializeOperatorMap(): EnumMap<Filter.AttributeOperator, String>
        {
            val map = EnumMap<Filter.AttributeOperator, String>(Filter.AttributeOperator::class.java)
            map[Filter.AttributeOperator.EQ] = "_attribute_ = :_attributeValue_"
            map[Filter.AttributeOperator.NE]= "_attribute_ <> :_attributeValue_"
            map[Filter.AttributeOperator.CO]= " contains(_attribute_, :_attributeValue_)"
            map[Filter.AttributeOperator.SW]= " begins_with(_attribute_, :_attributeValue_)"
            map[Filter.AttributeOperator.EW]= ""
            map[Filter.AttributeOperator.PR]= "attribute_exists(_attribute_) AND size(_attribute_) > 0"
            map[Filter.AttributeOperator.GT]= "_attribute_ > :_attributeValue_"
            map[Filter.AttributeOperator.GE]= "_attribute_ >= :_attributeValue_"
            map[Filter.AttributeOperator.LT]= "_attribute_ < :_attributeValue_"
            map[Filter.AttributeOperator.LE]= "_attribute_ <= :_attributeValue_"

            return map
        }

        private val reservedAttributeKeyNames = listOf("status", "scope", "owner")
    }
}
