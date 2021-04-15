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
package io.curity.identityserver.plugin.dynamodb.token

import io.curity.identityserver.plugin.dynamodb.toAttributeValue
import se.curity.identityserver.sdk.attribute.AccountAttributes
import se.curity.identityserver.sdk.data.authorization.Delegation
import se.curity.identityserver.sdk.data.query.Filter
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.util.EnumMap

class DelegationsFilterParser(filter: Filter): FilterParser(filter, filterDelegationAttributeNamesToDynamoAttributesMap) {
    companion object {
        private val filterDelegationAttributeNamesToDynamoAttributesMap = mapOf(
                Pair(AccountAttributes.USER_NAME, "owner"),
                Pair(Delegation.KEY_OWNER, "owner"),
                Pair(Delegation.KEY_SCOPE, "scope"),
                Pair(Delegation.KEY_CLIENT_ID, "clientId"),
                Pair("client_id", "clientId"),
                Pair(Delegation.KEY_REDIRECT_URI, "redirectUri"),
                Pair("redirect_uri", "redirectUri"),
                Pair(Delegation.KEY_STATUS, "status"),
                Pair("expires", "expires"),
                Pair("externalId", "id")
        )
    }
}

class UserAccountFilterParser(filter: Filter): FilterParser(filter, filterUserAccountAttributeNamesToDynamoAttributesMap) {
    companion object {
        private val filterUserAccountAttributeNamesToDynamoAttributesMap = mapOf(
                Pair("phoneNumbers", "phone"),
                Pair("emails", "email")
        )
    }
}

sealed class FilterParser(filter: Filter, val filterAttributeNamesToDynamoAttributesMap: Map<String, String>)
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
            attributeValues[":$name"] = if (filter.attributeName == "active") (filter.value as Boolean).toAttributeValue() else (filter.value as String).toAttributeValue()
            return getFilterExpressionPartWithOperator(filter.operator, filter.attributeName, name)
        }

        return ""
    }

    private fun getFilterExpressionPartWithOperator(operator: Filter.AttributeOperator, attributeKey: String, attributeValueName: String): String {
        val mappedAttributeKey = filterAttributeNamesToDynamoAttributesMap[attributeKey] ?: attributeKey
        var sanitizedAttributeKey = mappedAttributeKey

        if (reservedAttributeKeyNames.contains(mappedAttributeKey)) {
            sanitizedAttributeKey = "#$mappedAttributeKey"
            attributesNamesMap[sanitizedAttributeKey] = mappedAttributeKey
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
            // TODO: there is no "ends with" operator in DynamoDB, this should be further filtered in code
            map[Filter.AttributeOperator.EW]= " contains(_attribute_, :_attributeValue_)"
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
