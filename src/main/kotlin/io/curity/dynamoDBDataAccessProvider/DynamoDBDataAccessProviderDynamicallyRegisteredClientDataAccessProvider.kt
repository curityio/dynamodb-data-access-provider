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
package io.curity.dynamoDBDataAccessProvider

import io.curity.dynamoDBDataAccessProvider.configuration.DynamoDBDataAccessProviderDataAccessProviderConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.attribute.scim.v2.extensions.DynamicallyRegisteredClientAttributes
import se.curity.identityserver.sdk.datasource.DynamicallyRegisteredClientDataAccessProvider

class DynamoDBDataAccessProviderDynamicallyRegisteredClientDataAccessProvider(private val _configuration: DynamoDBDataAccessProviderDataAccessProviderConfig): DynamicallyRegisteredClientDataAccessProvider
{
    override fun getByClientId(clientId: String): DynamicallyRegisteredClientAttributes
    {
        _logger.debug("Getting dynamic client with id: {}", clientId)
        TODO()
    }

    override fun create(dynamicallyRegisteredClientAttributes: DynamicallyRegisteredClientAttributes)
    {
        _logger.debug("Received request to CREATE dynamic client with id : {}", dynamicallyRegisteredClientAttributes.clientId)
        TODO()
    }

    override fun update(dynamicallyRegisteredClientAttributes: DynamicallyRegisteredClientAttributes)
    {
        _logger.debug("Received request to UPDATE dynamic client for client : {}", dynamicallyRegisteredClientAttributes.clientId)
        TODO()
    }

    override fun delete(clientId: String)
    {
        _logger.debug("Received request to DELETE dynamic client : {}", clientId)
        TODO()
    }

    companion object
    {
        private val _logger: Logger = LoggerFactory.getLogger(DynamoDBDataAccessProviderCredentialDataAccessProvider::class.java)
    }
}
