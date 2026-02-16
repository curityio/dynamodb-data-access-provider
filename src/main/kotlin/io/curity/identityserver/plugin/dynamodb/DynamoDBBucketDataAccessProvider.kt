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
package io.curity.identityserver.plugin.dynamodb

import io.curity.identityserver.plugin.dynamodb.TenantAwareUniqueStringAttribute.KeyPrefixTemplate
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import org.slf4j.MarkerFactory
import se.curity.identityserver.sdk.data.GetBucketResult
import se.curity.identityserver.sdk.datasource.BucketDataAccessProvider
import se.curity.identityserver.sdk.service.authentication.TenantId
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant

/**
 * To allow any subject, purpose pair to exist in different tenants, the partition key (subject) includes the tenant ID.
 * - for default tenant: `{subject}`, e.g. `johndoe`
 * - for custom tenant: `s({tenantId})#subject`, e.g. 's(tenant1)#johndoe
 *
 * Note that when a bucket is owned by a custom tenant, a `tenantId` attribute is persisted with the bucket item.
 * This may help filtering all items for a given tenant. Buckets owned by default tenant do not have
 * a tenantId attribute.
 */
class DynamoDBBucketDataAccessProvider(
    private val _configuration: DynamoDBDataAccessProviderConfiguration,
    private val _dynamoDBClient: DynamoDBClient
) : BucketDataAccessProvider {
    // Lazy initialization is required to avoid cyclic dependencies while Femto containers are built.
    // TenantId should not be resolved from the configuration at DAP initialization time.
    private val _tenantId: TenantId by lazy {
        _configuration.getTenantId()
    }

    override fun getAttributes(subject: String, purpose: String): Map<String, Any> {
        return when (val result = getBucket(subject, purpose)) {
            is GetBucketResult.Success -> result.attributes
            else -> mapOf()
        }
    }

    override fun getBucket(
        subject: String, purpose: String
    ): GetBucketResult {
        _logger.debug(MASK_MARKER, "getAttributes with tenant: {}, subject: {} , purpose : {}",
            _tenantId.tenantId, subject, purpose)

        val request = GetItemRequest.builder()
            .tableName(BucketsTable.name(_configuration))
            .key(BucketsTable.key(_tenantId, subject, purpose))
            .consistentRead(true)
            .build()
        val response = _dynamoDBClient.getItem(request)

        if (!response.hasItem()) {
            return GetBucketResult.Error.NotFound.INSTANCE
        }
        val expires: Instant? = BucketsTable.expires.optionalFrom(response.item())?.let {
            Instant.ofEpochSecond(it)
        }
        if (expires != null && Instant.now().isAfter(expires)) {
            _logger.debug(
                MASK_MARKER,
                "A bucket entry was expired, so it will not be returned. subject: {}, purpose: {}, expires: {}",
                subject, purpose, expires
            )
            return GetBucketResult.Error.Expired(expires)
        }
        val attributesString = BucketsTable.attributes.optionalFrom(response.item())
            ?: throw SchemaErrorException(
                BucketsTable,
                BucketsTable.attributes
            )
        return GetBucketResult.Success(
            _configuration.getJsonHandler().fromJson(attributesString),
            expires
        )
    }

    override fun storeAttributes(subject: String, purpose: String, dataMap: Map<String, Any>): Map<String, Any> {
        return storeAttributes(subject, purpose, dataMap, null)
    }

    override fun storeAttributes(
        subject: String,
        purpose: String,
        dataMap: Map<String, Any>,
        expires: Instant?
    ): Map<String, Any> {
        _logger.debug(MASK_MARKER,
            "storeAttributes with tenant: {}, subject: {} , purpose : {}, data : {} and expires : {}",
            _tenantId.tenantId, subject, purpose, dataMap, expires
        )

        val attributesString = _configuration.getJsonHandler().toJson(dataMap)
        val now = Instant.now().epochSecond

        val request = UpdateItemRequest.builder()
            .tableName(BucketsTable.name(_configuration))
            .key(BucketsTable.key(_tenantId, subject, purpose))
            .apply {
                updateExpression(
                    attributesString, now, now, expires?.epochSecond
                ).applyTo(this)
            }.build()

        _dynamoDBClient.updateItem(request)

        return dataMap
    }

    override fun clearBucket(subject: String, purpose: String): Boolean {
        val request = DeleteItemRequest.builder()
            .tableName(BucketsTable.name(_configuration))
            .key(BucketsTable.key(_tenantId, subject, purpose))
            .returnValues(ReturnValue.ALL_OLD)
            .build()

        val response = _dynamoDBClient.deleteItem(request)

        return response.hasAttributes()
    }

    private object BucketsTable : Table("curity-bucket") {
        val tenantId = StringAttribute("tenantId")
        val subject = TenantAwareUniqueStringAttribute("subject", object: KeyPrefixTemplate {
            override val prefix = "s"
            override fun toPrefix(tenantId: TenantId): String =
                if (tenantId.tenantId == null) {
                    // For default tenant, no prefix is applied to subject.
                    ""
                } else {
                    // e.g. "s(tenant1)#"
                    "$prefix(${tenantId.tenantId})#"
                }
        })
        val purpose = StringAttribute("purpose")
        val attributes = StringAttribute("attributes")
        val created = NumberLongAttribute("created")
        val updated = NumberLongAttribute("updated")
        val expires = NumberLongAttribute("expires")

        fun key(tenantId: TenantId, subject: String, purpose: String) = mapOf(
            this.subject.toNameValuePair(this.subject.uniquenessValueFrom(tenantId, subject)),
            this.purpose.toNameValuePair(purpose)
        )
    }

    private fun updateExpression(
        attributesString: String,
        created: Long,
        updated: Long,
        expires: Long?,
    ): UpdateExpressionsBuilder {
        val builder = UpdateExpressionsBuilder()
        builder.update(BucketsTable.attributes, attributesString)
        builder.update(BucketsTable.updated, updated)
        builder.update(BucketsTable.expires, expires)
        builder.updateIfNotExists(BucketsTable.created, created)

        if(_tenantId.tenantId != null) {
            builder.updateIfNotExists(BucketsTable.tenantId, _tenantId.tenantId)
        }

        return builder
    }

    companion object {
        private val _logger: Logger = LoggerFactory.getLogger(DynamoDBBucketDataAccessProvider::class.java)
        private val MASK_MARKER : Marker = MarkerFactory.getMarker("MASK")
    }
}
