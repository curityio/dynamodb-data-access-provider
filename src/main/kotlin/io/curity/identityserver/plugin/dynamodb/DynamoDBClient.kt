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

import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import io.curity.identityserver.plugin.dynamodb.query.UnsupportedQueryException
import org.apache.http.conn.HttpHostConnectException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.alarm.ExternalServiceFailedAuthenticationAlarmException
import se.curity.identityserver.sdk.alarm.ExternalServiceFailedCommunicationAlarmException
import se.curity.identityserver.sdk.alarm.ExternalServiceFailedConnectionAlarmException
import se.curity.identityserver.sdk.errors.ErrorCode
import se.curity.identityserver.sdk.plugin.ManagedObject
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.DynamoDbResponse
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.QueryResponse
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse
import software.amazon.awssdk.services.sts.model.Credentials
import java.net.URI
import java.time.Duration

class DynamoDBClient(private val config: DynamoDBDataAccessProviderConfiguration) :
    ManagedObject<DynamoDBDataAccessProviderConfiguration>(config)
{
    private val _awsRegion = Region.of(config.getAwsRegion().awsRegion)
    private val client: DynamoDbClient = initializeDynamoDBClient()

    private fun initializeDynamoDBClient(): DynamoDbClient
    {
        val accessMethod = config.getDynamodbAccessMethod()
        /*Use Instance Profile from IAM Role applied to EC2 instance*/
        var credentials: AwsCredentialsProvider?

        if (accessMethod.isEC2InstanceProfile.isPresent && accessMethod.isEC2InstanceProfile.get())
        {
            credentials = InstanceProfileCredentialsProvider.builder().build()
        } else if (accessMethod.accessKeyIdAndSecret.isPresent)
        {
            val keyIdAndSecret = accessMethod.accessKeyIdAndSecret.get()
            credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    keyIdAndSecret.accessKeyId.get(),
                    keyIdAndSecret.accessKeySecret.get()
                )
            )

            /* roleARN is present, get temporary credentials through AssumeRole */
            if (keyIdAndSecret.awsRoleARN.isPresent)
            {
                credentials = getNewCredentialsFromAssumeRole(credentials, keyIdAndSecret.awsRoleARN.get())
            }
        } else if (accessMethod.aWSProfile.get().awsProfileName.isPresent)
        {
            val awsProfile = accessMethod.aWSProfile.get()
            credentials = ProfileCredentialsProvider.builder()
                .profileName(awsProfile.awsProfileName.get())
                .build()

            /* roleARN is present, get temporary credentials through AssumeRole */
            if (awsProfile.awsRoleARN.isPresent)
            {
                credentials = getNewCredentialsFromAssumeRole(credentials, awsProfile.awsRoleARN.get())
            }
        } else
        {
            credentials = null
        }

        val builder = DynamoDbClient.builder()
            .credentialsProvider(credentials)

        if (config.getEndpointOverride().isPresent)
        {
            builder.endpointOverride(URI.create(config.getEndpointOverride().get()))
        }
        builder.region(_awsRegion)

        builder.overrideConfiguration { c -> c
            .apiCallAttemptTimeout(config.getApiCallAttemptTimeout().map { Duration.ofSeconds(it) }.orElse(null))
            .apiCallTimeout(config.getApiCallTimeout().map { Duration.ofSeconds(it) }.orElse(null))
        }

        return builder.build()
    }

    private fun getNewCredentialsFromAssumeRole(credentialsProvider: AwsCredentialsProvider, roleARN: String): AwsCredentialsProvider
    {
        val stsClient: StsClient = StsClient.builder()
            .region(_awsRegion)
            .credentialsProvider(credentialsProvider)
            .build()
        val assumeRoleRequest: AssumeRoleRequest = AssumeRoleRequest.builder()
            .durationSeconds(3600)
            .roleArn(roleARN)
            .roleSessionName("curity-dynamodb-data-access")
            .build()

        return try
        {
            val assumeRoleResult: AssumeRoleResponse = stsClient.assumeRole(assumeRoleRequest)
            if (!assumeRoleResult.sdkHttpResponse().isSuccessful)
            {
                logger.warn(
                    "AssumeRole Request sent but was not successful: {}",
                    assumeRoleResult.sdkHttpResponse().statusText().get()
                )
                credentialsProvider //Returning the original credentials
            } else
            {
                val credentials: Credentials = assumeRoleResult.credentials()
                val asc: AwsSessionCredentials = AwsSessionCredentials.create(
                    credentials.accessKeyId(),
                    credentials.secretAccessKey(),
                    credentials.sessionToken()
                )
                logger.debug("AssumeRole Request successful: {}", assumeRoleResult.sdkHttpResponse().statusText())
                StaticCredentialsProvider.create(asc) //returning temp credentials from the assumed role
            }
        } catch (e: Exception)
        {
            logger.debug("AssumeRole Request failed: {}", e.message)
            throw config.getExceptionFactory().internalServerException(ErrorCode.EXTERNAL_SERVICE_ERROR)
        }
    }

    fun getItem(request: GetItemRequest): GetItemResponse = client.call { getItem(request) }

    fun putItem(request: PutItemRequest): PutItemResponse = client.call { putItem(request) }
    fun updateItem(request: UpdateItemRequest): UpdateItemResponse = client.call { updateItem(request) }

    fun deleteItem(request: DeleteItemRequest): DeleteItemResponse = client.call { deleteItem(request) }

    fun query(request: QueryRequest): QueryResponse = client.call { query(request) }
    fun scan(request: ScanRequest): ScanResponse = client.call { scan(request) }
    fun transactionWriteItems(request: TransactWriteItemsRequest): TransactWriteItemsResponse = client.call { transactWriteItems(request) }

    private fun <T : DynamoDbResponse> DynamoDbClient.call(block: DynamoDbClient.() -> T): T {
        try
        {
            return block()
        } catch (e: DynamoDbException)
        {
            when
            {
                e.awsErrorDetails()?.errorCode() == "ConditionalCheckFailedException" -> throw e
                e.awsErrorDetails()
                    ?.errorCode() == "UnrecognizedClientException" -> throw ExternalServiceFailedAuthenticationAlarmException(
                    e
                )
                e.statusCode() >= 500 -> throw ExternalServiceFailedConnectionAlarmException(e)
                else -> throw ExternalServiceFailedCommunicationAlarmException(e)
            }
        } catch (e: SdkClientException)
        {
            if (e.cause is HttpHostConnectException)
            {
                throw ExternalServiceFailedConnectionAlarmException(e)
            }
            // Not really sure what the cause is, so let's classify it as a communication issue
            throw ExternalServiceFailedCommunicationAlarmException(e)
        } catch (e: SdkException)
        {
            throw ExternalServiceFailedCommunicationAlarmException(e)
        }
    }

    override fun close()
    {
        client.close()
    }

    companion object
    {
        val logger: Logger = LoggerFactory.getLogger(DynamoDBClient::class.java)
    }
}
