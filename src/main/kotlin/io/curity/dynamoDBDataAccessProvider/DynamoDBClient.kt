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
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest
import software.amazon.awssdk.services.dynamodb.model.DynamoDbResponse
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.QueryResponse
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse
import software.amazon.awssdk.services.sts.model.Credentials
import java.net.URI

class DynamoDBClient(private val config: DynamoDBDataAccessProviderDataAccessProviderConfig): ManagedObject<DynamoDBDataAccessProviderDataAccessProviderConfig>(config)
{
    private val _awsRegion = Region.of(config.getAwsRegion().awsRegion)
    private val client: DynamoDbClient = initializeDynamoDBClient()

    private fun initializeDynamoDBClient(): DynamoDbClient
    {
        val accessMethod = config.getDynamodbAccessMethod()
        /*Use Instance Profile from IAM Role applied to EC2 instance*/
        var creds: AwsCredentialsProvider?

        if (accessMethod.isEC2InstanceProfile.isPresent && accessMethod.isEC2InstanceProfile.get()) {
            creds = InstanceProfileCredentialsProvider.builder().build()
        } else if (accessMethod.accessKeyIdAndSecret.isPresent) {
            val keyIdAndSecret = accessMethod.accessKeyIdAndSecret.get()
            creds = StaticCredentialsProvider.create(AwsBasicCredentials.create(keyIdAndSecret.accessKeyId.get(), keyIdAndSecret.accessKeySecret.get()))

            /* roleARN is present, get temporary credentials through AssumeRole */
            if (keyIdAndSecret.awsRoleARN.isPresent) {
                creds = getNewCredentialsFromAssumeRole(creds, keyIdAndSecret.awsRoleARN.get())
            }
        } else if (accessMethod.aWSProfile.get().awsProfileName.isPresent) {
            val awsProfile = accessMethod.aWSProfile.get()
            creds = ProfileCredentialsProvider.builder()
                    .profileName(awsProfile.awsProfileName.get())
                    .build()

            /* roleARN is present, get temporary credentials through AssumeRole */
            if (awsProfile.awsRoleARN.isPresent)
            {
                creds = getNewCredentialsFromAssumeRole(creds, awsProfile.awsRoleARN.get())
            }
        } else if (accessMethod.aWSDirect.get().hostname.isPresent) {
            val aWSDirect = accessMethod.aWSDirect.get()
            creds = StaticCredentialsProvider.create(AwsBasicCredentials.create(aWSDirect.accessKeyId.get(), aWSDirect.accessKeySecret.get()))

            /* roleARN is present, get temporary credentials through AssumeRole */
            if (aWSDirect.awsRoleARN.isPresent) {
                creds = getNewCredentialsFromAssumeRole(creds, aWSDirect.awsRoleARN.get())
            }
        } else {
            creds = null
        }

        if(!accessMethod.aWSDirect.get().hostname.isPresent) {
            return  DynamoDbClient.builder()
                .region(_awsRegion)
                .credentialsProvider(creds)
                .build()
        } else {
            val aWSDirect = accessMethod.aWSDirect.get()
            val hostname: String = aWSDirect.hostname.get()
            val endpoint = URI(hostname)

            return DynamoDbClient.builder()
                .endpointOverride(endpoint)
                .credentialsProvider(creds)
                .build()
        }
    }

    private fun getNewCredentialsFromAssumeRole(creds: AwsCredentialsProvider, roleARN: String): AwsCredentialsProvider
    {
        val stsClient: StsClient = StsClient.builder()
                .region(_awsRegion)
                .credentialsProvider(creds)
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
                logger.warn("AssumeRole Request sent but was not successful: {}",
                        assumeRoleResult.sdkHttpResponse().statusText().get())
                creds //Returning the original credentials
            }
            else
            {
                val credentials: Credentials = assumeRoleResult.credentials()
                val asc: AwsSessionCredentials = AwsSessionCredentials.create(credentials.accessKeyId(), credentials.secretAccessKey(), credentials.sessionToken())
                logger.debug("AssumeRole Request successful: {}", assumeRoleResult.sdkHttpResponse().statusText())
                StaticCredentialsProvider.create(asc) //returning temp credentials from the assumed role
            }
        }
        catch (e: Exception)
        {
            logger.debug("AssumeRole Request failed: {}", e.message)
            throw config.getExceptionFactory().internalServerException(ErrorCode.EXTERNAL_SERVICE_ERROR)
        }
    }

    fun getItem(getItemRequest: GetItemRequest): GetItemResponse = callClient(ClientMethod.GetItem, getItemRequest) as GetItemResponse
    fun putItem(request: PutItemRequest): PutItemResponse = callClient(ClientMethod.PutItem, request) as PutItemResponse
    fun updateItem(request: UpdateItemRequest): UpdateItemResponse = callClient(ClientMethod.UpdateItem, request) as UpdateItemResponse
    fun deleteItem(request: DeleteItemRequest): DeleteItemResponse = callClient(ClientMethod.DeleteItem, request) as DeleteItemResponse
    fun query(request: QueryRequest): QueryResponse = callClient(ClientMethod.Query, request) as QueryResponse
    fun scan(request: ScanRequest): ScanResponse = callClient(ClientMethod.Scan, request) as ScanResponse

    private fun callClient(method: ClientMethod, request: DynamoDbRequest): DynamoDbResponse? {
        return try {
            when (method) {
                ClientMethod.GetItem -> client.getItem(request as GetItemRequest)
                ClientMethod.PutItem -> client.putItem(request as PutItemRequest)
                ClientMethod.UpdateItem -> client.updateItem(request as UpdateItemRequest)
                ClientMethod.DeleteItem -> client.deleteItem(request as DeleteItemRequest)
                ClientMethod.Query -> client.query(request as QueryRequest)
                ClientMethod.Scan -> client.scan(request as ScanRequest)
            }
        } catch (e: DynamoDbException) {
            when
            {
                e.awsErrorDetails()?.errorCode() == "ConditionalCheckFailedException" -> throw e
                e.awsErrorDetails()?.errorCode() == "UnrecognizedClientException" -> throw ExternalServiceFailedAuthenticationAlarmException(e)
                e.statusCode() >= 500 -> throw ExternalServiceFailedConnectionAlarmException(e)
                else -> throw ExternalServiceFailedCommunicationAlarmException(e)
            }

        } catch (e: SdkException) {
            throw ExternalServiceFailedCommunicationAlarmException(e)
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DynamoDBClient::class.java)
    }

    enum class ClientMethod {
        GetItem, PutItem, UpdateItem, DeleteItem, Query, Scan
    }
}
