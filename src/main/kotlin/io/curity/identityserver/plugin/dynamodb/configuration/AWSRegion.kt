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
package io.curity.identityserver.plugin.dynamodb.configuration

enum class AWSRegion(val awsRegion: String)
{
    ap_south_1("ap-south-1"),
    eu_south_1("eu-south-1"),
    us_gov_east_1("us-gov-east-1"),
    ca_central_1("ca-central-1"),
    eu_central_1("eu-central-1"),
    us_west_1("us-west-1"),
    us_west_2("us-west-2"),
    af_south_1("af-south-1"),
    eu_north_1("eu-north-1"),
    eu_west_3("eu-west-3"),
    eu_west_2("eu-west-2"),
    eu_west_1("eu-west-1"),
    ap_northeast_2("ap-northeast-2"),
    ap_northeast_1("ap-northeast-1"),
    me_south_1("me-south-1"),
    sa_east_1("sa-east-1"),
    ap_east_1("ap-east-1"),
    cn_north_1("cn-north-1"),
    us_gov_west_1("us-gov-west-1"),
    ap_southeast_1("ap-southeast-1"),
    ap_southeast_2("ap-southeast-2"),
    us_iso_east_1("us-iso-east-1"),
    us_east_1("us-east-1"),
    us_east_2("us-east-2"),
    cn_northwest_1("cn-northwest-1"),
    us_isob_east_1("us-isob-east-1"),
    aws_global("aws-global"),
    aws_cn_global("aws-cn-global"),
    aws_us_gov_global("aws-us-gov-global"),
    aws_iso_global("aws-iso-global"),
    aws_iso_b_global("aws-iso-b-global")
}