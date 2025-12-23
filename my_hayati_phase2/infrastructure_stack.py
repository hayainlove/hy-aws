from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_iam as iam,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_opensearchservice as opensearch,
    aws_cognito as cognito,
    CfnOutput,
)
from constructs import Construct


class MyHayatiInfrastructureStack(Stack):
    """Infrastructure stack containing all foundational resources"""

    def __init__(self, scope: Construct, id: str, alarm_email: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # ---------------------------------------------------------------------
        # DynamoDB - Users Table
        # ---------------------------------------------------------------------
        self.users_table = dynamodb.Table(
            self,
            "UsersTable",
            table_name="users",
            partition_key=dynamodb.Attribute(
                name="user_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery=True,
        )

        self.users_table.add_global_secondary_index(
            index_name="EmailIndex",
            partition_key=dynamodb.Attribute(
                name="email",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        self.users_table.add_global_secondary_index(
            index_name="UserNameIndex",
            partition_key=dynamodb.Attribute(
                name="userName",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # ---------------------------------------------------------------------
        # DynamoDB - Orders Table
        # ---------------------------------------------------------------------
        self.orders_table = dynamodb.Table(
            self,
            "OrdersTable",
            table_name="orders",
            partition_key=dynamodb.Attribute(
                name="order_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery=True,
        )

        self.orders_table.add_global_secondary_index(
            index_name="UserOrdersIndex",
            partition_key=dynamodb.Attribute(
                name="user_id",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="createdAt",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        self.orders_table.add_global_secondary_index(
            index_name="OrderStatusIndex",
            partition_key=dynamodb.Attribute(
                name="status",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="createdAt",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # ---------------------------------------------------------------------
        # DynamoDB - Export Jobs Table
        # ---------------------------------------------------------------------
        self.export_jobs_table = dynamodb.Table(
            self,
            "ExportJobsTable",
            table_name="export_jobs",
            partition_key=dynamodb.Attribute(
                name="job_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery=False,
            time_to_live_attribute="ttl",
        )

        self.export_jobs_table.add_global_secondary_index(
            index_name="UserJobsIndex",
            partition_key=dynamodb.Attribute(
                name="user_id",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="createdAt",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # ---------------------------------------------------------------------
        # DynamoDB - Third Party Data Table
        # ---------------------------------------------------------------------
        self.third_party_data_table = dynamodb.Table(
            self,
            "ThirdPartyDataTable",
            table_name="third_party_data",
            partition_key=dynamodb.Attribute(
                name="item_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery=False,
        )

        self.third_party_data_table.add_global_secondary_index(
            index_name="SourceIndex",
            partition_key=dynamodb.Attribute(
                name="source",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="synced_at",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # ---------------------------------------------------------------------
        # SQS Queues for Order Processing
        # ---------------------------------------------------------------------
        self.orders_dlq = sqs.Queue(
            self,
            "OrdersDLQ",
            queue_name="orders-dead-letter-queue",
            retention_period=Duration.days(14),
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.orders_queue = sqs.Queue(
            self,
            "OrdersQueue",
            queue_name="orders-queue",
            visibility_timeout=Duration.seconds(300),
            receive_message_wait_time=Duration.seconds(20),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=self.orders_dlq
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ---------------------------------------------------------------------
        # SQS Queues for Export Jobs
        # ---------------------------------------------------------------------
        self.export_jobs_dlq = sqs.Queue(
            self,
            "ExportJobsDLQ",
            queue_name="export-jobs-dead-letter-queue",
            retention_period=Duration.days(14),
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.export_jobs_queue = sqs.Queue(
            self,
            "ExportJobsQueue",
            queue_name="export-jobs-queue",
            visibility_timeout=Duration.seconds(300),
            receive_message_wait_time=Duration.seconds(20),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=self.export_jobs_dlq
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ---------------------------------------------------------------------
        # S3 Bucket for User Files
        # ---------------------------------------------------------------------
        self.user_files_bucket = s3.Bucket(
            self,
            "UserFilesBucket",
            bucket_name=f"myhayati-user-files-{self.account}-{self.region}",
            versioned=False,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            cors=[
                s3.CorsRule(
                    allowed_methods=[
                        s3.HttpMethods.GET,
                        s3.HttpMethods.PUT,
                        s3.HttpMethods.POST,
                    ],
                    allowed_origins=["*"],
                    allowed_headers=["*"],
                    max_age=3000,
                )
            ],
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="DeleteOldVersions",
                    abort_incomplete_multipart_upload_after=Duration.days(7),
                    noncurrent_version_expiration=Duration.days(30),
                )
            ],
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ---------------------------------------------------------------------
        # S3 Bucket for Reports
        # ---------------------------------------------------------------------
        self.reports_bucket = s3.Bucket(
            self,
            "ReportsBucket",
            bucket_name=f"myhayati-reports-{self.account}-{self.region}",
            versioned=False,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="DeleteOldReports",
                    expiration=Duration.days(90),
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(30)
                        )
                    ]
                )
            ],
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ---------------------------------------------------------------------
        # SNS Topics
        # ---------------------------------------------------------------------
        self.alarm_topic = sns.Topic(
            self,
            "UserLambdaAlarmTopic",
            topic_name="myhayati-user-lambda-alarms",
        )

        self.alarm_topic.add_subscription(
            subs.EmailSubscription(alarm_email)
        )

        self.user_notification_topic = sns.Topic(
            self,
            "UserNotificationTopic",
            topic_name="myhayati-user-notifications",
            display_name="MyHayati User Notifications"
        )

        self.user_notification_topic.add_subscription(
            subs.EmailSubscription(alarm_email)
        )

        # ---------------------------------------------------------------------
        # OpenSearch Domain
        # ---------------------------------------------------------------------
        opensearch_master_role = iam.Role(
            self,
            "OpenSearchMasterRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Master role for OpenSearch domain",
        )

        self.search_domain = opensearch.Domain(
            self,
            "MyHayatiSearchDomain",
            version=opensearch.EngineVersion.OPENSEARCH_2_11,
            capacity=opensearch.CapacityConfig(
                data_node_instance_type="t3.small.search",
                data_nodes=1,
                multi_az_with_standby_enabled=False,
            ),
            ebs=opensearch.EbsOptions(volume_size=10),
            zone_awareness=opensearch.ZoneAwarenessConfig(enabled=False),
            fine_grained_access_control=opensearch.AdvancedSecurityOptions(
                master_user_arn=opensearch_master_role.role_arn,
            ),
            enforce_https=True,
            node_to_node_encryption=True,
            encryption_at_rest=opensearch.EncryptionAtRestOptions(enabled=True),
            use_unsigned_basic_auth=False,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ---------------------------------------------------------------------
        # Cognito User Pool
        # ---------------------------------------------------------------------
        self.user_pool = cognito.UserPool(
            self,
            "MyHayatiUserPool",
            user_pool_name="myhayati-user-pool",
            self_sign_up_enabled=True,
            sign_in_aliases=cognito.SignInAliases(
                email=True,
                username=False,
            ),
            auto_verify=cognito.AutoVerifiedAttrs(
                email=True
            ),
            standard_attributes=cognito.StandardAttributes(
                email=cognito.StandardAttribute(
                    required=True,
                    mutable=True
                ),
                given_name=cognito.StandardAttribute(
                    required=False,
                    mutable=True
                ),
                family_name=cognito.StandardAttribute(
                    required=False,
                    mutable=True
                ),
                phone_number=cognito.StandardAttribute(
                    required=False,
                    mutable=True
                ),
            ),
            password_policy=cognito.PasswordPolicy(
                min_length=8,
                require_lowercase=True,
                require_uppercase=True,
                require_digits=True,
                require_symbols=False,
            ),
            account_recovery=cognito.AccountRecovery.EMAIL_ONLY,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.user_pool_client = self.user_pool.add_client(
            "MyHayatiUserPoolClient",
            user_pool_client_name="myhayati-app-client",
            auth_flows=cognito.AuthFlow(
                user_password=True,
                user_srp=True,
                custom=False,
                admin_user_password=True,
            ),
            generate_secret=False,
            access_token_validity=Duration.hours(1),
            id_token_validity=Duration.hours(1),
            refresh_token_validity=Duration.days(30),
        )

        # ---------------------------------------------------------------------
        # IAM Role for Lambda Functions with ALL permissions upfront
        # ---------------------------------------------------------------------
        self.lambda_role = iam.Role(
            self,
            "MyHayatiLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Role for Lambda functions with all required permissions",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
            inline_policies={
                "LambdaPermissions": iam.PolicyDocument(
                    statements=[
                        # DynamoDB permissions
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "dynamodb:GetItem",
                                "dynamodb:PutItem",
                                "dynamodb:UpdateItem",
                                "dynamodb:DeleteItem",
                                "dynamodb:Query",
                                "dynamodb:Scan",
                                "dynamodb:BatchGetItem",
                                "dynamodb:BatchWriteItem",
                                "dynamodb:DescribeStream",
                                "dynamodb:GetRecords",
                                "dynamodb:GetShardIterator",
                                "dynamodb:ListStreams",
                            ],
                            resources=[
                                self.users_table.table_arn,
                                f"{self.users_table.table_arn}/index/*",
                                f"{self.users_table.table_arn}/stream/*",
                                self.orders_table.table_arn,
                                f"{self.orders_table.table_arn}/index/*",
                                self.export_jobs_table.table_arn,
                                f"{self.export_jobs_table.table_arn}/index/*",
                                self.third_party_data_table.table_arn,
                                f"{self.third_party_data_table.table_arn}/index/*",
                            ],
                        ),
                        # S3 permissions
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:DeleteObject",
                                "s3:ListBucket",
                            ],
                            resources=[
                                self.user_files_bucket.bucket_arn,
                                f"{self.user_files_bucket.bucket_arn}/*",
                                self.reports_bucket.bucket_arn,
                                f"{self.reports_bucket.bucket_arn}/*",
                            ],
                        ),
                        # SQS permissions
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "sqs:SendMessage",
                                "sqs:ReceiveMessage",
                                "sqs:DeleteMessage",
                                "sqs:GetQueueAttributes",
                                "sqs:GetQueueUrl",
                            ],
                            resources=[
                                self.orders_queue.queue_arn,
                                self.orders_dlq.queue_arn,
                                self.export_jobs_queue.queue_arn,
                                self.export_jobs_dlq.queue_arn,
                            ],
                        ),
                        # SNS permissions
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "sns:Publish",
                            ],
                            resources=[
                                self.alarm_topic.topic_arn,
                                self.user_notification_topic.topic_arn,
                            ],
                        ),
                        # OpenSearch permissions
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "es:ESHttpGet",
                                "es:ESHttpPut",
                                "es:ESHttpPost",
                                "es:ESHttpDelete",
                                "es:ESHttpHead",
                            ],
                            resources=[
                                f"{self.search_domain.domain_arn}/*"
                            ],
                        ),
                        # Step Functions permissions (wildcard since state machine created in app stack)
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "states:StartExecution",
                            ],
                            resources=[
                                f"arn:aws:states:{self.region}:{self.account}:stateMachine:*"
                            ],
                        ),
                        # Lambda invoke permissions for Step Functions
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "lambda:InvokeFunction",
                            ],
                            resources=[
                                f"arn:aws:lambda:{self.region}:{self.account}:function:*"
                            ],
                        ),
                    ]
                )
            }
        )

        # ---------------------------------------------------------------------
        # CloudFormation Outputs
        # ---------------------------------------------------------------------
        CfnOutput(self, "UsersTableName", value=self.users_table.table_name)
        CfnOutput(self, "UsersTableArn", value=self.users_table.table_arn)
        CfnOutput(self, "UsersTableStreamArn", value=self.users_table.table_stream_arn or "N/A")
        CfnOutput(self, "OrdersTableName", value=self.orders_table.table_name)
        CfnOutput(self, "OrdersTableArn", value=self.orders_table.table_arn)
        CfnOutput(self, "ExportJobsTableName", value=self.export_jobs_table.table_name)
        CfnOutput(self, "ExportJobsTableArn", value=self.export_jobs_table.table_arn)
        CfnOutput(self, "ThirdPartyDataTableName", value=self.third_party_data_table.table_name)
        CfnOutput(self, "ThirdPartyDataTableArn", value=self.third_party_data_table.table_arn)
        
        CfnOutput(self, "OrdersQueueUrl", value=self.orders_queue.queue_url)
        CfnOutput(self, "OrdersQueueArn", value=self.orders_queue.queue_arn)
        CfnOutput(self, "OrdersDLQUrl", value=self.orders_dlq.queue_url)
        CfnOutput(self, "ExportJobsQueueUrl", value=self.export_jobs_queue.queue_url)
        CfnOutput(self, "ExportJobsQueueArn", value=self.export_jobs_queue.queue_arn)
        CfnOutput(self, "ExportJobsDLQUrl", value=self.export_jobs_dlq.queue_url)
        
        CfnOutput(self, "UserFilesBucketName", value=self.user_files_bucket.bucket_name)
        CfnOutput(self, "UserFilesBucketArn", value=self.user_files_bucket.bucket_arn)
        CfnOutput(self, "ReportsBucketName", value=self.reports_bucket.bucket_name)
        CfnOutput(self, "ReportsBucketArn", value=self.reports_bucket.bucket_arn)
        
        CfnOutput(self, "AlarmTopicArn", value=self.alarm_topic.topic_arn)
        CfnOutput(self, "UserNotificationTopicArn", value=self.user_notification_topic.topic_arn)
        
        CfnOutput(self, "OpenSearchEndpoint", value=f"https://{self.search_domain.domain_endpoint}")
        CfnOutput(self, "OpenSearchDomainArn", value=self.search_domain.domain_arn)
        
        CfnOutput(self, "UserPoolId", value=self.user_pool.user_pool_id)
        CfnOutput(self, "UserPoolArn", value=self.user_pool.user_pool_arn)
        CfnOutput(self, "UserPoolClientId", value=self.user_pool_client.user_pool_client_id)
        
        CfnOutput(self, "LambdaRoleArn", value=self.lambda_role.role_arn, 
                  description="Lambda execution role ARN for application stack")