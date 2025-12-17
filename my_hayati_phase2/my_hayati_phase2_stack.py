from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_apigateway as apigateway,
    aws_cloudwatch as cloudwatch,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_cloudwatch_actions as cw_actions,
    aws_opensearchservice as opensearch,
    aws_lambda_event_sources as lambda_event_sources,
    aws_events as events,
    aws_events_targets as targets,
    CfnOutput,
)
from constructs import Construct


class MyHayatiPhase2Stack(Stack):

    def __init__(self, scope: Construct, id: str, alarm_email: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # ---------------------------------------------------------------------
        # DynamoDB
        # ---------------------------------------------------------------------
        users_table = dynamodb.Table(
            self,
            "UsersTable",
            table_name="users",
            partition_key=dynamodb.Attribute(
                name="user_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,  # Changed to capture both
            removal_policy=RemovalPolicy.RETAIN,  # Protect production data
            point_in_time_recovery=True,  # Enable backup
        )

        users_table.add_global_secondary_index(
            index_name="EmailIndex",
            partition_key=dynamodb.Attribute(
                name="email",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        users_table.add_global_secondary_index(
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
        orders_table = dynamodb.Table(
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

        # GSI to query orders by user
        orders_table.add_global_secondary_index(
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

        # GSI to query orders by status
        orders_table.add_global_secondary_index(
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
        # SQS Queues for Order Processing
        # ---------------------------------------------------------------------
        # Dead Letter Queue (DLQ) - for failed messages
        orders_dlq = sqs.Queue(
            self,
            "OrdersDLQ",
            queue_name="orders-dead-letter-queue",
            retention_period=Duration.days(14),  # Keep failed messages for 14 days
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Main Orders Queue
        orders_queue = sqs.Queue(
            self,
            "OrdersQueue",
            queue_name="orders-queue",
            visibility_timeout=Duration.seconds(300),  # 5 minutes (should be >= Lambda timeout)
            receive_message_wait_time=Duration.seconds(20),  # Long polling
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,  # After 3 failed attempts, move to DLQ
                queue=orders_dlq
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )
                
        # ---------------------------------------------------------------------
        # S3 Bucket for User Files
        # ---------------------------------------------------------------------
        user_files_bucket = s3.Bucket(
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
                        allowed_origins=["*"],  # In production, restrict to your domain
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
        # S3 Bucket for Reports (or use existing user_files_bucket)
        # ---------------------------------------------------------------------
        reports_bucket = s3.Bucket(
            self,
            "ReportsBucket",
            bucket_name=f"myhayati-reports-{self.account}-{self.region}",
            versioned=False,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="DeleteOldReports",
                    expiration=Duration.days(90),  # Auto-delete reports after 90 days
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
        # IAM Role for Lambda Functions
        # ---------------------------------------------------------------------
        lambda_role = iam.Role(
            self,
            "MyHayatiLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Role for Lambda functions to access DynamoDB and OpenSearch",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )

        users_table.grant_read_write_data(lambda_role)
        orders_table.grant_read_write_data(lambda_role)
        
        # Grant S3 permissions to Lambda role
        user_files_bucket.grant_read_write(lambda_role)
        reports_bucket.grant_read_write(lambda_role)
        
        # Grant SQS permissions to Lambda role
        orders_queue.grant_send_messages(lambda_role)  # For CreateOrder Lambda
        orders_queue.grant_consume_messages(lambda_role)  # For ProcessOrder Lambda
        orders_dlq.grant_consume_messages(lambda_role)  # To read DLQ messages
    

        # ---------------------------------------------------------------------
        # OpenSearch Domain
        # ---------------------------------------------------------------------
        # Create a separate IAM user for OpenSearch master user
        opensearch_master_role = iam.Role(
            self,
            "OpenSearchMasterRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Master role for OpenSearch domain",
        )

        search_domain = opensearch.Domain(
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

        # Grant Lambda role access to OpenSearch
        search_domain.grant_read_write(lambda_role)

        # ---------------------------------------------------------------------
        # OpenSearch Access Policy
        # ---------------------------------------------------------------------
        search_domain.add_access_policies(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                principals=[
                    iam.ArnPrincipal(lambda_role.role_arn),
                    iam.ArnPrincipal(opensearch_master_role.role_arn),
                ],
                actions=[
                    "es:ESHttpGet",
                    "es:ESHttpPost",
                    "es:ESHttpPut",
                    "es:ESHttpDelete",
                    "es:ESHttpHead",
                    "es:ESHttpPatch",
                ],
                resources=[f"{search_domain.domain_arn}/*"],
            )
        )

        # ---------------------------------------------------------------------
        # Outputs: OpenSearch
        # ---------------------------------------------------------------------
        CfnOutput(
            self,
            "OpenSearchEndpoint",
            value=f"https://{search_domain.domain_endpoint}",
        )

        # ---------------------------------------------------------------------
        # SNS + Alarms
        # ---------------------------------------------------------------------
        alarm_topic = sns.Topic(
            self,
            "UserLambdaAlarmTopic",
            topic_name="myhayati-user-lambda-alarms",
        )

        alarm_topic.add_subscription(
            subs.EmailSubscription(alarm_email)
        )

        # ---------------------------------------------------------------------
        # Lambda Layers
        # ---------------------------------------------------------------------
        base_layer = _lambda.LayerVersion(
            self,
            "BaseLayer",
            code=_lambda.Code.from_asset("lambda/layers/base"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
        )

        generic_layer = _lambda.LayerVersion(
            self,
            "GenericLayer",
            code=_lambda.Code.from_asset("lambda/layers/generic"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
        )

        layers = [base_layer, generic_layer]

        # ---------------------------------------------------------------------
        # Lambda Functions
        # ---------------------------------------------------------------------
        def create_lambda(id: str, handler: str, path: str, timeout: int = 30, memory: int = 256):
            return _lambda.Function(
                self,
                id,
                runtime=_lambda.Runtime.PYTHON_3_12,
                handler=handler,
                code=_lambda.Code.from_asset(path),
                layers=layers,
                role=lambda_role,
                timeout=Duration.seconds(timeout),
                memory_size=memory,
                environment={
                    "USERS_TABLE": users_table.table_name,
                    "ORDERS_TABLE": orders_table.table_name,
                    "ORDERS_QUEUE_URL": orders_queue.queue_url,
                    "ORDERS_DLQ_URL": orders_dlq.queue_url,
                    "OPENSEARCH_ENDPOINT": f"https://{search_domain.domain_endpoint}",
                    "USER_FILES_BUCKET": user_files_bucket.bucket_name,
                },
            )

        # API Lambda Functions
        create_user_fn = create_lambda(
            "CreateUserFunction", "create_user.handler", "lambda/create_user"
        )
        get_user_fn = create_lambda(
            "GetUserFunction", "get_user.handler", "lambda/get_user"
        )
        update_user_fn = create_lambda(
            "UpdateUserFunction", "update_user.handler", "lambda/update_user"
        )
        delete_user_fn = create_lambda(
            "DeleteUserFunction", "delete_user.handler", "lambda/delete_user"
        )
        search_users_fn = create_lambda(
            "SearchUsersFunction", "search_users.handler", "lambda/search_users", timeout=30, memory=256
        )

        # Stream Processor Lambda Function
        stream_processor_fn = create_lambda(
            "StreamProcessorFunction",
            "stream_processor.handler",
            "lambda/stream_processor",
            timeout=60,  # Longer timeout for batch processing
            memory=512,  # More memory for processing
        )
        
        # File Upload/Download Lambda Functions
        generate_upload_url_fn = create_lambda(
            "GenerateUploadUrlFunction",
            "generate_upload_url.handler",
            "lambda/generate_upload_url"
        )

        generate_download_url_fn = create_lambda(
            "GenerateDownloadUrlFunction",
            "generate_download_url.handler",
            "lambda/generate_download_url"
        )

        delete_file_fn = create_lambda(
            "DeleteFileFunction",
            "delete_file.handler",
            "lambda/delete_file"
        )
        
        # Order Processing Lambda Functions
        create_order_fn = create_lambda(
            "CreateOrderFunction",
            "create_order.handler",
            "lambda/create_order"
        )

        get_order_fn = create_lambda(
            "GetOrderFunction",
            "get_order.handler",
            "lambda/get_order"
        )

        list_orders_fn = create_lambda(
            "ListOrdersFunction",
            "list_orders.handler",
            "lambda/list_orders"
        )

        process_order_fn = create_lambda(
            "ProcessOrderFunction",
            "process_order.handler",
            "lambda/process_order",
            timeout=60,  # Longer timeout for order processing
            memory=512
        )
        
        daily_report_fn = create_lambda(
            "DailyReportFunction",
            "daily_report.handler",
            "lambda/daily_report",
            timeout=300,  # 5 minutes for large datasets
            memory=1024   # More memory for CSV processing
        )
        
        # Add environment variable for reports bucket
        daily_report_fn.add_environment("REPORTS_BUCKET", reports_bucket.bucket_name)
        
        # ---------------------------------------------------------------------
        # EventBridge Rule for Payment Batch Processing (11 PM UTC)
        # ---------------------------------------------------------------------
        # Process all pending payments at 11 PM UTC daily
        payment_batch_rule = events.Rule(
            self,
            "PaymentBatchRule",
            rule_name="myhayati-payment-batch-processing",
            description="Schedule batch payment pending processing at 11 PM UTC daily",
            schedule=events.Schedule.cron(
                minute="0",
                hour="23",  # 11 PM UTC
                month="*",
                week_day="*",
                year="*"
            ),
            enabled=True,  # Set to False if you want to enable it manually later
        )

        # Add Lambda as target
        payment_batch_rule.add_target(
            targets.LambdaFunction(daily_report_fn)
        )
        
        # Add DynamoDB Stream as event source for the stream processor
        stream_processor_fn.add_event_source(
            lambda_event_sources.DynamoEventSource(
                users_table,
                starting_position=_lambda.StartingPosition.TRIM_HORIZON,
                batch_size=100,  # Process up to 100 records at once
                bisect_batch_on_error=True,  # Split batch on error for better error handling
                retry_attempts=3,
                max_batching_window=Duration.seconds(10),  # Wait up to 10s to collect batch
            )
        )
        
        process_order_fn.add_event_source(
            lambda_event_sources.SqsEventSource(
                orders_queue,
                batch_size=10,  # Process up to 10 orders at once
                max_batching_window=Duration.seconds(5),  # Wait up to 5s to collect batch
                report_batch_item_failures=True,  # Enable partial batch responses
            )
        )

        # ---------------------------------------------------------------------
        # CloudWatch Alarms
        # ---------------------------------------------------------------------
        def add_error_alarm(name: str, fn: _lambda.Function):
            error_alarm = cloudwatch.Alarm(
                self,
                f"{name}ErrorAlarm",
                metric=fn.metric_errors(period=Duration.minutes(5)),
                threshold=1,
                evaluation_periods=1,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                alarm_description=f"Error alarm for {name}",
            )
            error_alarm.add_alarm_action(cw_actions.SnsAction(alarm_topic))

            # Add throttle alarm
            throttle_alarm = cloudwatch.Alarm(
                self,
                f"{name}ThrottleAlarm",
                metric=fn.metric_throttles(period=Duration.minutes(5)),
                threshold=5,
                evaluation_periods=1,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                alarm_description=f"Throttle alarm for {name}",
            )
            throttle_alarm.add_alarm_action(cw_actions.SnsAction(alarm_topic))

        add_error_alarm("CreateUser", create_user_fn)
        add_error_alarm("GetUser", get_user_fn)
        add_error_alarm("UpdateUser", update_user_fn)
        add_error_alarm("DeleteUser", delete_user_fn)
        add_error_alarm("StreamProcessor", stream_processor_fn)
        add_error_alarm("SearchUsers", search_users_fn)
        add_error_alarm("GenerateUploadUrl", generate_upload_url_fn)
        add_error_alarm("GenerateDownloadUrl", generate_download_url_fn)
        add_error_alarm("DeleteFile", delete_file_fn)
        add_error_alarm("CreateOrder", create_order_fn)
        add_error_alarm("GetOrder", get_order_fn)
        add_error_alarm("ListOrders", list_orders_fn)
        add_error_alarm("ProcessOrder", process_order_fn)
        add_error_alarm("DailyReport", daily_report_fn)

        # ---------------------------------------------------------------------
        # API Gateway
        # ---------------------------------------------------------------------
        api = apigateway.RestApi(
            self,
            "UserApi",
            rest_api_name="MyHayati User Service",
            deploy_options=apigateway.StageOptions(stage_name="dev"),
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
                allow_headers=["Content-Type", "Authorization"],
            ),
        )

        users = api.root.add_resource("users")
        users.add_method("POST", apigateway.LambdaIntegration(create_user_fn))

        user_by_id = users.add_resource("{userId}")
        user_by_id.add_method("GET", apigateway.LambdaIntegration(get_user_fn))
        user_by_id.add_method("PUT", apigateway.LambdaIntegration(update_user_fn))
        user_by_id.add_method("DELETE", apigateway.LambdaIntegration(delete_user_fn))
        
        files = user_by_id.add_resource("files")

        upload_url = files.add_resource("upload-url")
        upload_url.add_method("POST", apigateway.LambdaIntegration(generate_upload_url_fn))

        file_by_type = files.add_resource("{fileType}")
        file_by_type.add_method("GET", apigateway.LambdaIntegration(generate_download_url_fn))
        file_by_type.add_method("DELETE", apigateway.LambdaIntegration(delete_file_fn))
        
        search = users.add_resource("search")
        search.add_method(
            "GET", apigateway.LambdaIntegration(search_users_fn)
        )
        
        # Order routes
        orders = api.root.add_resource("orders")
        orders.add_method("POST", apigateway.LambdaIntegration(create_order_fn))
        orders.add_method("GET", apigateway.LambdaIntegration(list_orders_fn))

        order_by_id = orders.add_resource("{orderId}")
        order_by_id.add_method("GET", apigateway.LambdaIntegration(get_order_fn))

        CfnOutput(self, "ApiUrl", value=api.url)
        CfnOutput(self, "UsersTableName", value=users_table.table_name)
        CfnOutput(self, "UsersTableStreamArn", value=users_table.table_stream_arn or "N/A")
        CfnOutput(self, "UserFilesBucketName", value=user_files_bucket.bucket_name)
        CfnOutput(self, "OrdersTableName", value=orders_table.table_name)
        CfnOutput(self, "OrdersQueueUrl", value=orders_queue.queue_url)
        CfnOutput(self, "OrdersQueueArn", value=orders_queue.queue_arn)
        CfnOutput(self, "OrdersDLQUrl", value=orders_dlq.queue_url)
        CfnOutput(self,"ReportsBucketName", value=reports_bucket.bucket_name, description="S3 bucket where daily reports are stored")
        CfnOutput(self,"DailyReportSchedule", value="Daily at 11:00 PM UTC (7:00 AM Malaysia Time next day)", description="Schedule for daily CSV reports")
