from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_apigateway as apigateway,
    aws_lambda_event_sources as lambda_event_sources,
    aws_events as events,
    aws_events_targets as targets,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    CfnOutput,
    Fn,
)
from constructs import Construct


class MyHayatiApplicationStack(Stack):
    """Application stack containing Lambda functions, API Gateway, and Step Functions"""

    def __init__(self, scope: Construct, id: str, infra_stack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Import values from infrastructure stack
        lambda_role = infra_stack.lambda_role
        users_table = infra_stack.users_table
        orders_table = infra_stack.orders_table
        export_jobs_table = infra_stack.export_jobs_table
        third_party_data_table = infra_stack.third_party_data_table
        orders_queue = infra_stack.orders_queue
        orders_dlq = infra_stack.orders_dlq
        export_jobs_queue = infra_stack.export_jobs_queue
        user_files_bucket = infra_stack.user_files_bucket
        reports_bucket = infra_stack.reports_bucket
        user_notification_topic = infra_stack.user_notification_topic
        alarm_topic = infra_stack.alarm_topic
        search_domain = infra_stack.search_domain
        user_pool = infra_stack.user_pool
        user_pool_client = infra_stack.user_pool_client

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

        requests_layer = _lambda.LayerVersion(
            self,
            "RequestsLayer",
            code=_lambda.Code.from_asset("lambda/layers/requests_layer"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="Requests library for HTTP calls to 3rd party APIs"
        )

        layers = [base_layer, generic_layer, requests_layer]

        # ---------------------------------------------------------------------
        # Lambda Functions Helper
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
                    "EXPORT_JOBS_TABLE": export_jobs_table.table_name,
                    "EXPORT_JOBS_QUEUE_URL": export_jobs_queue.queue_url,
                    "OPENSEARCH_ENDPOINT": f"https://{search_domain.domain_endpoint}",
                    "USER_FILES_BUCKET": user_files_bucket.bucket_name,
                    "REPORTS_BUCKET": reports_bucket.bucket_name,
                    "USER_NOTIFICATION_TOPIC_ARN": user_notification_topic.topic_arn,
                    "THIRD_PARTY_DATA_TABLE": third_party_data_table.table_name,
                },
            )

        # ---------------------------------------------------------------------
        # Lambda Functions - User Management
        # ---------------------------------------------------------------------
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

        stream_processor_fn = create_lambda(
            "StreamProcessorFunction", "stream_processor.handler", "lambda/stream_processor", timeout=60, memory=512
        )

        # ---------------------------------------------------------------------
        # Lambda Functions - File Management
        # ---------------------------------------------------------------------
        generate_upload_url_fn = create_lambda(
            "GenerateUploadUrlFunction", "generate_upload_url.handler", "lambda/generate_upload_url"
        )

        generate_download_url_fn = create_lambda(
            "GenerateDownloadUrlFunction", "generate_download_url.handler", "lambda/generate_download_url"
        )

        delete_file_fn = create_lambda(
            "DeleteFileFunction", "delete_file.handler", "lambda/delete_file"
        )

        # ---------------------------------------------------------------------
        # Lambda Functions - Order Management
        # ---------------------------------------------------------------------
        create_order_fn = create_lambda(
            "CreateOrderFunction", "create_order.handler", "lambda/create_order"
        )

        get_order_fn = create_lambda(
            "GetOrderFunction", "get_order.handler", "lambda/get_order"
        )

        list_orders_fn = create_lambda(
            "ListOrdersFunction", "list_orders.handler", "lambda/list_orders"
        )

        process_order_fn = create_lambda(
            "ProcessOrderFunction", "process_order.handler", "lambda/process_order", timeout=60, memory=512
        )

        # ---------------------------------------------------------------------
        # Lambda Functions - Reports & Testing
        # ---------------------------------------------------------------------
        daily_report_fn = create_lambda(
            "DailyReportFunction", "daily_report.handler", "lambda/daily_report", timeout=300, memory=1024
        )

        test_sns_fn = create_lambda(
            "TestSNSFunction", "test_sns.handler", "lambda/test_sns"
        )

        # ---------------------------------------------------------------------
        # Lambda Functions - Export Jobs
        # ---------------------------------------------------------------------
        create_export_job_fn = create_lambda(
            "CreateExportJobFunction", "create_export_job.handler", "lambda/create_export_job", timeout=30, memory=256
        )

        get_export_job_fn = create_lambda(
            "GetExportJobFunction", "get_export_job.handler", "lambda/get_export_job", timeout=30, memory=256
        )

        process_export_job_fn = create_lambda(
            "ProcessExportJobFunction", "process_export_job.handler", "lambda/process_export_job", timeout=300, memory=1024
        )

        # ---------------------------------------------------------------------
        # Lambda Functions - 3rd Party Integration
        # ---------------------------------------------------------------------
        sync_third_party_fn = _lambda.Function(
            self,
            "SyncThirdPartyFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="sync_third_party.handler",
            code=_lambda.Code.from_asset("lambda/sync_third_party_with_retry"),
            layers=[base_layer, generic_layer, requests_layer],
            role=lambda_role,
            timeout=Duration.seconds(60),
            memory_size=512,
            environment={
                "THIRD_PARTY_DATA_TABLE": third_party_data_table.table_name,
            },
        )

        get_third_party_data_fn = _lambda.Function(
            self,
            "GetThirdPartyDataFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="get_third_party_data.handler",
            code=_lambda.Code.from_asset("lambda/get_third_party_data"),
            layers=[base_layer, generic_layer],
            role=lambda_role,
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "THIRD_PARTY_DATA_TABLE": third_party_data_table.table_name,
            },
        )

        # ---------------------------------------------------------------------
        # Lambda Functions - Cognito Triggers
        # ---------------------------------------------------------------------
        cognito_post_confirmation_fn = create_lambda(
            "CognitoPostConfirmationFunction",
            "cognito_post_confirmation.handler",
            "lambda/cognito_post_confirmation",
            timeout=30,
            memory=256
        )

        # ---------------------------------------------------------------------
        # Lambda Functions - Step Functions Integration
        # ---------------------------------------------------------------------
        sync_with_retry_fn = _lambda.Function(
            self,
            "SyncThirdPartyWithRetryFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="sync_third_party_with_retry.handler",
            code=_lambda.Code.from_asset("lambda/sync_third_party_with_retry"),
            layers=[base_layer, generic_layer, requests_layer],
            role=lambda_role,
            timeout=Duration.seconds(60),
            memory_size=512,
            environment={
                "THIRD_PARTY_DATA_TABLE": third_party_data_table.table_name,
                "ALARM_TOPIC_ARN": alarm_topic.topic_arn,
            },
        )

        # ---------------------------------------------------------------------
        # Step Functions - Third Party Sync with Retry
        # ---------------------------------------------------------------------
        sync_task = tasks.LambdaInvoke(
            self,
            "SyncThirdPartyTask",
            lambda_function=sync_with_retry_fn,
            payload=sfn.TaskInput.from_object({
                "resource_type": sfn.JsonPath.string_at("$.resource_type"),
                "limit": sfn.JsonPath.number_at("$.limit"),
                "attempt.$": "$.attempt"
            }),
            result_path="$.syncResult",
            retry_on_service_exceptions=False
        )

        check_success = sfn.Choice(self, "CheckSyncSuccess")

        sync_succeeded = sfn.Succeed(
            self,
            "SyncSucceeded",
            comment="Third party sync completed successfully"
        )

        increment_attempt = sfn.Pass(
            self,
            "IncrementAttempt",
            parameters={
                "resource_type.$": "$.resource_type",
                "limit.$": "$.limit",
                "attempt.$": "States.MathAdd($.attempt, 1)"
            }
        )

        wait_before_retry = sfn.Wait(
            self,
            "WaitBeforeRetry",
            time=sfn.WaitTime.duration(Duration.seconds(30))
        )

        max_retries_reached = sfn.Fail(
            self,
            "MaxRetriesReached",
            cause="Maximum retry attempts reached",
            error="ThirdPartySyncFailed"
        )

        check_retry_limit = sfn.Choice(self, "CheckRetryLimit")

        definition = (
            sync_task
            .next(check_success
                  .when(
                    sfn.Condition.boolean_equals("$.syncResult.Payload.success", True),
                    sync_succeeded
                  )
                  .otherwise(
                    increment_attempt
                    .next(check_retry_limit
                          .when(
                            sfn.Condition.number_less_than("$.attempt", 4),
                            wait_before_retry.next(sync_task)
                          )
                          .otherwise(max_retries_reached)
                    )
                  )
            )
        )

        sync_state_machine = sfn.StateMachine(
            self,
            "ThirdPartySyncStateMachine",
            state_machine_name="myhayati-third-party-sync",
            definition=definition,
            timeout=Duration.minutes(15),
            tracing_enabled=True,
        )

        # Don't use grant_invoke - it modifies the role in the other stack
        # sync_with_retry_fn.grant_invoke(sync_state_machine)

        trigger_sync_fn = _lambda.Function(
            self,
            "TriggerSyncFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="trigger_sync.handler",
            code=_lambda.Code.from_asset("lambda/trigger_sync"),
            layers=[base_layer, generic_layer],
            role=lambda_role,
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "STATE_MACHINE_ARN": sync_state_machine.state_machine_arn,
            },
        )

        # Don't use grant_start_execution - it modifies the role in the other stack
        # Permission already added to Lambda role in infrastructure stack
        # sync_state_machine.grant_start_execution(trigger_sync_fn)

        # ---------------------------------------------------------------------
        # Lambda Event Sources
        # ---------------------------------------------------------------------
        stream_processor_fn.add_event_source(
            lambda_event_sources.DynamoEventSource(
                users_table,
                starting_position=_lambda.StartingPosition.TRIM_HORIZON,
                batch_size=100,
                bisect_batch_on_error=True,
                retry_attempts=3,
                max_batching_window=Duration.seconds(10),
            )
        )

        process_order_fn.add_event_source(
            lambda_event_sources.SqsEventSource(
                orders_queue,
                batch_size=10,
                max_batching_window=Duration.seconds(5),
                report_batch_item_failures=True,
            )
        )

        process_export_job_fn.add_event_source(
            lambda_event_sources.SqsEventSource(
                export_jobs_queue,
                batch_size=10,
                max_batching_window=Duration.seconds(5),
                report_batch_item_failures=True,
            )
        )

        # ---------------------------------------------------------------------
        # EventBridge Rules
        # ---------------------------------------------------------------------
        payment_batch_rule = events.Rule(
            self,
            "PaymentBatchRule",
            rule_name="myhayati-payment-batch-processing",
            description="Schedule batch payment pending processing at 11 PM UTC daily",
            schedule=events.Schedule.cron(
                minute="0",
                hour="23",
                month="*",
                week_day="*",
                year="*"
            ),
            enabled=True,
        )

        payment_batch_rule.add_target(
            targets.LambdaFunction(daily_report_fn)
        )

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

        # ---------------------------------------------------------------------
        # Cognito Authorizer
        # ---------------------------------------------------------------------
        cfn_authorizer = apigateway.CfnAuthorizer(
            self,
            "CognitoCfnAuthorizer",
            rest_api_id=api.rest_api_id,
            name="myhayati-cognito-authorizer",
            type="COGNITO_USER_POOLS",
            identity_source="method.request.header.Authorization",
            provider_arns=[user_pool.user_pool_arn],
        )

        def attach_cognito_to_method(method: apigateway.Method):
            cfn_method = method.node.default_child
            if isinstance(cfn_method, apigateway.CfnMethod):
                cfn_method.authorizer_id = cfn_authorizer.ref
                cfn_method.authorization_type = "COGNITO_USER_POOLS"

        # User routes - PROTECTED
        users = api.root.add_resource("users")
        m = users.add_method(
            "POST",
            apigateway.LambdaIntegration(create_user_fn),
        )
        attach_cognito_to_method(m)

        user_by_id = users.add_resource("{userId}")
        m = user_by_id.add_method(
            "GET",
            apigateway.LambdaIntegration(get_user_fn),
        )
        attach_cognito_to_method(m)
        m = user_by_id.add_method(
            "PUT",
            apigateway.LambdaIntegration(update_user_fn),
        )
        attach_cognito_to_method(m)
        m = user_by_id.add_method(
            "DELETE",
            apigateway.LambdaIntegration(delete_user_fn),
        )
        attach_cognito_to_method(m)

        # File routes - PROTECTED
        files = user_by_id.add_resource("files")

        upload_url = files.add_resource("upload-url")
        m = upload_url.add_method(
            "POST",
            apigateway.LambdaIntegration(generate_upload_url_fn),
        )
        attach_cognito_to_method(m)

        file_by_type = files.add_resource("{fileType}")
        m = file_by_type.add_method(
            "GET",
            apigateway.LambdaIntegration(generate_download_url_fn),
        )
        attach_cognito_to_method(m)
        m = file_by_type.add_method(
            "DELETE",
            apigateway.LambdaIntegration(delete_file_fn),
        )
        attach_cognito_to_method(m)

        # Search routes - PUBLIC
        search = users.add_resource("search")
        search.add_method("GET", apigateway.LambdaIntegration(search_users_fn))

        # Order routes - PROTECTED
        orders = api.root.add_resource("orders")
        m = orders.add_method(
            "POST",
            apigateway.LambdaIntegration(create_order_fn),
        )
        attach_cognito_to_method(m)
        m = orders.add_method(
            "GET",
            apigateway.LambdaIntegration(list_orders_fn),
        )
        attach_cognito_to_method(m)

        order_by_id = orders.add_resource("{orderId}")
        m = order_by_id.add_method(
            "GET",
            apigateway.LambdaIntegration(get_order_fn),
        )
        attach_cognito_to_method(m)

        # Export routes - PROTECTED
        exports = api.root.add_resource("exports")
        m = exports.add_method(
            "POST",
            apigateway.LambdaIntegration(create_export_job_fn),
        )
        attach_cognito_to_method(m)
        m = exports.add_method(
            "GET",
            apigateway.LambdaIntegration(get_export_job_fn),
        )
        attach_cognito_to_method(m)

        export_by_id = exports.add_resource("{jobId}")
        m = export_by_id.add_method(
            "GET",
            apigateway.LambdaIntegration(get_export_job_fn),
        )
        attach_cognito_to_method(m)

        # Integration routes - PUBLIC
        integrations = api.root.add_resource("integrations")

        sync_route = integrations.add_resource("sync")
        sync_route.add_method("POST", apigateway.LambdaIntegration(sync_third_party_fn))

        data_route = integrations.add_resource("data")
        data_route.add_method("GET", apigateway.LambdaIntegration(get_third_party_data_fn))

        # Step Functions trigger route
        sync_with_retry_route = integrations.add_resource("sync-with-retry")
        sync_with_retry_route.add_method("POST", apigateway.LambdaIntegration(trigger_sync_fn))

        # Test routes - PUBLIC
        test = api.root.add_resource("test")
        test_sns = test.add_resource("sns")
        test_sns.add_method("GET", apigateway.LambdaIntegration(test_sns_fn))

        # ---------------------------------------------------------------------
        # CloudFormation Outputs
        # ---------------------------------------------------------------------
        CfnOutput(self, "ApiUrl", value=api.url)
        CfnOutput(self, "StateMachineArn", value=sync_state_machine.state_machine_arn)