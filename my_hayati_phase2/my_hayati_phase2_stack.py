from aws_cdk import (
    Stack,
    aws_dynamodb as dynamodb,
)
from constructs import Construct

class MyHayatiPhase2Stack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Users Table
        users_table = dynamodb.Table(
            self,
            "UsersTable",            # CDK logical ID
            table_name="users",      # Physical table name in DynamoDB
            partition_key=dynamodb.Attribute(
                name="user_id",      # Partition key name
                type=dynamodb.AttributeType.STRING
            ),
            stream=dynamodb.StreamViewType.NEW_IMAGE  # Optional
        )

        # Products Table
        products_table = dynamodb.Table(
            self,
            "ProductsTable",         # CDK logical ID
            table_name="products",   # Physical table name in DynamoDB
            partition_key=dynamodb.Attribute(
                name="product_id",   # Partition key name
                type=dynamodb.AttributeType.STRING
            ),
            stream=dynamodb.StreamViewType.NEW_IMAGE  # Optional
        )
