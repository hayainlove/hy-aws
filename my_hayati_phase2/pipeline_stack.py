from aws_cdk import (
    Stack,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_codebuild as codebuild,
    aws_iam as iam,
    aws_s3 as s3,
    RemovalPolicy,
    SecretValue,
    CfnOutput,
)
from constructs import Construct


class MyHayatiPipelineStack(Stack):
    """CI/CD Pipeline Stack for MyHayati application"""

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # ---------------------------------------------------------------------
        # S3 Bucket for Pipeline Artifacts
        # ---------------------------------------------------------------------
        artifact_bucket = s3.Bucket(
            self,
            "PipelineArtifactsBucket",
            bucket_name=f"myhayati-pipeline-artifacts-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # ---------------------------------------------------------------------
        # IAM Role for CodeBuild
        # ---------------------------------------------------------------------
        codebuild_role = iam.Role(
            self,
            "CodeBuildRole",
            assumed_by=iam.ServicePrincipal("codebuild.amazonaws.com"),
            description="Role for CodeBuild to deploy CDK stacks",
        )

        # Grant permissions for CDK deployment
        codebuild_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess")
        )
        # NOTE: In production, use more restrictive permissions instead of AdministratorAccess

        # ---------------------------------------------------------------------
        # CodeBuild Project
        # ---------------------------------------------------------------------
        build_project = codebuild.Project(
            self,
            "MyHayatiBuildProject",
            project_name="MyHayati-CDK-Build",
            description="Builds and deploys MyHayati CDK application",
            role=codebuild_role,
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                compute_type=codebuild.ComputeType.SMALL,
                privileged=False,
            ),
            build_spec=codebuild.BuildSpec.from_source_filename("buildspec.yml"),
            timeout=Duration.minutes(60),
            cache=codebuild.Cache.local(codebuild.LocalCacheMode.SOURCE),
        )

        # ---------------------------------------------------------------------
        # Pipeline Artifacts
        # ---------------------------------------------------------------------
        source_output = codepipeline.Artifact("SourceOutput")
        build_output = codepipeline.Artifact("BuildOutput")

        # ---------------------------------------------------------------------
        # CodePipeline
        # ---------------------------------------------------------------------
        pipeline = codepipeline.Pipeline(
            self,
            "MyHayatiPipeline",
            pipeline_name="MyHayati-CDK-Pipeline",
            artifact_bucket=artifact_bucket,
            restart_execution_on_update=True,
        )

        # ---------------------------------------------------------------------
        # Source Stage - GitHub
        # ---------------------------------------------------------------------
        # IMPORTANT: You need to create a GitHub personal access token and store it in AWS Secrets Manager
        # 1. Go to GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
        # 2. Generate new token with 'repo' and 'admin:repo_hook' permissions
        # 3. Store it in AWS Secrets Manager with name 'github-token'
        
        source_stage = pipeline.add_stage(stage_name="Source")
        
        # Option 1: Using GitHub (you'll need to configure OAuth or token)
        source_action = codepipeline_actions.GitHubSourceAction(
            action_name="GitHub_Source",
            owner="hayainlove",              # Your GitHub username
            repo="hy-aws",                    # Your repo name
            branch="main",                    # Or "master" depending on your default branch
            oauth_token=SecretValue.secrets_manager("github-token"),  # GitHub token from Secrets Manager
            output=source_output,
            trigger=codepipeline_actions.GitHubTrigger.WEBHOOK,
        )
        source_stage.add_action(source_action)

        # ---------------------------------------------------------------------
        # Build Stage - CodeBuild
        # ---------------------------------------------------------------------
        build_stage = pipeline.add_stage(stage_name="Build")
        
        build_action = codepipeline_actions.CodeBuildAction(
            action_name="CDK_Build_Deploy",
            project=build_project,
            input=source_output,
            outputs=[build_output],
        )
        build_stage.add_action(build_action)

        # ---------------------------------------------------------------------
        # CloudFormation Outputs
        # ---------------------------------------------------------------------
        CfnOutput(
            self,
            "PipelineName",
            value=pipeline.pipeline_name,
            description="CodePipeline name"
        )
        
        CfnOutput(
            self,
            "PipelineUrl",
            value=f"https://{self.region}.console.aws.amazon.com/codesuite/codepipeline/pipelines/{pipeline.pipeline_name}/view",
            description="CodePipeline console URL"
        )
        
        CfnOutput(
            self,
            "BuildProjectName",
            value=build_project.project_name,
            description="CodeBuild project name"
        )
