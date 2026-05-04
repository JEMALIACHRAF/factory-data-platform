"""
"RedshiftStack" – RDS PostgreSQL (remplace Redshift pour compatibilité compte)
Free Tier eligible: db.t3.micro, 20GB storage
SQL quasi-identique à Redshift pour le portfolio
SSL désactivé via Parameter Group pour connexion Power BI
"""
from aws_cdk import (
    Stack, RemovalPolicy, Duration, CfnOutput,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_iam as iam,
)
from constructs import Construct


class RedshiftStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.Vpc,
        processed_bucket: s3.Bucket,
        env_name: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # ── Parameter Group – SSL désactivé ───────────────────────────────────
        no_ssl_param_group = rds.ParameterGroup(
            self, "NoSslParamGroup",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_16_6
            ),
            description="Disable SSL for dev/portfolio",
            parameters={
                "rds.force_ssl": "0"
            },
        )

        # ── Security Group ────────────────────────────────────────────────────
        self.sg = ec2.SecurityGroup(
            self, "PostgresSG",
            vpc=vpc,
            security_group_name=f"postgres-sg-{env_name}",
            description="RDS PostgreSQL security group",
            allow_all_outbound=True,
        )
        self.sg.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(5432),
            description="PostgreSQL from internet (dev only)",
        )

        # ── Credentials ───────────────────────────────────────────────────────
        self.admin_secret = rds.DatabaseSecret(
            self, "PostgresSecret",
            username="factoryadmin",
            secret_name=f"factory/postgres/admin-{env_name}",
        )

        # ── RDS PostgreSQL (Free Tier) ────────────────────────────────────────
        self.db = rds.DatabaseInstance(
            self, "PostgresDB",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_16_6
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.MICRO,
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),
            security_groups=[self.sg],
            database_name="factory_dw",
            credentials=rds.Credentials.from_secret(self.admin_secret),
            publicly_accessible=True,
            allocated_storage=20,
            max_allocated_storage=20,
            deletion_protection=False,
            removal_policy=RemovalPolicy.DESTROY,
            backup_retention=Duration.days(1),
            multi_az=False,
            parameter_group=no_ssl_param_group,  # ← SSL désactivé
        )

        # ── IAM Role ──────────────────────────────────────────────────────────
        self.redshift_role = iam.Role(
            self, "DBRole",
            role_name=f"factory-redshift-role-{env_name}",
            assumed_by=iam.ServicePrincipal("rds.amazonaws.com"),
        )
        processed_bucket.grant_read(self.redshift_role)

        # ── Outputs ───────────────────────────────────────────────────────────
        CfnOutput(self, "DBEndpoint",
            value=self.db.db_instance_endpoint_address,
            export_name=f"RedshiftEndpoint-{env_name}",
        )
        CfnOutput(self, "DBPort",
            value=self.db.db_instance_endpoint_port,
            export_name=f"RedshiftPort-{env_name}",
        )
        CfnOutput(self, "DBSecretArn",
            value=self.admin_secret.secret_arn,
            export_name=f"RedshiftSecretArn-{env_name}",
        )
        CfnOutput(self, "RedshiftRoleArn",
            value=self.redshift_role.role_arn,
            export_name=f"RedshiftRoleArn-{env_name}",
        )