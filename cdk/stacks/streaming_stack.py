"""
StreamingStack – MSK Kafka Cluster
Production: 2 brokers, TLS_PLAINTEXT, CloudWatch logs
Fix: kafka.t3.small for faster provisioning, explicit AZ subnets
"""
from aws_cdk import (
    Stack,
    CfnOutput,
    aws_ec2 as ec2,
    aws_msk as msk,
    aws_logs as logs,
    RemovalPolicy,
)
from constructs import Construct


class StreamingStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.Vpc,
        env_name: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # ── CloudWatch Log Group ──────────────────────────────────────────────
        log_group = logs.LogGroup(
            self, "MskLogGroup",
            log_group_name=f"/aws/msk/factory-{env_name}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ── Security Group ────────────────────────────────────────────────────
        self.msk_sg = ec2.SecurityGroup(
            self, "MskSG",
            vpc=vpc,
            security_group_name=f"msk-sg-{env_name}",
            description="MSK Kafka security group",
            allow_all_outbound=True,
        )
        self.msk_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(9092),
            description="Kafka plaintext",
        )
        self.msk_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(9094),
            description="Kafka TLS",
        )
        self.msk_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(2181),
            description="Zookeeper",
        )

        # ── Subnets — exactly 2, one per AZ ──────────────────────────────────
        # MSK requires one subnet per AZ, matching number_of_broker_nodes
        private_subnets = vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            one_per_az=True,                # garantit 1 subnet par AZ
        ).subnet_ids[:2]                    # exactement 2 pour 2 brokers

        # ── MSK Cluster ───────────────────────────────────────────────────────
        # kafka.t3.small → provisionne plus vite que m5.large
        # Passe à kafka.m5.large en prod réelle
        self.cluster = msk.CfnCluster(
            self, "MskCluster",
            cluster_name=f"factory-kafka-{env_name}",
            kafka_version="3.6.0",          # version stable LTS
            number_of_broker_nodes=2,       # 1 broker par AZ
            broker_node_group_info=msk.CfnCluster.BrokerNodeGroupInfoProperty(
                instance_type="kafka.t3.small",  # plus rapide à provisionner
                client_subnets=private_subnets,
                security_groups=[self.msk_sg.security_group_id],
                storage_info=msk.CfnCluster.StorageInfoProperty(
                    ebs_storage_info=msk.CfnCluster.EBSStorageInfoProperty(
                        volume_size=20,     # 20GB suffit pour les tests
                    )
                ),
            ),
            encryption_info=msk.CfnCluster.EncryptionInfoProperty(
                encryption_in_transit=msk.CfnCluster.EncryptionInTransitProperty(
                    client_broker="TLS_PLAINTEXT",
                    in_cluster=True,
                )
            ),
            enhanced_monitoring="PER_TOPIC_PER_BROKER",
            logging_info=msk.CfnCluster.LoggingInfoProperty(
                broker_logs=msk.CfnCluster.BrokerLogsProperty(
                    cloud_watch_logs=msk.CfnCluster.CloudWatchLogsProperty(
                        enabled=True,
                        log_group=log_group.log_group_name,
                    )
                )
            ),
            # Topics auto-create pour les tests
            configuration_info=msk.CfnCluster.ConfigurationInfoProperty(
                arn=f"arn:aws:kafka:{self.region}:{self.account}:configuration/factory-config/1-1",
                revision=1,
            ) if False else None,   # désactivé — utilise config par défaut
        )

        # ── Outputs ───────────────────────────────────────────────────────────
        CfnOutput(self, "MskClusterArn",
            value=self.cluster.ref,
            export_name=f"MskClusterArn-{env_name}",
            description="MSK Cluster ARN",
        )