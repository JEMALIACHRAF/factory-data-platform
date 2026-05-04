from aws_cdk import (
    Stack, Duration,
    aws_ec2 as ec2,
    aws_msk as msk,
    aws_iam as iam,
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

        # Security Group for MSK
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

        private_subnets = vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
        ).subnet_ids

        # MSK Cluster
        self.cluster = msk.CfnCluster(
            self, "MskCluster",
            cluster_name=f"factory-kafka-{env_name}",
            kafka_version="3.5.1",
            number_of_broker_nodes=2,
            broker_node_group_info=msk.CfnCluster.BrokerNodeGroupInfoProperty(
                instance_type="kafka.m5.large",
                client_subnets=private_subnets[:2],
                security_groups=[self.msk_sg.security_group_id],
                storage_info=msk.CfnCluster.StorageInfoProperty(
                    ebs_storage_info=msk.CfnCluster.EBSStorageInfoProperty(
                        volume_size=100,
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
                        log_group=f"/aws/msk/factory-{env_name}",
                    )
                )
            ),
        )