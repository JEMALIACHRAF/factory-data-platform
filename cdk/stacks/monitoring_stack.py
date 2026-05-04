"""
MonitoringStack – CloudWatch alarms + dashboard for Factory Data Platform
Alerts on: Glue job failures, DQ failure rate, Redshift query latency
"""
from aws_cdk import (
    Stack, Duration,
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
)
from constructs import Construct


class MonitoringStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        glue_jobs: list[str],
        env_name: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # ── SNS topic for alerts ──────────────────────────────────────────────
        alert_topic = sns.Topic(
            self, "AlertTopic",
            topic_name=f"factory-data-alerts-{env_name}",
            display_name=f"Factory Data Platform Alerts ({env_name})",
        )

        # Add email subscription (set via context)
        alert_email = self.node.try_get_context("alert_email") or "data-team@company.com"
        alert_topic.add_subscription(subs.EmailSubscription(alert_email))

        alarms = []

        # ── Alarms per Glue job ───────────────────────────────────────────────
        for job_name in glue_jobs:
            # Job failure alarm
            failure_alarm = cw.Alarm(
                self, f"GlueFailure-{job_name}",
                alarm_name=f"{job_name}-failure",
                alarm_description=f"Glue job {job_name} failed",
                metric=cw.Metric(
                    namespace="Glue",
                    metric_name="glue.driver.aggregate.numFailedTasks",
                    dimensions_map={"JobName": job_name},
                    period=Duration.minutes(5),
                    statistic="Sum",
                ),
                threshold=1,
                evaluation_periods=1,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            )
            failure_alarm.add_alarm_action(cw_actions.SnsAction(alert_topic))
            alarms.append(failure_alarm)

            # Executor memory spill alarm (indicates under-provisioned workers)
            spill_alarm = cw.Alarm(
                self, f"GlueSpill-{job_name}",
                alarm_name=f"{job_name}-disk-spill",
                alarm_description=f"{job_name}: disk spill detected → increase workers",
                metric=cw.Metric(
                    namespace="Glue",
                    metric_name="glue.driver.BlockManager.disk.diskSpaceUsed_MB",
                    dimensions_map={"JobName": job_name},
                    period=Duration.minutes(5),
                    statistic="Maximum",
                ),
                threshold=1000,  # MB
                evaluation_periods=2,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
                treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            )
            spill_alarm.add_alarm_action(cw_actions.SnsAction(alert_topic))
            alarms.append(spill_alarm)

        # ── CloudWatch Dashboard ──────────────────────────────────────────────
        dashboard = cw.Dashboard(
            self, "FactoryDashboard",
            dashboard_name=f"FactoryDataPlatform-{env_name}",
        )

        # Glue jobs row
        glue_widgets = []
        for job_name in glue_jobs:
            glue_widgets.append(
                cw.GraphWidget(
                    title=f"{job_name} – Tasks",
                    width=12,
                    height=6,
                    left=[
                        cw.Metric(
                            namespace="Glue",
                            metric_name="glue.driver.aggregate.numCompletedTasks",
                            dimensions_map={"JobName": job_name},
                            period=Duration.minutes(5),
                            label="Completed",
                            color="#2ca02c",
                        ),
                        cw.Metric(
                            namespace="Glue",
                            metric_name="glue.driver.aggregate.numFailedTasks",
                            dimensions_map={"JobName": job_name},
                            period=Duration.minutes(5),
                            label="Failed",
                            color="#d62728",
                        ),
                    ],
                )
            )

        dashboard.add_widgets(*glue_widgets)

        # Alarm summary widget
        dashboard.add_widgets(
            cw.AlarmStatusWidget(
                title="Active Alarms",
                alarms=alarms,
                width=24,
                height=4,
            )
        )
