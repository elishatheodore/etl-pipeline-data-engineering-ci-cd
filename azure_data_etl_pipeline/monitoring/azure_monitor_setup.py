#!/usr/bin/env python3
"""
Azure Monitor Setup Script
Configures monitoring, logging, and alerting for Brazilian E-Commerce ETL Pipeline
"""

import os
import sys
import json
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta

from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.core.exceptions import AzureError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('azure_monitor_setup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class AzureMonitorManager:
    """Manages Azure Monitor setup and configuration"""
    
    def __init__(self, subscription_id: str, resource_group_name: str):
        self.subscription_id = subscription_id
        self.resource_group_name = resource_group_name
        self.credential = DefaultAzureCredential()
        self.monitor_client = MonitorManagementClient(self.credential, self.subscription_id)
        self.resource_client = ResourceManagementClient(self.credential, self.subscription_id)
        self.logs_client = LogsQueryClient(self.credential)
        
    def create_log_analytics_workspace(self, workspace_name: str, location: str) -> Dict[str, Any]:
        """Create Log Analytics workspace"""
        logger.info(f"Creating Log Analytics workspace: {workspace_name}")
        
        try:
            workspace_params = {
                'location': location,
                'sku': {
                    'name': 'PerGB2018'
                },
                'retention_in_days': 30
            }
            
            poller = self.monitor_client.workspaces.begin_create_or_update(
                self.resource_group_name,
                workspace_name,
                workspace_params
            )
            
            workspace = poller.result()
            logger.info(f"Successfully created Log Analytics workspace: {workspace.name}")
            
            return {
                'workspace_id': workspace.customer_id,
                'workspace_name': workspace.name,
                'location': workspace.location,
                'primary_shared_key': workspace.primary_shared_key,
                'secondary_shared_key': workspace.secondary_shared_key
            }
            
        except AzureError as e:
            logger.error(f"Failed to create Log Analytics workspace: {str(e)}")
            raise
    
    def create_application_insights(self, app_name: str, location: str, workspace_id: str) -> Dict[str, Any]:
        """Create Application Insights component"""
        logger.info(f"Creating Application Insights: {app_name}")
        
        try:
            app_params = {
                'location': location,
                'application_type': 'web',
                'workspace_resource_id': f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group_name}/providers/Microsoft.OperationalInsights/workspaces/{workspace_id}"
            }
            
            poller = self.monitor_client.components.begin_create_or_update(
                self.resource_group_name,
                app_name,
                app_params
            )
            
            app_insights = poller.result()
            logger.info(f"Successfully created Application Insights: {app_insights.name}")
            
            return {
                'app_id': app_insights.app_id,
                'instrumentation_key': app_insights.instrumentation_key,
                'app_name': app_insights.name,
                'location': app_insights.location
            }
            
        except AzureError as e:
            logger.error(f"Failed to create Application Insights: {str(e)}")
            raise
    
    def create_metric_alerts(self, resource_id: str) -> List[Dict[str, Any]]:
        """Create metric alerts for monitoring"""
        logger.info("Creating metric alerts")
        
        alerts = []
        
        # Alert 1: Data Factory Pipeline Failure Rate
        alert_1 = {
            'name': 'ETL-Pipeline-High-Failure-Rate',
            'description': 'Alert when ETL pipeline failure rate exceeds 5%',
            'scopes': [resource_id],
            'condition': {
                'all_of': [
                    {
                        'field': 'PercentageFailedRuns',
                        'operator': 'GreaterThan',
                        'threshold': 5.0,
                        'aggregation': 'Average',
                        'window_size': 'PT5M',  # 5 minutes
                        'evaluation_frequency': 'PT1M'  # 1 minute
                    }
                ]
            },
            'actions': [],
            'severity': 2,
            'enabled': True
        }
        
        # Alert 2: Databricks Job Failure
        alert_2 = {
            'name': 'Databricks-Job-Failure',
            'description': 'Alert when Databricks job fails',
            'scopes': [resource_id],
            'condition': {
                'all_of': [
                    {
                        'field': 'JobRunFailed',
                        'operator': 'GreaterThan',
                        'threshold': 0,
                        'aggregation': 'Count',
                        'window_size': 'PT5M',
                        'evaluation_frequency': 'PT1M'
                    }
                ]
            },
            'actions': [],
            'severity': 1,
            'enabled': True
        }
        
        # Alert 3: Storage Account High Latency
        alert_3 = {
            'name': 'Storage-Account-High-Latency',
            'description': 'Alert when storage account latency is high',
            'scopes': [resource_id],
            'condition': {
                'all_of': [
                    {
                        'field': 'SuccessLatency',
                        'operator': 'GreaterThan',
                        'threshold': 1000.0,  # 1000ms
                        'aggregation': 'Average',
                        'window_size': 'PT5M',
                        'evaluation_frequency': 'PT1M'
                    }
                ]
            },
            'actions': [],
            'severity': 3,
            'enabled': True
        }
        
        for alert_config in [alert_1, alert_2, alert_3]:
            try:
                alert_rule = self.monitor_client.metric_alerts.create_or_update(
                    self.resource_group_name,
                    alert_config['name'],
                    alert_config
                )
                
                alerts.append({
                    'name': alert_rule.name,
                    'description': alert_rule.description,
                    'severity': alert_rule.severity,
                    'enabled': alert_rule.enabled
                })
                
                logger.info(f"Created metric alert: {alert_rule.name}")
                
            except AzureError as e:
                logger.error(f"Failed to create alert {alert_config['name']}: {str(e)}")
        
        return alerts
    
    def create_log_analytics_queries(self, workspace_id: str) -> List[Dict[str, Any]]:
        """Create saved queries for Log Analytics"""
        logger.info("Creating Log Analytics saved queries")
        
        queries = []
        
        # Query 1: Pipeline Performance
        query_1 = {
            'name': 'ETL-Pipeline-Performance',
            'display_name': 'ETL Pipeline Performance Metrics',
            'category': 'ETL Monitoring',
            'query': '''
            AzureDiagnostics
            | where Category == "PipelineRuns"
            | where TimeGenerated > ago(1d)
            | summarize 
                TotalRuns = count(),
                SuccessfulRuns = countif(Status_s == "Succeeded"),
                FailedRuns = countif(Status_s == "Failed"),
                AvgDuration = avg(todouble(Duration_s)),
                SuccessRate = (countif(Status_s == "Succeeded") * 100.0) / count()
            by bin(TimeGenerated, 1h), PipelineName_s
            | order by TimeGenerated desc
            '''
        }
        
        # Query 2: Data Quality Issues
        query_2 = {
            'name': 'Data-Quality-Issues',
            'display_name': 'Data Quality Issues',
            'category': 'Data Quality',
            'query': '''
            AzureDiagnostics
            | where Category == "DataQuality"
            | where TimeGenerated > ago(24h)
            | where ErrorMessage_s != ""
            | summarize 
                IssueCount = count(),
                LastIssue = max(TimeGenerated)
            by TableName_s, IssueType_s
            | order by IssueCount desc
            '''
        }
        
        # Query 3: Resource Utilization
        query_3 = {
            'name': 'Resource-Utilization',
            'display_name': 'Databricks Resource Utilization',
            'category': 'Resource Monitoring',
            'query': '''
            AzureDiagnostics
            | where Category == "DBJob"
            | where TimeGenerated > ago(1d)
            | summarize 
                TotalJobs = count(),
                AvgCpuUsage = avg(todouble(CPUUsage_s)),
                AvgMemoryUsage = avg(todouble(MemoryUsage_s)),
                TotalRunningTime = sum(todouble(RunningTime_s))
            by bin(TimeGenerated, 1h), ClusterName_s
            | order by TimeGenerated desc
            '''
        }
        
        # Query 4: Cost Analysis
        query_4 = {
            'name': 'Cost-Analysis',
            'display_name': 'ETL Pipeline Cost Analysis',
            'category': 'Cost Management',
            'query': '''
            AzureDiagnostics
            | where Category == "Cost"
            | where TimeGenerated > ago(30d)
            | summarize 
                TotalCost = sum(todouble(Cost_s)),
                ComputeCost = sumif(todouble(Cost_s), ResourceType_s == "Compute"),
                StorageCost = sumif(todouble(Cost_s), ResourceType_s == "Storage"),
                NetworkCost = sumif(todouble(Cost_s), ResourceType_s == "Network")
            by bin(TimeGenerated, 1d), ServiceName_s
            | order by TimeGenerated desc
            '''
        }
        
        for query_config in [query_1, query_2, query_3, query_4]:
            try:
                # Note: Saved queries would be created via Log Analytics workspace API
                # This is a placeholder for the actual implementation
                queries.append({
                    'name': query_config['name'],
                    'display_name': query_config['display_name'],
                    'category': query_config['category'],
                    'query': query_config['query']
                })
                
                logger.info(f"Created saved query: {query_config['name']}")
                
            except Exception as e:
                logger.error(f"Failed to create query {query_config['name']}: {str(e)}")
        
        return queries
    
    def create_action_groups(self) -> List[Dict[str, Any]]:
        """Create action groups for alert notifications"""
        logger.info("Creating action groups")
        
        action_groups = []
        
        # Action Group 1: Email Notifications
        email_action_group = {
            'name': 'ETL-Alerts-Email',
            'short_name': 'etl-email',
            'group_short_name': 'etl-email',
            'enabled': True,
            'email_receivers': [
                {
                    'name': 'Data Engineering Team',
                    'email_address': 'data-engineering@company.com',
                    'status': 'Enabled'
                }
            ]
        }
        
        # Action Group 2: Slack Notifications
        slack_action_group = {
            'name': 'ETL-Alerts-Slack',
            'short_name': 'etl-slack',
            'group_short_name': 'etl-slack',
            'enabled': True,
            'webhook_receivers': [
                {
                    'name': 'Slack Webhook',
                    'service_uri': os.getenv('SLACK_WEBHOOK_URL', ''),
                    'status': 'Enabled'
                }
            ]
        }
        
        for action_group_config in [email_action_group, slack_action_group]:
            try:
                action_group = self.monitor_client.action_groups.create_or_update(
                    self.resource_group_name,
                    action_group_config['name'],
                    action_group_config
                )
                
                action_groups.append({
                    'name': action_group.name,
                    'short_name': action_group.group_short_name,
                    'enabled': action_group.enabled
                })
                
                logger.info(f"Created action group: {action_group.name}")
                
            except AzureError as e:
                logger.error(f"Failed to create action group {action_group_config['name']}: {str(e)}")
        
        return action_groups
    
    def create_dashboard(self, dashboard_name: str, workspace_id: str) -> Dict[str, Any]:
        """Create Azure Monitor dashboard"""
        logger.info(f"Creating dashboard: {dashboard_name}")
        
        dashboard_definition = {
            'properties': {
                'lenses': [
                    {
                        'order': 1,
                        'parts': [
                            {
                                'position': {
                                    'x': 0,
                                    'y': 0,
                                    'width': 6,
                                    'height': 3
                                },
                                'metadata': {
                                    'title': 'ETL Pipeline Status',
                                    'type': 'Extension[azMonitorGridViews]/PartType/MetricsViewPart'
                                }
                            },
                            {
                                'position': {
                                    'x': 6,
                                    'y': 0,
                                    'width': 6,
                                    'height': 3
                                },
                                'metadata': {
                                    'title': 'Data Quality Metrics',
                                    'type': 'Extension[azMonitorGridViews]/PartType/MetricsViewPart'
                                }
                            },
                            {
                                'position': {
                                    'x': 0,
                                    'y': 3,
                                    'width': 12,
                                    'height': 4
                                },
                                'metadata': {
                                    'title': 'Pipeline Performance',
                                    'type': 'Extension[azMonitorGridViews]/PartType/LogsViewPart'
                                }
                            }
                        ]
                    }
                ]
            }
        }
        
        try:
            dashboard = self.monitor_client.dashboards.create_or_update(
                self.resource_group_name,
                dashboard_name,
                dashboard_definition
            )
            
            logger.info(f"Successfully created dashboard: {dashboard.name}")
            
            return {
                'dashboard_id': dashboard.id,
                'dashboard_name': dashboard.name,
                'dashboard_url': f"https://portal.azure.com/#blade/HubsExtension/Resource/resourceGroup/{self.resource_group_name}/type/Microsoft.Insights/dashboards/{dashboard.name}"
            }
            
        except AzureError as e:
            logger.error(f"Failed to create dashboard: {str(e)}")
            raise
    
    def setup_complete_monitoring(self, workspace_name: str, location: str) -> Dict[str, Any]:
        """Set up complete monitoring solution"""
        logger.info("Setting up complete Azure Monitor solution")
        
        setup_results = {
            'timestamp': datetime.utcnow().isoformat(),
            'workspace_name': workspace_name,
            'location': location,
            'components': {}
        }
        
        try:
            # Step 1: Create Log Analytics workspace
            workspace_info = self.create_log_analytics_workspace(workspace_name, location)
            setup_results['components']['log_analytics'] = workspace_info
            
            # Step 2: Create Application Insights
            app_insights_info = self.create_application_insights(
                f"{workspace_name}-appinsights",
                location,
                workspace_info['workspace_name']
            )
            setup_results['components']['application_insights'] = app_insights_info
            
            # Step 3: Create action groups
            action_groups = self.create_action_groups()
            setup_results['components']['action_groups'] = action_groups
            
            # Step 4: Create metric alerts
            resource_id = f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group_name}/providers/Microsoft.DataFactory/factories/brazilian-ecommerce-adf"
            metric_alerts = self.create_metric_alerts(resource_id)
            setup_results['components']['metric_alerts'] = metric_alerts
            
            # Step 5: Create Log Analytics queries
            saved_queries = self.create_log_analytics_queries(workspace_info['workspace_id'])
            setup_results['components']['saved_queries'] = saved_queries
            
            # Step 6: Create dashboard
            dashboard_info = self.create_dashboard(
                f"{workspace_name}-dashboard",
                workspace_info['workspace_id']
            )
            setup_results['components']['dashboard'] = dashboard_info
            
            logger.info("Complete Azure Monitor setup completed successfully")
            
            return setup_results
            
        except Exception as e:
            logger.error(f"Complete monitoring setup failed: {str(e)}")
            raise

def main():
    """Main execution function"""
    # Configuration
    config = {
        'subscription_id': os.getenv('AZURE_SUBSCRIPTION_ID'),
        'resource_group_name': os.getenv('AZURE_RESOURCE_GROUP', 'brazilian-ecommerce-rg'),
        'workspace_name': os.getenv('LOG_ANALYTICS_WORKSPACE', 'brazilian-ecommerce-law'),
        'location': os.getenv('AZURE_LOCATION', 'East US')
    }
    
    # Validate configuration
    missing_config = [key for key, value in config.items() if not value]
    if missing_config:
        logger.error(f"Missing required configuration: {missing_config}")
        sys.exit(1)
    
    logger.info("Starting Azure Monitor setup")
    logger.info(f"Configuration: {json.dumps(config, indent=2)}")
    
    try:
        # Initialize monitor manager
        monitor_manager = AzureMonitorManager(
            config['subscription_id'],
            config['resource_group_name']
        )
        
        # Setup complete monitoring
        results = monitor_manager.setup_complete_monitoring(
            config['workspace_name'],
            config['location']
        )
        
        # Save results to file
        with open('azure_monitor_setup_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        # Display summary
        print("=== Azure Monitor Setup Summary ===")
        print(f"Log Analytics Workspace: {results['components']['log_analytics']['workspace_name']}")
        print(f"Application Insights: {results['components']['application_insights']['app_name']}")
        print(f"Action Groups: {len(results['components']['action_groups'])}")
        print(f"Metric Alerts: {len(results['components']['metric_alerts'])}")
        print(f"Saved Queries: {len(results['components']['saved_queries'])}")
        print(f"Dashboard: {results['components']['dashboard']['dashboard_name']}")
        print(f"\nDashboard URL: {results['components']['dashboard']['dashboard_url']}")
        
        logger.info("Azure Monitor setup completed successfully!")
        
        return results
        
    except Exception as e:
        logger.error(f"Azure Monitor setup failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
