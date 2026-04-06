#!/usr/bin/env python3
"""
Cloud-Agnostic Monitoring Setup
Configures Prometheus and Grafana for multi-cloud ETL pipeline monitoring
"""

import os
import sys
import json
import logging
import yaml
import requests
from typing import Dict, Any, List
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('prometheus_grafana_setup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class PrometheusGrafanaManager:
    """Manages Prometheus and Grafana setup for cloud-agnostic monitoring"""
    
    def __init__(self, prometheus_url: str, grafana_url: str, grafana_token: str = None):
        self.prometheus_url = prometheus_url
        self.grafana_url = grafana_url
        self.grafana_token = grafana_token
        self.grafana_headers = {
            'Authorization': f'Bearer {grafana_token}',
            'Content-Type': 'application/json'
        } if grafana_token else {}
        
    def create_prometheus_config(self, cloud_provider: str, targets: List[str]) -> Dict[str, Any]:
        """Create Prometheus configuration for cloud provider"""
        logger.info(f"Creating Prometheus configuration for {cloud_provider}")
        
        config = {
            'global': {
                'scrape_interval': '15s',
                'evaluation_interval': '15s'
            },
            'rule_files': [
                'etl_pipeline_rules.yml',
                'data_quality_rules.yml',
                'cost_monitoring_rules.yml'
            ],
            'scrape_configs': [
                # Prometheus itself
                {
                    'job_name': 'prometheus',
                    'static_configs': [
                        {'targets': ['localhost:9090']}
                    ]
                },
                # Grafana
                {
                    'job_name': 'grafana',
                    'static_configs': [
                        {'targets': ['localhost:3000']}
                    ],
                    'metrics_path': '/metrics'
                }
            ]
        }
        
        # Cloud-specific configurations
        if cloud_provider.lower() == 'azure':
            config['scrape_configs'].extend([
                {
                    'job_name': 'azure-monitor',
                    'azure_sd_configs': [{
                        'subscription_id': os.getenv('AZURE_SUBSCRIPTION_ID'),
                        'tenant_id': os.getenv('AZURE_TENANT_ID'),
                        'client_id': os.getenv('AZURE_CLIENT_ID'),
                        'client_secret': os.getenv('AZURE_CLIENT_SECRET'),
                        'resource_type': 'microsoft.datafactory/factories',
                        'port': 9090
                    }]
                },
                {
                    'job_name': 'databricks',
                    'static_configs': [
                        {'targets': targets}
                    ],
                    'metrics_path': '/metrics'
                }
            ])
        elif cloud_provider.lower() == 'aws':
            config['scrape_configs'].extend([
                {
                    'job_name': 'cloudwatch',
                    'ec2_sd_configs': [{
                        'region': os.getenv('AWS_REGION', 'us-east-1'),
                        'port': 9106,
                        'filters': [
                            {'name': 'tag:Environment', 'values': ['production']}
                        ]
                    }]
                },
                {
                    'job_name': 'databricks-aws',
                    'static_configs': [
                        {'targets': targets}
                    ]
                }
            ])
        elif cloud_provider.lower() == 'gcp':
            config['scrape_configs'].extend([
                {
                    'job_name': 'gcp-monitoring',
                    'gcp_sd_configs': [{
                        'project': os.getenv('GCP_PROJECT_ID'),
                        'zone': 'us-central1-a',
                        'port': 9100
                    }]
                },
                {
                    'job_name': 'databricks-gcp',
                    'static_configs': [
                        {'targets': targets}
                    ]
                }
            ])
        
        return config
    
    def create_alerting_rules(self) -> List[Dict[str, Any]]:
        """Create Prometheus alerting rules"""
        logger.info("Creating Prometheus alerting rules")
        
        rules = [
            # ETL Pipeline Rules
            {
                'name': 'etl_pipeline_rules.yml',
                'groups': [
                    {
                        'name': 'etl_pipeline',
                        'rules': [
                            {
                                'alert': 'ETLPipelineHighFailureRate',
                                'expr': 'rate(etl_pipeline_failures_total[5m]) > 0.05',
                                'for': '2m',
                                'labels': {
                                    'severity': 'warning',
                                    'service': 'etl-pipeline'
                                },
                                'annotations': {
                                    'summary': 'ETL pipeline failure rate is high',
                                    'description': 'ETL pipeline failure rate is {{ $value | humanizePercentage }} over the last 5 minutes'
                                }
                            },
                            {
                                'alert': 'ETLPipelineSlowExecution',
                                'expr': 'etl_pipeline_duration_seconds > 3600',
                                'for': '5m',
                                'labels': {
                                    'severity': 'warning',
                                    'service': 'etl-pipeline'
                                },
                                'annotations': {
                                    'summary': 'ETL pipeline execution is slow',
                                    'description': 'ETL pipeline has been running for {{ $value }} seconds'
                                }
                            },
                            {
                                'alert': 'ETLPipelineNoData',
                                'expr': 'up{job="etl_pipeline"} == 0',
                                'for': '1m',
                                'labels': {
                                    'severity': 'critical',
                                    'service': 'etl-pipeline'
                                },
                                'annotations': {
                                    'summary': 'ETL pipeline is down',
                                    'description': 'ETL pipeline has been down for more than 1 minute'
                                }
                            }
                        ]
                    }
                ]
            },
            # Data Quality Rules
            {
                'name': 'data_quality_rules.yml',
                'groups': [
                    {
                        'name': 'data_quality',
                        'rules': [
                            {
                                'alert': 'DataQualityHighNullRate',
                                'expr': 'data_quality_null_percentage > 0.1',
                                'for': '5m',
                                'labels': {
                                    'severity': 'warning',
                                    'service': 'data-quality'
                                },
                                'annotations': {
                                    'summary': 'High null value rate detected',
                                    'description': 'Table {{ $labels.table }} has {{ $value | humanizePercentage }} null values'
                                }
                            },
                            {
                                'alert': 'DataQualityDuplicateRecords',
                                'expr': 'data_quality_duplicate_count > 0',
                                'for': '1m',
                                'labels': {
                                    'severity': 'warning',
                                    'service': 'data-quality'
                                },
                                'annotations': {
                                    'summary': 'Duplicate records detected',
                                    'description': 'Table {{ $labels.table }} has {{ $value }} duplicate records'
                                }
                            }
                        ]
                    }
                ]
            },
            # Cost Monitoring Rules
            {
                'name': 'cost_monitoring_rules.yml',
                'groups': [
                    {
                        'name': 'cost_monitoring',
                        'rules': [
                            {
                                'alert': 'HighComputeCost',
                                'expr': 'increase(compute_cost_dollars[24h]) > 100',
                                'for': '1h',
                                'labels': {
                                    'severity': 'warning',
                                    'service': 'cost-monitoring'
                                },
                                'annotations': {
                                    'summary': 'High compute cost detected',
                                    'description': 'Compute cost in the last 24h is ${{ $value }}'
                                }
                            },
                            {
                                'alert': 'HighStorageCost',
                                'expr': 'increase(storage_cost_dollars[24h]) > 50',
                                'for': '1h',
                                'labels': {
                                    'severity': 'warning',
                                    'service': 'cost-monitoring'
                                },
                                'annotations': {
                                    'summary': 'High storage cost detected',
                                    'description': 'Storage cost in the last 24h is ${{ $value }}'
                                }
                            }
                        ]
                    }
                ]
            }
        ]
        
        return rules
    
    def create_grafana_datasources(self) -> List[Dict[str, Any]]:
        """Create Grafana data sources"""
        logger.info("Creating Grafana data sources")
        
        datasources = [
            {
                'name': 'Prometheus',
                'type': 'prometheus',
                'url': self.prometheus_url,
                'access': 'proxy',
                'isDefault': True,
                'jsonData': {
                    'timeInterval': '5s',
                    'queryTimeout': '60s',
                    'httpMethod': 'POST'
                }
            },
            {
                'name': 'Azure Monitor',
                'type': 'grafana-azure-monitor-datasource',
                'url': 'https://management.azure.com/',
                'access': 'proxy',
                'jsonData': {
                    'subscriptionId': os.getenv('AZURE_SUBSCRIPTION_ID'),
                    'tenantId': os.getenv('AZURE_TENANT_ID'),
                    'clientId': os.getenv('AZURE_CLIENT_ID'),
                    'clientSecret': os.getenv('AZURE_CLIENT_SECRET'),
                    'cloudName': 'azurecloud'
                }
            },
            {
                'name': 'CloudWatch',
                'type': 'cloudwatch',
                'url': 'https://monitoring.us-east-1.amazonaws.com',
                'access': 'proxy',
                'jsonData': {
                    'authType': 'credentials',
                    'defaultRegion': os.getenv('AWS_REGION', 'us-east-1'),
                    'assumeRoleArn': os.getenv('AWS_ASSUME_ROLE_ARN', '')
                }
            }
        ]
        
        return datasources
    
    def create_grafana_dashboards(self) -> List[Dict[str, Any]]:
        """Create Grafana dashboards"""
        logger.info("Creating Grafana dashboards")
        
        dashboards = [
            # ETL Pipeline Dashboard
            {
                'dashboard': {
                    'id': None,
                    'title': 'ETL Pipeline Overview',
                    'tags': ['etl', 'brazilian-ecommerce'],
                    'timezone': 'browser',
                    'panels': [
                        {
                            'id': 1,
                            'title': 'Pipeline Status',
                            'type': 'stat',
                            'targets': [
                                {
                                    'expr': 'up{job="etl_pipeline"}',
                                    'legendFormat': '{{ job }}'
                                }
                            ],
                            'fieldConfig': {
                                'defaults': {
                                    'mappings': [
                                        {'options': {'0': {'text': 'DOWN', 'color': 'red'}}, 'type': 'value'},
                                        {'options': {'1': {'text': 'UP', 'color': 'green'}}, 'type': 'value'}
                                    ]
                                }
                            },
                            'gridPos': {'h': 8, 'w': 6, 'x': 0, 'y': 0}
                        },
                        {
                            'id': 2,
                            'title': 'Pipeline Duration',
                            'type': 'graph',
                            'targets': [
                                {
                                    'expr': 'etl_pipeline_duration_seconds',
                                    'legendFormat': '{{ pipeline_name }}'
                                }
                            ],
                            'gridPos': {'h': 8, 'w': 18, 'x': 6, 'y': 0}
                        },
                        {
                            'id': 3,
                            'title': 'Failure Rate',
                            'type': 'graph',
                            'targets': [
                                {
                                    'expr': 'rate(etl_pipeline_failures_total[5m])',
                                    'legendFormat': 'Failure Rate'
                                }
                            ],
                            'gridPos': {'h': 8, 'w': 12, 'x': 0, 'y': 8}
                        },
                        {
                            'id': 4,
                            'title': 'Records Processed',
                            'type': 'graph',
                            'targets': [
                                {
                                    'expr': 'etl_pipeline_records_processed_total',
                                    'legendFormat': '{{ table_name }}'
                                }
                            ],
                            'gridPos': {'h': 8, 'w': 12, 'x': 12, 'y': 8}
                        }
                    ],
                    'time': {'from': 'now-1h', 'to': 'now'},
                    'refresh': '5s'
                }
            },
            # Data Quality Dashboard
            {
                'dashboard': {
                    'id': None,
                    'title': 'Data Quality Metrics',
                    'tags': ['data-quality', 'brazilian-ecommerce'],
                    'timezone': 'browser',
                    'panels': [
                        {
                            'id': 1,
                            'title': 'Null Percentage by Table',
                            'type': 'graph',
                            'targets': [
                                {
                                    'expr': 'data_quality_null_percentage',
                                    'legendFormat': '{{ table_name }}'
                                }
                            ],
                            'gridPos': {'h': 8, 'w': 12, 'x': 0, 'y': 0}
                        },
                        {
                            'id': 2,
                            'title': 'Duplicate Records',
                            'type': 'graph',
                            'targets': [
                                {
                                    'expr': 'data_quality_duplicate_count',
                                    'legendFormat': '{{ table_name }}'
                                }
                            ],
                            'gridPos': {'h': 8, 'w': 12, 'x': 12, 'y': 0}
                        },
                        {
                            'id': 3,
                            'title': 'Data Freshness',
                            'type': 'graph',
                            'targets': [
                                {
                                    'expr': 'time() - data_quality_last_update_timestamp',
                                    'legendFormat': '{{ table_name }}'
                                }
                            ],
                            'gridPos': {'h': 8, 'w': 24, 'x': 0, 'y': 8}
                        }
                    ],
                    'time': {'from': 'now-24h', 'to': 'now'},
                    'refresh': '1m'
                }
            },
            # Cost Monitoring Dashboard
            {
                'dashboard': {
                    'id': None,
                    'title': 'Cost Monitoring',
                    'tags': ['cost', 'brazilian-ecommerce'],
                    'timezone': 'browser',
                    'panels': [
                        {
                            'id': 1,
                            'title': 'Daily Compute Cost',
                            'type': 'graph',
                            'targets': [
                                {
                                    'expr': 'increase(compute_cost_dollars[24h])',
                                    'legendFormat': 'Compute Cost'
                                }
                            ],
                            'gridPos': {'h': 8, 'w': 12, 'x': 0, 'y': 0}
                        },
                        {
                            'id': 2,
                            'title': 'Daily Storage Cost',
                            'type': 'graph',
                            'targets': [
                                {
                                    'expr': 'increase(storage_cost_dollars[24h])',
                                    'legendFormat': 'Storage Cost'
                                }
                            ],
                            'gridPos': {'h': 8, 'w': 12, 'x': 12, 'y': 0}
                        },
                        {
                            'id': 3,
                            'title': 'Cost by Service',
                            'type': 'piechart',
                            'targets': [
                                {
                                    'expr': 'increase(cost_dollars[24h]) by service',
                                    'legendFormat': '{{ service }}'
                                }
                            ],
                            'gridPos': {'h': 8, 'w': 24, 'x': 0, 'y': 8}
                        }
                    ],
                    'time': {'from': 'now-7d', 'to': 'now'},
                    'refresh': '1h'
                }
            }
        ]
        
        return dashboards
    
    def setup_prometheus(self, cloud_provider: str, config_path: str = 'prometheus.yml') -> bool:
        """Set up Prometheus configuration"""
        try:
            # Create configuration
            config = self.create_prometheus_config(cloud_provider, ['databricks-cluster:9090'])
            
            # Save configuration
            with open(config_path, 'w') as f:
                yaml.dump(config, f, default_flow_style=False)
            
            # Create alerting rules
            rules = self.create_alerting_rules()
            for rule in rules:
                rule_path = f"prometheus_rules/{rule['name']}"
                os.makedirs(os.path.dirname(rule_path), exist_ok=True)
                with open(rule_path, 'w') as f:
                    yaml.dump(rule, f, default_flow_style=False)
            
            logger.info("Prometheus configuration created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup Prometheus: {str(e)}")
            return False
    
    def setup_grafana(self) -> Dict[str, Any]:
        """Set up Grafana data sources and dashboards"""
        results = {
            'datasources': [],
            'dashboards': [],
            'errors': []
        }
        
        try:
            # Create data sources
            datasources = self.create_grafana_datasources()
            for ds in datasources:
                try:
                    response = requests.post(
                        f"{self.grafana_url}/api/datasources",
                        headers=self.grafana_headers,
                        json=ds
                    )
                    if response.status_code == 200:
                        results['datasources'].append(ds['name'])
                        logger.info(f"Created Grafana datasource: {ds['name']}")
                    else:
                        error_msg = f"Failed to create datasource {ds['name']}: {response.text}"
                        results['errors'].append(error_msg)
                        logger.error(error_msg)
                except Exception as e:
                    error_msg = f"Error creating datasource {ds['name']}: {str(e)}"
                    results['errors'].append(error_msg)
                    logger.error(error_msg)
            
            # Create dashboards
            dashboards = self.create_grafana_dashboards()
            for dashboard in dashboards:
                try:
                    dashboard['dashboard']['id'] = None  # Let Grafana assign ID
                    response = requests.post(
                        f"{self.grafana_url}/api/dashboards/db",
                        headers=self.grafana_headers,
                        json=dashboard
                    )
                    if response.status_code == 200:
                        dashboard_info = response.json()
                        results['dashboards'].append({
                            'title': dashboard['dashboard']['title'],
                            'uid': dashboard_info['uid'],
                            'url': f"{self.grafana_url}/d/{dashboard_info['uid']}"
                        })
                        logger.info(f"Created Grafana dashboard: {dashboard['dashboard']['title']}")
                    else:
                        error_msg = f"Failed to create dashboard {dashboard['dashboard']['title']}: {response.text}"
                        results['errors'].append(error_msg)
                        logger.error(error_msg)
                except Exception as e:
                    error_msg = f"Error creating dashboard {dashboard['dashboard']['title']}: {str(e)}"
                    results['errors'].append(error_msg)
                    logger.error(error_msg)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to setup Grafana: {str(e)}")
            return results
    
    def export_metrics_config(self, output_path: str = 'metrics_config.json') -> bool:
        """Export metrics configuration for documentation"""
        try:
            config = {
                'prometheus_url': self.prometheus_url,
                'grafana_url': self.grafana_url,
                'metrics': {
                    'etl_pipeline': [
                        'etl_pipeline_duration_seconds',
                        'etl_pipeline_records_processed_total',
                        'etl_pipeline_failures_total',
                        'etl_pipeline_last_success_timestamp'
                    ],
                    'data_quality': [
                        'data_quality_null_percentage',
                        'data_quality_duplicate_count',
                        'data_quality_last_update_timestamp',
                        'data_quality_row_count'
                    ],
                    'cost': [
                        'compute_cost_dollars',
                        'storage_cost_dollars',
                        'network_cost_dollars'
                    ],
                    'databricks': [
                        'databricks_cluster_cpu_usage_percent',
                        'databricks_cluster_memory_usage_percent',
                        'databricks_job_duration_seconds',
                        'databricks_job_status'
                    ]
                },
                'alert_rules': [
                    'ETLPipelineHighFailureRate',
                    'ETLPipelineSlowExecution',
                    'ETLPipelineNoData',
                    'DataQualityHighNullRate',
                    'DataQualityDuplicateRecords',
                    'HighComputeCost',
                    'HighStorageCost'
                ]
            }
            
            with open(output_path, 'w') as f:
                json.dump(config, f, indent=2)
            
            logger.info(f"Metrics configuration exported to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export metrics config: {str(e)}")
            return False

def main():
    """Main execution function"""
    # Configuration
    config = {
        'cloud_provider': os.getenv('CLOUD_PROVIDER', 'azure'),
        'prometheus_url': os.getenv('PROMETHEUS_URL', 'http://localhost:9090'),
        'grafana_url': os.getenv('GRAFANA_URL', 'http://localhost:3000'),
        'grafana_token': os.getenv('GRAFANA_TOKEN', '')
    }
    
    logger.info("Starting Prometheus and Grafana setup")
    logger.info(f"Configuration: {json.dumps(config, indent=2)}")
    
    try:
        # Initialize manager
        manager = PrometheusGrafanaManager(
            config['prometheus_url'],
            config['grafana_url'],
            config['grafana_token']
        )
        
        # Setup Prometheus
        if manager.setup_prometheus(config['cloud_provider']):
            logger.info("Prometheus setup completed")
        else:
            logger.error("Prometheus setup failed")
        
        # Setup Grafana
        grafana_results = manager.setup_grafana()
        logger.info(f"Grafana setup completed: {len(grafana_results['datasources'])} datasources, {len(grafana_results['dashboards'])} dashboards")
        
        # Export metrics configuration
        manager.export_metrics_config()
        
        # Display results
        print("=== Monitoring Setup Summary ===")
        print(f"Cloud Provider: {config['cloud_provider']}")
        print(f"Prometheus URL: {config['prometheus_url']}")
        print(f"Grafana URL: {config['grafana_url']}")
        print(f"\nGrafana Data Sources: {len(grafana_results['datasources'])}")
        for ds in grafana_results['datasources']:
            print(f"  - {ds}")
        print(f"\nGrafana Dashboards: {len(grafana_results['dashboards'])}")
        for dashboard in grafana_results['dashboards']:
            print(f"  - {dashboard['title']}: {dashboard['url']}")
        
        if grafana_results['errors']:
            print(f"\nErrors: {len(grafana_results['errors'])}")
            for error in grafana_results['errors']:
                print(f"  - {error}")
        
        logger.info("Monitoring setup completed successfully!")
        
        return {
            'status': 'success',
            'prometheus_configured': True,
            'grafana_results': grafana_results,
            'config': config
        }
        
    except Exception as e:
        logger.error(f"Monitoring setup failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
