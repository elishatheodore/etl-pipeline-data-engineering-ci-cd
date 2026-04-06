#!/usr/bin/env python3
"""
Microsoft Fabric Lakehouse Setup Script
Configures lakehouse, datasets, and reports for Brazilian E-Commerce analytics
"""

import os
import sys
import json
import logging
from typing import Dict, Any, List
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fabric_setup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class FabricLakehouseManager:
    """Manages Microsoft Fabric Lakehouse setup and configuration"""
    
    def __init__(self, workspace_id: str, tenant_id: str, client_id: str, client_secret: str):
        self.workspace_id = workspace_id
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.base_url = "https://api.fabric.microsoft.com/v1"
        
    def get_access_token(self) -> bool:
        """Get access token for Microsoft Fabric API"""
        try:
            token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            
            data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'scope': 'https://api.fabric.microsoft.com/.default'
            }
            
            response = requests.post(token_url, data=data)
            response.raise_for_status()
            
            self.access_token = response.json()['access_token']
            logger.info("Successfully obtained Fabric access token")
            return True
            
        except Exception as e:
            logger.error(f"Failed to get access token: {str(e)}")
            return False
    
    def make_api_request(self, method: str, endpoint: str, data=None) -> Dict[str, Any]:
        """Make API request to Microsoft Fabric"""
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        url = f"{self.base_url}/{endpoint}"
        
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=headers, json=data)
            elif method.upper() == 'PUT':
                response = requests.put(url, headers=headers, json=data)
            elif method.upper() == 'DELETE':
                response = requests.delete(url, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            raise
    
    def create_lakehouse(self, lakehouse_name: str, description: str = "") -> Dict[str, Any]:
        """Create a new lakehouse in the workspace"""
        logger.info(f"Creating lakehouse: {lakehouse_name}")
        
        payload = {
            "displayName": lakehouse_name,
            "description": description,
            "type": "Lakehouse"
        }
        
        try:
            response = self.make_api_request('POST', f'workspaces/{self.workspace_id}/lakehouses', payload)
            lakehouse_id = response['id']
            logger.info(f"Successfully created lakehouse {lakehouse_name} with ID: {lakehouse_id}")
            return response
        except Exception as e:
            logger.error(f"Failed to create lakehouse: {str(e)}")
            raise
    
    def create_shortcut(self, lakehouse_id: str, shortcut_name: str, path: str, target: Dict[str, Any]) -> Dict[str, Any]:
        """Create a shortcut in the lakehouse"""
        logger.info(f"Creating shortcut: {shortcut_name}")
        
        payload = {
            "name": shortcut_name,
            "path": path,
            "target": target
        }
        
        try:
            response = self.make_api_request('POST', f'workspaces/{self.workspace_id}/lakehouses/{lakehouse_id}/shortcuts', payload)
            logger.info(f"Successfully created shortcut {shortcut_name}")
            return response
        except Exception as e:
            logger.error(f"Failed to create shortcut: {str(e)}")
            raise
    
    def setup_brazilian_ecommerce_lakehouse(self) -> Dict[str, Any]:
        """Set up the complete Brazilian E-Commerce lakehouse"""
        logger.info("Setting up Brazilian E-Commerce Lakehouse")
        
        # Create lakehouse
        lakehouse_name = "BrazilianEcommerceLakehouse"
        description = "Lakehouse for Brazilian E-Commerce data analytics and reporting"
        
        try:
            lakehouse_response = self.create_lakehouse(lakehouse_name, description)
            lakehouse_id = lakehouse_response['id']
            
            # Create shortcuts to Azure Data Lake Storage
            shortcuts = [
                {
                    "name": "gold_layer",
                    "path": "gold",
                    "target": {
                        "type": "AdlsGen2",
                        "adlsGen2": {
                            "location": f"https://stbrazilianecommerce.dfs.core.windows.net/gold",
                            "subPath": "gold",
                            "connectionId": "your-connection-id"  # This would be configured separately
                        }
                    }
                },
                {
                    "name": "silver_layer",
                    "path": "silver",
                    "target": {
                        "type": "AdlsGen2",
                        "adlsGen2": {
                            "location": f"https://stbrazilianecommerce.dfs.core.windows.net/silver",
                            "subPath": "silver",
                            "connectionId": "your-connection-id"
                        }
                    }
                },
                {
                    "name": "bronze_layer",
                    "path": "bronze",
                    "target": {
                        "type": "AdlsGen2",
                        "adlsGen2": {
                            "location": f"https://stbrazilianecommerce.dfs.core.windows.net/bronze",
                            "subPath": "bronze",
                            "connectionId": "your-connection-id"
                        }
                    }
                }
            ]
            
            # Create shortcuts (note: connection IDs need to be pre-configured)
            for shortcut in shortcuts:
                try:
                    self.create_shortcut(lakehouse_id, shortcut['name'], shortcut['path'], shortcut['target'])
                except Exception as e:
                    logger.warning(f"Could not create shortcut {shortcut['name']}: {str(e)}")
            
            return {
                "status": "success",
                "lakehouse_id": lakehouse_id,
                "lakehouse_name": lakehouse_name,
                "shortcuts_created": len(shortcuts)
            }
            
        except Exception as e:
            logger.error(f"Failed to setup lakehouse: {str(e)}")
            raise
    
    def create_semantic_model(self, lakehouse_id: str, model_name: str) -> Dict[str, Any]:
        """Create a semantic model for analytics"""
        logger.info(f"Creating semantic model: {model_name}")
        
        # Define the semantic model structure
        model_definition = {
            "displayName": model_name,
            "description": "Semantic model for Brazilian E-Commerce analytics",
            "type": "SemanticModel",
            "definition": {
                "parts": [
                    {
                        "path": "model.bim",
                        "payload": self._generate_bim_file(),
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        
        try:
            response = self.make_api_request('POST', f'workspaces/{self.workspace_id}/semanticModels', model_definition)
            logger.info(f"Successfully created semantic model {model_name}")
            return response
        except Exception as e:
            logger.error(f"Failed to create semantic model: {str(e)}")
            raise
    
    def _generate_bim_file(self) -> str:
        """Generate BIM file content for semantic model"""
        bim_content = {
            "name": "BrazilianEcommerceModel",
            "tables": [
                {
                    "name": "FactOrders",
                    "columns": [
                        {"name": "OrderID", "dataType": "String"},
                        {"name": "CustomerID", "dataType": "String"},
                        {"name": "ProductID", "dataType": "String"},
                        {"name": "SellerID", "dataType": "String"},
                        {"name": "DateKey", "dataType": "Int64"},
                        {"name": "Price", "dataType": "Decimal"},
                        {"name": "FreightValue", "dataType": "Decimal"},
                        {"name": "OrderItemTotalValue", "dataType": "Decimal"},
                        {"name": "ReviewScore", "dataType": "Int64"},
                        {"name": "OrderStatus", "dataType": "String"},
                        {"name": "DeliveryStatus", "dataType": "String"},
                        {"name": "OrderDeliveryDays", "dataType": "Int64"}
                    ],
                    "measures": [
                        {
                            "name": "Total Revenue",
                            "expression": "SUM(FactOrders[OrderItemTotalValue])",
                            "formatString": "\\$#,0.00"
                        },
                        {
                            "name": "Total Orders",
                            "expression": "DISTINCTCOUNT(FactOrders[OrderID])",
                            "formatString": "#,0"
                        },
                        {
                            "name": "Average Order Value",
                            "expression": "DIVIDE([Total Revenue], [Total Orders])",
                            "formatString": "\\$#,0.00"
                        },
                        {
                            "name": "Average Review Score",
                            "expression": "AVERAGE(FactOrders[ReviewScore])",
                            "formatString": "#,0.0"
                        }
                    ]
                },
                {
                    "name": "DimCustomers",
                    "columns": [
                        {"name": "CustomerKey", "dataType": "Int64"},
                        {"name": "CustomerID", "dataType": "String"},
                        {"name": "CustomerCity", "dataType": "String"},
                        {"name": "CustomerState", "dataType": "String"},
                        {"name": "CustomerRegion", "dataType": "String"}
                    ]
                },
                {
                    "name": "DimProducts",
                    "columns": [
                        {"name": "ProductKey", "dataType": "Int64"},
                        {"name": "ProductID", "dataType": "String"},
                        {"name": "ProductCategoryNameEnglish", "dataType": "String"},
                        {"name": "ProductWeightG", "dataType": "Decimal"},
                        {"name": "ProductVolumeCM3", "dataType": "Decimal"}
                    ]
                },
                {
                    "name": "DimDate",
                    "columns": [
                        {"name": "DateKey", "dataType": "Int64"},
                        {"name": "Date", "dataType": "DateTime"},
                        {"name": "Year", "dataType": "Int64"},
                        {"name": "Quarter", "dataType": "Int64"},
                        {"name": "Month", "dataType": "Int64"},
                        {"name": "MonthName", "dataType": "String"},
                        {"name": "DayName", "dataType": "String"},
                        {"name": "IsWeekend", "dataType": "Boolean"}
                    ]
                },
                {
                    "name": "DimSellers",
                    "columns": [
                        {"name": "SellerKey", "dataType": "Int64"},
                        {"name": "SellerID", "dataType": "String"},
                        {"name": "SellerCity", "dataType": "String"},
                        {"name": "SellerState", "dataType": "String"},
                        {"name": "SellerRegion", "dataType": "String"}
                    ]
                }
            ],
            "relationships": [
                {
                    "name": "FactOrdersDimCustomers",
                    "fromTable": "FactOrders",
                    "fromColumn": "CustomerID",
                    "toTable": "DimCustomers",
                    "toColumn": "CustomerID"
                },
                {
                    "name": "FactOrdersDimProducts",
                    "fromTable": "FactOrders",
                    "fromColumn": "ProductID",
                    "toTable": "DimProducts",
                    "toColumn": "ProductID"
                },
                {
                    "name": "FactOrdersDimDate",
                    "fromTable": "FactOrders",
                    "fromColumn": "DateKey",
                    "toTable": "DimDate",
                    "toColumn": "DateKey"
                },
                {
                    "name": "FactOrdersDimSellers",
                    "fromTable": "FactOrders",
                    "fromColumn": "SellerID",
                    "toTable": "DimSellers",
                    "toColumn": "SellerID"
                }
            ]
        }
        
        import base64
        bim_json = json.dumps(bim_content, indent=2)
        return base64.b64encode(bim_json.encode('utf-8')).decode('utf-8')
    
    def create_report(self, lakehouse_id: str, report_name: str) -> Dict[str, Any]:
        """Create a Power BI report"""
        logger.info(f"Creating report: {report_name}")
        
        # Define report template
        report_definition = {
            "displayName": report_name,
            "description": "Brazilian E-Commerce Analytics Dashboard",
            "type": "Report",
            "definition": {
                "parts": [
                    {
                        "path": "report.json",
                        "payload": self._generate_report_template(),
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        
        try:
            response = self.make_api_request('POST', f'workspaces/{self.workspace_id}/reports', report_definition)
            logger.info(f"Successfully created report {report_name}")
            return response
        except Exception as e:
            logger.error(f"Failed to create report: {str(e)}")
            raise
    
    def _generate_report_template(self) -> str:
        """Generate report template"""
        report_template = {
            "version": "1.0",
            "visualizations": [
                {
                    "name": "TotalRevenueCard",
                    "type": "card",
                    "layout": {
                        "x": 0,
                        "y": 0,
                        "width": 200,
                        "height": 100
                    },
                    "dataMappings": [
                        {
                            "measure": "Total Revenue"
                        }
                    ]
                },
                {
                    "name": "OrdersByDate",
                    "type": "lineChart",
                    "layout": {
                        "x": 0,
                        "y": 100,
                        "width": 400,
                        "height": 200
                    },
                    "dataMappings": [
                        {
                            "category": "Date",
                            "measure": "Total Orders"
                        }
                    ]
                },
                {
                    "name": "RevenueByCategory",
                    "type": "barChart",
                    "layout": {
                        "x": 400,
                        "y": 100,
                        "width": 400,
                        "height": 200
                    },
                    "dataMappings": [
                        {
                            "category": "ProductCategoryNameEnglish",
                            "measure": "Total Revenue"
                        }
                    ]
                },
                {
                    "name": "SalesByRegion",
                    "type": "map",
                    "layout": {
                        "x": 0,
                        "y": 300,
                        "width": 800,
                        "height": 300
                    },
                    "dataMappings": [
                        {
                            "location": "CustomerState",
                            "measure": "Total Revenue"
                        }
                    ]
                }
            ]
        }
        
        import base64
        report_json = json.dumps(report_template, indent=2)
        return base64.b64encode(report_json.encode('utf-8')).decode('utf-8')

def main():
    """Main execution function"""
    # Configuration - these should come from environment variables or Key Vault
    config = {
        'workspace_id': os.getenv('FABRIC_WORKSPACE_ID'),
        'tenant_id': os.getenv('FABRIC_TENANT_ID'),
        'client_id': os.getenv('FABRIC_CLIENT_ID'),
        'client_secret': os.getenv('FABRIC_CLIENT_SECRET')
    }
    
    # Validate configuration
    missing_config = [key for key, value in config.items() if not value]
    if missing_config:
        logger.error(f"Missing required configuration: {missing_config}")
        sys.exit(1)
    
    logger.info("Starting Microsoft Fabric Lakehouse setup")
    
    try:
        # Initialize Fabric manager
        fabric_manager = FabricLakehouseManager(**config)
        
        # Get access token
        if not fabric_manager.get_access_token():
            logger.error("Failed to obtain Fabric access token")
            sys.exit(1)
        
        # Setup lakehouse
        lakehouse_result = fabric_manager.setup_brazilian_ecommerce_lakehouse()
        lakehouse_id = lakehouse_result['lakehouse_id']
        
        # Create semantic model
        semantic_model_result = fabric_manager.create_semantic_model(
            lakehouse_id, 
            "BrazilianEcommerceModel"
        )
        
        # Create report
        report_result = fabric_manager.create_report(
            lakehouse_id,
            "BrazilianEcommerceDashboard"
        )
        
        # Log results
        setup_summary = {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "lakehouse": lakehouse_result,
            "semantic_model": semantic_model_result,
            "report": report_result
        }
        
        logger.info(f"Fabric setup completed successfully: {json.dumps(setup_summary, indent=2)}")
        
        return setup_summary
        
    except Exception as e:
        logger.error(f"Fabric setup failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
