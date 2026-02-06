"""
ETL Pipeline Cost Calculator
Calculates costs per datafeed for S3 and Lambda services
"""

import boto3
from datetime import datetime, timedelta
from typing import Dict, List
import yaml


class CostCalculator:
    def __init__(self, config_path: str = 'config.yaml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.region = self.config['region']
        self.s3_client = boto3.client('s3', region_name=self.region)
        self.cloudwatch = boto3.client('cloudwatch', region_name=self.region)
        self.lambda_client = boto3.client('lambda', region_name=self.region)
        
        self.pricing = self.config['pricing']
    
    def get_s3_storage_cost(self, datafeed: Dict) -> Dict:
        """Calculate S3 storage costs for a datafeed"""
        bucket = self.config['s3']['bucket']
        prefix = datafeed['prefix']
        
        total_size_bytes = 0
        object_count = 0
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_size_bytes += obj['Size']
                    object_count += 1
        
        size_gb = total_size_bytes / (1024 ** 3)
        monthly_storage_cost = size_gb * self.pricing['s3']['storage_per_gb_month']
        
        return {
            'size_gb': round(size_gb, 4),
            'object_count': object_count,
            'monthly_storage_cost': round(monthly_storage_cost, 4)
        }
    
    def get_s3_request_cost(self, datafeed: Dict, hours: int = 24) -> Dict:
        """Estimate S3 request costs based on CloudWatch metrics"""
        bucket = self.config['s3']['bucket']
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        # Get request metrics (if enabled)
        put_requests = self._get_cloudwatch_sum(
            namespace='AWS/S3',
            metric_name='PutRequests',
            dimensions=[
                {'Name': 'BucketName', 'Value': bucket},
                {'Name': 'FilterId', 'Value': datafeed['name']}
            ],
            start_time=start_time,
            end_time=end_time
        )
        
        get_requests = self._get_cloudwatch_sum(
            namespace='AWS/S3',
            metric_name='GetRequests',
            dimensions=[
                {'Name': 'BucketName', 'Value': bucket},
                {'Name': 'FilterId', 'Value': datafeed['name']}
            ],
            start_time=start_time,
            end_time=end_time
        )
        
        put_cost = (put_requests / 1000) * self.pricing['s3']['put_request_per_1000']
        get_cost = (get_requests / 1000) * self.pricing['s3']['get_request_per_1000']
        
        return {
            'put_requests': int(put_requests),
            'get_requests': int(get_requests),
            'put_cost': round(put_cost, 4),
            'get_cost': round(get_cost, 4),
            'total_request_cost': round(put_cost + get_cost, 4)
        }

    def get_lambda_cost(self, function_config: Dict, hours: int = 24) -> Dict:
        """Calculate Lambda costs for a function"""
        function_name = function_config['name']
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        # Get invocation count
        invocations = self._get_cloudwatch_sum(
            namespace='AWS/Lambda',
            metric_name='Invocations',
            dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
            start_time=start_time,
            end_time=end_time
        )
        
        # Get duration (milliseconds)
        duration_ms = self._get_cloudwatch_sum(
            namespace='AWS/Lambda',
            metric_name='Duration',
            dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
            start_time=start_time,
            end_time=end_time
        )
        
        # Get memory size
        try:
            response = self.lambda_client.get_function_configuration(
                FunctionName=function_name
            )
            memory_mb = response['MemorySize']
        except Exception:
            memory_mb = 128  # Default
        
        # Calculate costs
        invocation_cost = (invocations / 1_000_000) * self.pricing['lambda']['request_per_million']
        
        gb_seconds = (duration_ms / 1000) * (memory_mb / 1024)
        compute_cost = gb_seconds * self.pricing['lambda']['duration_per_gb_second']
        
        return {
            'invocations': int(invocations),
            'duration_ms': round(duration_ms, 2),
            'memory_mb': memory_mb,
            'gb_seconds': round(gb_seconds, 4),
            'invocation_cost': round(invocation_cost, 4),
            'compute_cost': round(compute_cost, 4),
            'total_lambda_cost': round(invocation_cost + compute_cost, 4)
        }
    
    def get_datafeed_costs(self, hours: int = 24) -> List[Dict]:
        """Calculate total costs per datafeed"""
        results = []
        
        for datafeed in self.config['s3']['datafeeds']:
            datafeed_name = datafeed['name']
            
            # S3 costs
            s3_storage = self.get_s3_storage_cost(datafeed)
            s3_requests = self.get_s3_request_cost(datafeed, hours)
            
            # Lambda costs
            lambda_functions = [
                f for f in self.config['lambda']['functions']
                if f['datafeed'] == datafeed_name
            ]
            
            lambda_total_cost = 0
            lambda_details = []
            for func in lambda_functions:
                lambda_cost = self.get_lambda_cost(func, hours)
                lambda_total_cost += lambda_cost['total_lambda_cost']
                lambda_details.append({
                    'function': func['name'],
                    **lambda_cost
                })
            
            # Aggregate costs
            total_cost = (
                s3_storage['monthly_storage_cost'] / 30 * (hours / 24) +  # Prorated storage
                s3_requests['total_request_cost'] +
                lambda_total_cost
            )
            
            results.append({
                'datafeed': datafeed_name,
                'period_hours': hours,
                's3_storage': s3_storage,
                's3_requests': s3_requests,
                'lambda_details': lambda_details,
                'lambda_total_cost': round(lambda_total_cost, 4),
                'total_cost': round(total_cost, 4)
            })
        
        return results
    
    def _get_cloudwatch_sum(self, namespace: str, metric_name: str, 
                           dimensions: List[Dict], start_time: datetime, 
                           end_time: datetime) -> float:
        """Helper to get sum of CloudWatch metric"""
        try:
            response = self.cloudwatch.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1 hour
                Statistics=['Sum']
            )
            
            total = sum(point['Sum'] for point in response['Datapoints'])
            return total
        except Exception as e:
            print(f"Warning: Could not fetch {metric_name}: {e}")
            return 0.0
