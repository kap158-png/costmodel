"""
Real-time Cost Monitor
Continuously monitors and displays ETL pipeline costs per datafeed
"""

import time
import os
from datetime import datetime
from tabulate import tabulate
from cost_calculator import CostCalculator


class CostMonitor:
    def __init__(self, config_path: str = 'config.yaml'):
        self.calculator = CostCalculator(config_path)
        with open(config_path, 'r') as f:
            import yaml
            self.config = yaml.safe_load(f)
    
    def display_costs(self, costs_data):
        """Display costs in a formatted table"""
        os.system('clear' if os.name == 'posix' else 'cls')
        
        print("=" * 80)
        print(f"ETL Pipeline Cost Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        print()
        
        # Summary table
        summary_data = []
        for datafeed in costs_data:
            summary_data.append([
                datafeed['datafeed'],
                f"${datafeed['s3_storage']['size_gb']:.2f} GB",
                datafeed['s3_storage']['object_count'],
                f"${datafeed['s3_requests']['total_request_cost']:.4f}",
                f"${datafeed['lambda_total_cost']:.4f}",
                f"${datafeed['total_cost']:.4f}"
            ])
        
        print("COST SUMMARY (Last 24 Hours)")
        print(tabulate(
            summary_data,
            headers=['Datafeed', 'Storage', 'Objects', 'S3 Requests', 'Lambda', 'Total Cost'],
            tablefmt='grid'
        ))
        print()
        
        # Detailed breakdown
        for datafeed in costs_data:
            print(f"\n{datafeed['datafeed'].upper()} - DETAILED BREAKDOWN")
            print("-" * 80)
            
            # S3 Details
            s3_storage = datafeed['s3_storage']
            s3_requests = datafeed['s3_requests']
            print(f"  S3 Storage:")
            print(f"    Size: {s3_storage['size_gb']:.4f} GB")
            print(f"    Objects: {s3_storage['object_count']}")
            print(f"    Monthly Storage Cost: ${s3_storage['monthly_storage_cost']:.4f}")
            print(f"  S3 Requests:")
            print(f"    PUT: {s3_requests['put_requests']} (${s3_requests['put_cost']:.4f})")
            print(f"    GET: {s3_requests['get_requests']} (${s3_requests['get_cost']:.4f})")
            
            # Lambda Details
            print(f"  Lambda Functions:")
            for func in datafeed['lambda_details']:
                print(f"    {func['function']}:")
                print(f"      Invocations: {func['invocations']}")
                print(f"      Duration: {func['duration_ms']:.2f} ms")
                print(f"      Memory: {func['memory_mb']} MB")
                print(f"      GB-Seconds: {func['gb_seconds']:.4f}")
                print(f"      Cost: ${func['total_lambda_cost']:.4f}")
        
        # Total across all datafeeds
        total_all = sum(d['total_cost'] for d in costs_data)
        print("\n" + "=" * 80)
        print(f"TOTAL COST (All Datafeeds): ${total_all:.4f}")
        print("=" * 80)
    
    def run(self):
        """Run the monitor continuously"""
        refresh_interval = self.config['monitoring']['refresh_interval_seconds']
        lookback_hours = self.config['monitoring']['lookback_hours']
        
        print("Starting ETL Pipeline Cost Monitor...")
        print(f"Refresh interval: {refresh_interval} seconds")
        print(f"Lookback period: {lookback_hours} hours")
        print()
        
        try:
            while True:
                try:
                    costs_data = self.calculator.get_datafeed_costs(hours=lookback_hours)
                    self.display_costs(costs_data)
                    print(f"\nNext refresh in {refresh_interval} seconds... (Press Ctrl+C to exit)")
                    time.sleep(refresh_interval)
                except Exception as e:
                    print(f"Error fetching costs: {e}")
                    print(f"Retrying in {refresh_interval} seconds...")
                    time.sleep(refresh_interval)
        except KeyboardInterrupt:
            print("\n\nMonitor stopped by user.")


if __name__ == '__main__':
    monitor = CostMonitor()
    monitor.run()
