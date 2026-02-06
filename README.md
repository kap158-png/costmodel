# ETL Pipeline Cost Monitor

Real-time cost monitoring solution for AWS ETL pipelines with per-datafeed cost attribution.

## Features

- Real-time cost tracking for S3 and Lambda services
- Per-datafeed cost attribution using S3 prefixes
- Breakdown of compute, storage, and request costs
- Continuous monitoring with configurable refresh intervals
- No cost allocation tags required

## Prerequisites

- Python 3.8+
- AWS credentials configured (via `aws configure` or environment variables)
- IAM permissions for:
  - `s3:ListBucket`
  - `s3:GetObject`
  - `cloudwatch:GetMetricStatistics`
  - `lambda:GetFunctionConfiguration`

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your pipeline in `config.yaml`:
   - Update `region` to your AWS region
   - Set your S3 `bucket` name
   - Define your `datafeeds` with names and prefixes
   - Map Lambda `functions` to datafeeds
   - Adjust `pricing` based on your region (see AWS pricing page)

3. (Optional) Enable S3 request metrics:
   - Go to S3 Console → Your Bucket → Metrics → Request metrics
   - Create filters for each datafeed prefix
   - Use the filter name as the datafeed `name` in config.yaml

## Usage

### Run the monitor:
```bash
python monitor.py
```

The monitor will:
- Refresh every 5 minutes (configurable)
- Show costs for the last 24 hours (configurable)
- Display a summary table and detailed breakdown per datafeed
- Continue running until stopped with Ctrl+C

### One-time cost report:
```python
from cost_calculator import CostCalculator

calculator = CostCalculator('config.yaml')
costs = calculator.get_datafeed_costs(hours=24)

for datafeed in costs:
    print(f"{datafeed['datafeed']}: ${datafeed['total_cost']:.4f}")
```

## Cost Components

### S3 Costs
- **Storage**: Based on total size of objects under each prefix
- **PUT Requests**: Uploads, copies, lifecycle transitions
- **GET Requests**: Downloads, listings
- **Data Transfer**: Outbound data transfer (if applicable)

### Lambda Costs
- **Invocations**: Number of function executions
- **Compute**: GB-seconds based on memory allocation and duration

## Configuration

Edit `config.yaml` to customize:

- `refresh_interval_seconds`: How often to update costs (default: 300)
- `lookback_hours`: Time window for cost analysis (default: 24)
- `pricing`: Update based on your AWS region and pricing tier

## Limitations

- S3 request metrics require manual setup in AWS Console
- Costs are estimates based on CloudWatch metrics and AWS pricing
- Data transfer costs require additional CloudWatch metrics setup
- Historical cost data limited by CloudWatch retention (15 months)

## Extending

To add more cost dimensions:

1. Add pricing to `config.yaml`
2. Implement getter method in `CostCalculator`
3. Update `get_datafeed_costs()` to include new costs
4. Modify `monitor.py` display to show new metrics

## AWS Pricing References

- S3: https://aws.amazon.com/s3/pricing/
- Lambda: https://aws.amazon.com/lambda/pricing/
- Data Transfer: https://aws.amazon.com/ec2/pricing/on-demand/#Data_Transfer
