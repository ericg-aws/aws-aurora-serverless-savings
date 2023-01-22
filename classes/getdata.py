import boto3
from botocore.config import Config
from datetime import datetime, timedelta
import logging
import pandas as pd
from random import randrange
import traceback

class Getdata(object):
    
    def cw_rds_pull_metric(self, cw_client, metric_name, namespace, instance_name, instance, stat, period, args):
        try:
            logging.info(f'Pulling cloudwatch data for: {instance}')
            id_name = f'rdsmetricpull{randrange(1000000)}'
            
            if args.start_time and args.end_time is not None:
                start = args.start_time
                end = args.end_time
            else:
                start = ((datetime.utcnow().replace(microsecond=0, second=0, minute=0) - timedelta(hours=1)) - timedelta(days=args.days_back))
                end = (datetime.utcnow().replace(microsecond=0, second=0, minute=0) - timedelta(hours=1))

            logging.info(f'Start and end times are: {start}, {end}')
            cw_response = cw_client.get_metric_data(
                MetricDataQueries=[
                    {
                        'Id': id_name,
                        'MetricStat': {
                            'Metric': {
                                'Namespace': namespace,
                                'MetricName': metric_name,
                                'Dimensions': [
                                    {
                                        'Name': instance_name,
                                        'Value': instance
                                    },
                                ]
                            },
                            'Period': period,
                            'Stat': stat,
                        },
                        'ReturnData': True
                    }
                ],
                StartTime=start,
                EndTime=end,
                ScanBy='TimestampDescending'
            )

            df_temp = pd.DataFrame(cw_response['MetricDataResults'][0])
            return df_temp
        except Exception as e: 
            logging.error(f'An error occurred cloudwatch metric pull for {instance}')
            traceback.print_exc()