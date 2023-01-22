import boto3
from botocore.config import Config
import logging
import pandas as pd
import traceback

class Getinstanceinfo(object):

    def get_instance_config(self, row, pricing_df):
        try:
            client = boto3.client('rds', region_name=row.region)
            db_instance = client.describe_db_instances(DBInstanceIdentifier=row.instance)
            instance_type = db_instance['DBInstances'][0]['DBInstanceClass']
            temp_df = pricing_df[pricing_df['InstanceType']==instance_type]
            temp_df['Memory'] = temp_df['Memory'].str.extract('(\d+)', expand=False)
            temp_df['Memory'] = temp_df['Memory'].astype(int)
            temp_df['vCPU'] = temp_df['vCPU'].astype(int)
            vcpu = temp_df['vCPU'].iloc[0]
            memory = temp_df['Memory'].iloc[0]
            return memory, vcpu, instance_type
        except Exception as e: 
            logging.error(f'An error occurred during instance info gathering')
            traceback.print_exc()
            pass
    
    def get_instance_data(self):
        try:
            pricing_csv = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/us-east-1/index.csv'
            pricing_df = pd.read_csv(pricing_csv, skiprows=5)
            pricing_df.columns = pricing_df.columns.str.replace(' ', '')
            return pricing_df
        except Exception as e: 
            logging.error(f'An error occurred during bulk pricing pull')
            traceback.print_exc()

    def get_current_price(self, region, db_engine, db_instance_size, ri_purchase_option, ri_term_type, ri_deployment_option):
        try:
            pricing_csv = f'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/{region}/index.csv'
            pricing_df = pd.read_csv(pricing_csv, skiprows=5)
            
            temp_df = pricing_df.loc[(pricing_df['Instance Type'] == db_instance_size) & \
            (pricing_df['TermType'] == ri_purchase_option) & \
            (pricing_df['PurchaseOption'] == ri_term_type) & \
            (pricing_df['Database Engine'] == db_engine) & \
            (pricing_df['Deployment Option'] == ri_deployment_option)]

            provisioned_price_hourly = temp_df['PricePerUnit'].iloc[0]
            
            temp_df2 = pricing_df.loc[(pricing_df['Product Family'] == 'ServerlessV2') & \
            (pricing_df['Database Engine'] == db_engine)]

            serverless_price_hourly = temp_df2['PricePerUnit'].iloc[0]

            return provisioned_price_hourly, serverless_price_hourly 
        except Exception as e: 
            logging.error(f'An error occurred during instance pricing pull')
            traceback.print_exc()