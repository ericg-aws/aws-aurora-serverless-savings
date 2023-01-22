#!/usr/bin/env python
# purpose: to pull cloudwatch statistics RDS provisioned instances to infer serverlessv2 capacity units (ACU) 
# example for days back from current: python inference-get-metrics.py -d 4
# utc start and endtime example:  python inference-get-metrics.py -s '2022-06-25 02:00:00' -e '2022-07-12 02:00:00'

from classes.getdata import Getdata
from classes.getinstanceinfo import Getinstanceinfo

import argparse
import boto3
from botocore.config import Config
import logging
import os
import pandas as pd
import pickle
import traceback

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# parse command-line arguments for region and input file
# csv must have columns: instance,region
def parse_args():
    try:
        parser = argparse.ArgumentParser(description='cloudwatch metric pull script')
        parser.add_argument('-i', '--input_file', help='input_file', type=str, required=False)
        parser.add_argument('-o', '--ouput_file', help='ouput_file', type=str, required=False)
        parser.add_argument('-c', '--cost_file', help='cost_file', type=str, required=False)
        parser.add_argument('-d', '--days_back', help='days_back', type=int, required=False)
        parser.add_argument('-s', '--start_time', help='start_time', type=str, required=False)
        parser.add_argument('-e', '--end_time', help='end_time', type=str, required=False)
        parser.add_argument('-g', '--db_engine', help='db_engine', type=str, required=False)
        parser.add_argument('-t', '--ri_term_type', help='ri_term_type', type=str, required=False)
        parser.add_argument('-p', '--ri_purchase_option', help='ri_purchase_option', type=str, required=False)
        parser.add_argument('-r', '--ri_deployment_option', help='ri_deployment_option', type=str, required=False)
        parser.set_defaults(input_file = 'data/provisioned_instances.csv', \
                            output_file = 'data/inference_output.csv', \
                            cost_file = 'data/cost_output.csv', \
                            days_back = 4, \
                            ri_term_type = 'No Upfront', \
                            ri_purchase_option = 'Reserved', \
                            ri_deployment_option = 'Single-AZ', \
                            db_engine = 'Aurora PostgreSQL' \
                            )
        # options: Single-AZ, Multi-AZ
        # options: MariaDB, PostgreSQL, Aurora MySQL, Aurora PostgreSQL
        args = parser.parse_args()
        return args
    except Exception as e: 
        logging.error(f'An error occurred during parsing of args')
        traceback.print_exc()

def infer_acu(args, df_provisioned):
    try:
        model = pickle.load(open('model.pickle.dat', "rb"))
        x = df_provisioned.drop(['timestamp', 'provisioned_instance', 'provisioned_region'], axis=1).copy()
        predictions = model.predict(x)
        # evaluate predictions
        logging.info(f'Prediction ACU data mean is: {predictions.mean():.2f}')
        df_provisioned['serverless_acu'] = predictions.tolist()
        df_provisioned = df_provisioned.round(decimals = 2)
        return df_provisioned
    except Exception as e: 
        logging.error(f'An error occurred during import of model')
        traceback.print_exc()

def get_rds_data(args, instance_df, getinstanceinfo):
        getdata = Getdata()
        
        # all instances df 
        df_combined = pd.DataFrame()
        instance_price_list = []
        
        # boto3 client config
        config = Config(
            retries = dict(
                max_attempts = 10
            )
        )
        
        for row in instance_df.itertuples():
            try:
                logging.info(f'Processing {row.instance}')
                instance_price_temp_df = pd.DataFrame(columns=['provisioned_instance', 'provisioned_price_hourly', 'serverless_price_hourly'])
                
                cw_client = boto3.client('cloudwatch', region_name=row.region, config=config)
                df_provisioned = getdata.cw_rds_pull_metric(cw_client, 'CPUUtilization', 'AWS/RDS', 'DBInstanceIdentifier', \
                    row.instance, 'Average', 300, args)

                df_provisioned.drop(['Id', 'Label', 'StatusCode'], axis=1, inplace=True)
                df_provisioned.rename(columns = {'Values':'provisioned_util', 'Timestamps':'timestamp' }, inplace=True)
                
                df_provisioned['provisioned_vcpu'] = row.vpcu
                df_provisioned['provisioned_mem'] = row.memory
                df_provisioned['provisioned_instance'] = row.instance
                df_provisioned['provisioned_region'] = row.region

                logging.info(f'df_provisioned row count: {int(df_provisioned.shape[0])}')
                
                provisioned_price_hourly, serverless_price_hourly = getinstanceinfo.get_current_price(row.region, \
                    args.db_engine, row.instance_type, args.ri_purchase_option, args.ri_term_type, args.ri_deployment_option)
                
                instance_price_list.append([row.instance, provisioned_price_hourly, serverless_price_hourly])

                # combine specific instance dataframe with all instances dataframe
                df_combined = pd.concat([df_combined, df_provisioned])
                
            except Exception as e: 
                logging.error(f'An error occurred during making call for instance: {row.instance}')
                traceback.print_exc()
                pass
        return df_combined, instance_price_list


def calc_provisioned_monthly(cost):
    try:
        monthly_cost = (cost*730)
        return monthly_cost
    except Exception as e: 
        logging.error(f'An error occurred during the monthly cost calculation')
        traceback.print_exc()
    
def calc_costs(args, instance_price_list, df_combined):
    try:
        df_averages = df_combined.groupby(['provisioned_instance'], as_index=False).mean()
        for instance in instance_price_list:
            provisioned_instance = instance[0]
            provisioned_price = instance[1]
            serverless_price = instance[2]
            df_averages.loc[df_averages['provisioned_instance'] == provisioned_instance, 'provisioned_monthly_cost'] = calc_provisioned_monthly(provisioned_price)
            df_averages['serverless_monthly_cost'] = (df_averages.serverless_acu*730*serverless_price)
        df_averages['serverless_savings'] = (abs((df_averages['serverless_monthly_cost']/df_averages['provisioned_monthly_cost']) - 1) * 100)
        df_averages = df_averages.round({'serverless_monthly_cost':1, 'serverless_acu':3, 'serverless_savings':0})

        # output cost savings to local csv 
        if args.cost_file is not None:
            df_averages.to_csv(args.cost_file, index=False)
            logging.info(f'Output file written to: {args.cost_file}')
        else:
            if not os.path.exists('data'):
                os.makedirs('data')
            cost_file = f"data/cost_output.csv"
            df_averages.to_csv(cost_file, index=False)
            logging.info(f'Output file written to: {cost_file}')

    except Exception as e: 
        logging.error(f'An error occurred calculating costs for instance: {provisioned_instance}')
        traceback.print_exc()

def main():

    getinstanceinfo = Getinstanceinfo()
        
    args = parse_args()

    # read in instance csv 
    instance_df = pd.read_csv(args.input_file, sep=',', header=0, na_filter=True)
    
    # pull down bulk price list
    pricing_df = getinstanceinfo.get_instance_data()
    
    # get instance memory and cpu
    instance_df[['memory', 'vpcu', 'instance_type']] = instance_df.apply (lambda row: getinstanceinfo.get_instance_config(row, pricing_df), \
        axis=1, result_type='expand')
    
    # get both provisioned and serverless data for needed time period
    df_combined, instance_price_list = get_rds_data(args, instance_df, getinstanceinfo)

    if not df_combined.empty:
        # infer ACU values
        df_infer = infer_acu(args, df_combined)
        df_infer.to_csv(args.output_file, index=False)
        
        # calculate provisioned and serverless costs - per month and % savings
        calc_costs(args, instance_price_list, df_combined)

if __name__ == "__main__":
    main()