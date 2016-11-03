#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
    Game recommendation engine
    Input: user game inventory list
    Output: top n recommended games per user in JSON format
    Written by Faye Yan, 2016
"""

import re
import os
import sys
import json
import yaml
import logging
import argparse

from datetime import datetime
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS

sc = SparkContext()


def proc_args():
    args_parser = argparse.ArgumentParser(description="Output game recommendations in JSON format")
    args_parser.add_argument('input_file', help='Path to the user_inventory file')
    args_parser.add_argument('--config', '-c', help='Path to config file', default='conf/config.yml')
    args_parser.add_argument('--output_path', '-o', help='Path to output file folder', default='.')
    args_parser.add_argument('--output_format', '-f', help='Format of the output file, currently supported: JSON',
                             choices=['json','xml','html'], default='json')

    args,_ = args_parser.parse_known_args()
    return args


def parse_raw_string(raw_string):
    user_inventory = json.loads(raw_string)
    return user_inventory.items()[0]

#index user id with incremental numbers, will use the numbers to identify users
def id_index(x):
    ((user_id,lst_inventory),index) = x
    return (index, user_id)

#extract inventory of users that had ever played, using the index numbers as keys
def create_tuple(x):
    ((user_id,lst_inventory),index) = x
    if lst_inventory != None:
        return (index, [(i.get('appid'), i.get('playtime_forever')) 
                        for i in lst_inventory if i.get('playtime_forever') > 0])
    else:
        return (index, [])

def main():

    now = datetime.now().strftime('%Y%m%d-%H%M%S')
    # parse commandline parameters
    args = proc_args()
    path_user_inventory = args.input_file
    if not os.path.isfile(path_user_inventory):
        logging.exception('Exit. Invalid input file: %s' % path_user_inventory)
        sys.exit(-1)
    
    out_path = args.output_path
    out_format = args.output_format
    if out_format != 'json':
        logging.info('Sorry. JSON is the only format currently supported.')
        out_format = 'json'        

    # parse config
    logging.info('Parsing config file...')
    config_dict = yaml.load(open(args.config))
    config = config_dict['recommendation']

    # set output file name
    logging.info('Setup output file...')
    path_recommend_games = os.path.join(out_path, config['path_recommend_games'].replace('[timestamp]', now)
                                                                                .replace('foramt', out_format))
    
    # indexing user inventory
    logging.info('Indexing user inventory by user id...')
    user_inventory_rdd = sc.textFile(path_user_inventory).map(parse_raw_string).zipWithIndex()
    # collect (index,user ids)
    logging.info('Collecting index user ids...')
    dic_id_index = user_inventory_rdd.map(id_index).collectAsMap()
    # convert dataframe format
    logging.info('Converting datafram format...')
    training_rdd = user_inventory_rdd.map(create_tuple)\
                                     .flatMapValues(lambda x: x)\
                                     .map(lambda (index,(appid,time)):(index,appid,time))

    # extract the top 10 recommended games
    logging.info('Extract the top 10 recommended games...')
    model = ALS.train(training_rdd, config['model_feature_num'])
    dic_recommended = {}
    for index in dic_id_index.keys():
        try:
            lst_recommended = [i.product for i in model.recommendProducts(index, config['recommend_num'])]
            user_id = dic_id_index.get(index)
            dic_recommended.update({user_id: lst_recommended})
        except:
            pass

    # create output
    logging.info('Creating output file: %s' % path_recommend_games)
    if out_format == 'json':
        json.dump(dic_recommended, open(path_recommend_games, 'wb'), indent=2)
    else:
        #TODO, we can create html page later for web view
        pass

    logging.info('Recommendation Done.')