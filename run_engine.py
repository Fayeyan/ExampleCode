#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
    Wrapper script for the game recommentation engine, including crawler, database & recomm.
    Input: list of user ids as a flat file
    Output: top n recommended games per user in JSON format
    Written by Faye Yan, 2016
"""
import os
import sys
import yaml
import logging
import argparse

from logging import config
from datetime import datetime
from subprocess import Popen, PIPE, STDOUT

from game_rec.crawler import get_inventory_for_user, get_app_details, get_game_page
from game_rec.database import parse_app_info, parse_app_steamspy, merge_dfs, save_to_db

def proc_args():
    args_parser = argparse.ArgumentParser(description="Game Recommendation Engine")
    args_parser.add_argument('input_file', help='Path to user id file')
    args_parser.add_argument('--config', '-c', help='Path to config file', default='conf/config.yml')
    args_parser.add_argument('--output_path', '-o', help='Path to output file folder', default='out/')
    args_parser.add_argument('--output_format', '-f', help='Format of the output file, currently supported: JSON',
                             choices=['json','xml','html'], default='json')

    args,_ = args_parser.parse_known_args()
    return args

def spark_submit(script, args):
    command = Popen(["/usr/hdp/current/spark-client/bin/spark-submit", "--num-executors", "5",
                     "--master", "yarn", script] + args)
    return command.wait()

def main():

    now = datetime.now().strftime('%Y%m%d-%H%M%S')
    # parse commandline parameters
    args = proc_args()
    path_user_id = args.input_file
    if not os.path.isfile(path_user_id):
        logging.exception('Exit. Invalid input file.')
        sys.exit(-1)
    out_path = args.output_path
    out_format = args.output_format
    if out_format != 'json':
        logging.info('Sorry. JSON is the only format currently supported.')
        out_format = 'json'

    # parse config
    config_dict = yaml.load(open(args.config))

    # setup logger
    logging.config.dictConfig(config_dict['log'])
    logger = logging.getLogger()

    ######################
    # step 1: run crawlers
    config = config_dict['crawler']
    # setup output files
    path_user_inventory = os.path.join(out_path, config['path_user_inventory'].replace('[timestamp]', now))
    path_app_user = os.path.join(out_path, config['path_app_user'].replace('[timestamp]', now))
    path_app_info = os.path.join(out_path, config['path_app_info'].replace('[timestamp]', now))
    path_app_steamspy = os.path.join(out_path, config['path_app_steamspy'].replace('[timestamp]', now))

    # step 1.1: get game inventory of each steam user id
    logger.info('Getting game inventory of each steam user id...')
    total_ct = get_inventory_for_user(path_user_id, path_user_inventory, config)
    logger.info('  ...processed %s steam user ids.' % total_ct)

    # step 1.2: get app details
    logger.info('Getting game app details...')
    lst_app_id = get_app_details(path_app_info, path_app_user, config, repeat=config['repeat_num'])
    logger.info('  ...returned %s app ids.' % len(lst_app_id))

    # step 1.3: get game's steamSpy page
    logger.info('Getting game page...')
    get_game_page(lst_app_id, path_app_steamspy, config, repeat=config['repeat_num'])
    logger.info('Crawler Done.')

    ########################################
    # step 2: parse and save crawler outputs
    config = config_dict['database']
    # setup output files
    path_steam_app_info = os.path.join(out_path, config['path_steam_app_info'].replace('[timestamp]', now))
    path_steam_app_tag = os.path.join(out_path, config['path_steam_app_tag'].replace('[timestamp]', now))
    path_master_app_info = os.path.join(out_path, config['path_master_app_info'].replace('[timestamp]', now))

    # step 2.1: parse app info
    logger.info('Parsing app info: %s' % path_app_info)
    df_steam_app = parse_app_info(path_app_info, path_steam_app_info)
    logger.info('Steam app info file created.')

    # step 2.2: parse app steamspy
    logger.info('Parsing app steamspy: %s' % path_app_steamspy)
    df_app_tag = parse_app_steamspy(path_app_steamspy, path_steam_app_tag)
    logger.info('Steam app steamSpy file created.')

    # step 2.3: merge dataframes
    logger.info('Merging dataframes...')
    merge_dfs(df_app_tag, df_steam_app, path_master_app_info)
    logger.info('Dataframes merged.')

    # step 2.4: save steam app info to MySQL
    logger.info('Saving steam app info to DB...')
    save_to_db(path_steam_app_info, config)
    logger.info('Save to MySQL Done.')

    #####################################################################################
    # step 3: generate top 5 game recommendations for each user, using pySpark ALS module 
    logger.info('Utilizing Spark to generate list of top recommendations...')
    exit_code = spark_submit('game_rec/recommendation.py',
                             [path_user_inventory, '-c', args.config, '-o', args.output_path, '-f', args.output_format])
    if exit_code != 0:
        logger.exception('Recommendation generation failed w/ exit_code {}'.format(exit_code))

    logger.info('Done.')


if __name__ == '__main__':
    main()






