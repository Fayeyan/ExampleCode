#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
    Libraries to parse app info, app steamspy tagging info for future uses
    Input: crawler outputs including app info and app tagging info
    Output: 3 dataframe files and app info in MySQL table
    Written by Faye Yan, 2016
"""

import re
import os
import sys
import json
import yaml
import logging
import argparse
import sqlalchemy
import pandas as pd

from bs4 import BeautifulSoup
from datetime import datetime

def proc_args():
    args_parser = argparse.ArgumentParser(description="Parse and save crawler outputs to DB")
    args_parser.add_argument('app_info', help='Path to the crawler output: app_info')
    args_parser.add_argument('app_steamspy', help='Path to the crawler output: app_steamspy')
    args_parser.add_argument('--config', '-c', help='Path to config file', default='conf/config.yml')
    args_parser.add_argument('--output_path', '-o', help='Path to output file folder', default='.')

    args,_ = args_parser.parse_known_args()
    return args

def save_to_db(path_steam_app_info, config):
    ''' save to MySQL
        :param path_steam_app_info: steam app info file
        :param config:
        :return: N/A
    '''
    # replace MySQL and csv path
    engine = sqlalchemy.create_engine(config['db_conn'])
    # create table schema if needed
    engine.execute('''
        CREATE TABLE IF NOT EXISTS `tbl_steam_app`(
            `steam_appid` INT,
            `name` VARCHAR(500) CHARACTER SET utf8mb4,
            `type` VARCHAR(15),
            `initial_price` FLOAT,
            `release_date` VARCHAR(20),
            `score` INT,
            `recommendation` INT,
            `windows` BOOLEAN,
            `mac` BOOLEAN,
            `linux` BOOLEAN,
            `header_image` VARCHAR(100)
        );
        ''')
    # write to table
    engine.execute('''
        LOAD DATA INFILE '%s' INTO TABLE `tbl_steam_app`
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (@steam_appid, @name, @type, @initial_price, @release_date, @score, @recommendation, @windows, @mac, @linux, @header_image)
        SET
        steam_appid = nullif(@steam_appid, ''),
        name = nullif(@name, ''),
        type = nullif(@type, ''),
        initial_price = nullif(@initial_price,''),
        release_date = nullif(@release_date,''),
        score = nullif(@score,''),
        recommendation = nullif(@recommendation, ''),
        windows = nullif(@windows, ''),
        mac = nullif(@mac, ''),
        linux = nullif(@linux, ''),
        header_image = nullif(@header_image, '');
        ''' % path_steam_app_info)


def merge_dfs(df_app_tag, df_steam_app, path_master_app_info, logging):
    ''' merge two tables
        :param df_app_tag:
        :param df_steam_app:
        :param path_master_app_info:
        :return: N/A
    '''
    df_master = df_steam_app.merge(df_app_tag, on='steam_appid', how='left')
    df_master.to_csv(path_master_app_info, encoding='utf8', index=False)

def parse_app_steamspy(path_app_steamSpy, path_steam_app_tag):
    ''' extract app tags from steamSpy page
        :param path_app_steamSpy:
        :param path_steam_app_tag:
        :return: df_app_tag
    '''
    with open(path_app_steamSpy, 'rb') as f:
        dic_tag = {}

        lst_raw_string = f.readlines()
        total_count = len(lst_raw_string)
        current_count = 0

        for raw_string in lst_raw_string:
            app_json = json.loads(raw_string)
            steam_appid = int(app_json.keys()[0])
            soup = BeautifulSoup(app_json.values()[0], 'lxml')
            app_summary = soup.find('div', {'class': 'p-r-30'})
            for i in app_summary.find_all('a', href=re.compile('/tag/.*')):
                tag = i.string.lower().replace(' ', '_').replace('-', '_')
                if tag in dic_tag:
                    dic_tag[tag].update({steam_appid: 1})
                else:
                    dic_tag[tag] = {steam_appid: 1}

            current_count += 1
    df_app_tag = pd.DataFrame(dic_tag)
    df_app_tag.index.name = 'steam_appid'
    df_app_tag.reset_index(inplace=True)
    df_app_tag.to_csv(path_steam_app_tag, encoding='utf8', index=False)

    return df_app_tag

def parse_app_info(path_app_info, path_steam_app_info):
    ''' extract app info
        param path_app_info: app info from web crawler
        param path_steam_app_info: parsed and extractd app info
        return: df_app_info
    '''
    with open(path_app_info, 'rb') as f:
        dic_steam_app = {'initial_price': {}, 'name': {}, 'score': {}, 'windows': {}, 'mac': {}, 'linux': {},
                         'type': {}, 'release_date': {}, 'recommendation': {}, 'header_image': {}}
        lst_raw_string = f.readlines()
        total_count = len(lst_raw_string)
        current_count = 0
        for raw_string in lst_raw_string:
            app_data = json.loads(raw_string).values()[0]  # this should match the way dumps app detail
            if app_data.get(
                    'success') == True:  # if success is False, steam api doesn't have information for the requested app id. We can skip that.
                app_data = app_data.get('data')
                steam_id = app_data.get('steam_appid')
                initial_price = app_data.get('price_overview', {}).get('initial')
                if app_data.get('is_free') == True:
                    initial_price = 0  # set price to 0 if the game is free
                app_name = app_data.get('name')
                critic_score = app_data.get('metacritic', {}).get('score')
                app_type = app_data.get('type')
                for (platform, is_supported) in app_data.get('platforms').items():
                    if is_supported == True:
                        dic_steam_app[platform].update({steam_id: 1})
                if app_data.get('release_date', {}).get('coming_soon') == False:
                    release_date = app_data.get('release_date', {}).get('date')
                    if not release_date == '':
                        if re.search(',', release_date) == None:
                            release_date = datetime.strptime(release_date, '%b %Y')
                        else:
                            release_date = datetime.strptime(release_date, '%b %d, %Y')

                recommendation = app_data.get('recommendations', {}).get('total')
                header_image = app_data.get('header_image')
                dic_steam_app['initial_price'].update({steam_id: initial_price})
                dic_steam_app['name'].update({steam_id: app_name})
                dic_steam_app['score'].update({steam_id: critic_score})
                dic_steam_app['type'].update({steam_id: app_type})
                dic_steam_app['release_date'].update({steam_id: release_date})
                dic_steam_app['recommendation'].update({steam_id: recommendation})
                dic_steam_app['header_image'].update({steam_id: header_image})
            current_count += 1
    df_steam_app = pd.DataFrame(dic_steam_app)
    df_steam_app.initial_price = df_steam_app.initial_price.map(lambda x: x / 100.0)
    df_steam_app.index.name = 'steam_appid'
    df_steam_app['windows'] = df_steam_app.windows.fillna(0)
    df_steam_app['mac'] = df_steam_app.mac.fillna(0)
    df_steam_app['linux'] = df_steam_app.linux.fillna(0)
    df_steam_app = df_steam_app[
        ['name', 'type', 'initial_price', 'release_date', 'score', 'recommendation', 'windows', 'mac', 'linux',
         'header_image']]
    df_steam_app.reset_index(inplace=True)
    df_steam_app.to_csv(path_steam_app_info, encoding='utf8', index=False)
    return df_steam_app

def main():

    now = datetime.now().strftime('%Y%m%d-%H%M%S')
    # parse commandline parameters
    args = proc_args()
    if not os.path.isfile(args.app_info):
        logging.exception('Exit. Invalid input app_info file.')
        sys.exit(-1)
    if not os.path.isfile(args.app_steamspy):
        logging.exception('Exit. Invalid input app_steamspy file.')
        sys.exit(-1)
    out_path = args.output_path

    # parse config
    logging.info('Parsing config file...')
    config_dict = yaml.load(open(args.config))
    config = config_dict['database']

    # get input path
    path_app_info = args.app_info
    path_app_steamspy = args.app_steamspy
    # setup output files
    logging.info('Setup output files...')
    path_steam_app_info = os.path.join(out_path, config['path_steam_app_info'].replace('[timestamp]', now))
    path_steam_app_tag = os.path.join(out_path, config['path_steam_app_tag'].replace('[timestamp]', now))
    path_master_app_info = os.path.join(out_path, config['path_master_app_info'].replace('[timestamp]', now))

    # step 1: parse app info
    logging.info('Parsing app info: %s' % path_app_info)
    df_steam_app = parse_app_info(path_app_info, path_steam_app_info)
    logging.info('Steam app info file created.')

    # step 2: parse app steamspy
    logging.info('Parsing app steamspy: %s' % path_app_steamspy)
    df_app_tag = parse_app_steamspy(path_app_steamspy, path_steam_app_tag)
    logging.info('Steam app steamSpy file created.')

    # step 3: merge dataframes
    logging.info('Merging dataframes...')
    merge_dfs(df_app_tag, df_steam_app, path_master_app_info)
    logging.info('Dataframes merged.')

    # step 4: save steam app info to MySQL
    logging.info('Saving steam app info to DB...')
    save_to_db(path_steam_app_info, config)
    logging.info('Save to MySQL Done.')


if __name__ == '__main__':
    main()





