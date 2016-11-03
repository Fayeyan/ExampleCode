#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
    Web crawlers to get user inventory info and app info from api.steampowered.com and steamspy.com 
    To be used for user-specific game recommendations
    Input: list of user ids as a flat file
    Output: User inventory info, app info & app tagging info
    Written by Faye Yan, 2016
"""

import os
import sys
import time
import json
import yaml
import logging
import requests
import argparse

from datetime import datetime

def proc_args():
    args_parser = argparse.ArgumentParser(description="Web Crawler")
    args_parser.add_argument('input_file', help='Path to user id file')
    args_parser.add_argument('--config', '-c', help='Path to config file', default='conf/config.yml')
    args_parser.add_argument('--output_path', '-o', help='Path to output file folder', default='.')

    args,_ = args_parser.parse_known_args()
    return args

def get_inventory_for_user(path_user_id, path_user_inventory, config_dict):
    ''' crawler 1: get game inventory of each steam user id
        :param path_user_id: user id list input
        :param path_user_inventory: user inventory output
        :return: total_count: total number of user ids
    '''
    lst_user_id = []
    with open(path_user_id, 'rb') as f:
        lst_user_id = f.readlines()
    total_count = len(lst_user_id)
    current_count = 0
    with open(path_user_inventory, 'wb') as f:
        for user_id in lst_user_id:
            base_url = config_dict['base_url']
            params = {'key': config_dict['key'],
                      'steamid': user_id,
                      'format': 'json'}
            r = requests.get(base_url, params=params)
            user_inventory = r.json().get('response').get('games')
            f.write(json.dumps({user_id.strip(): user_inventory}))
            f.write('\n')
            current_count += 1

    return total_count

def get_app_details(path_app_info, path_app_user, config_dict, repeat=3):
    ''' crawler 2: get app details
        :param path_app_info: app details, from steam web api
        :param path_app_user: estimated user counts of each steam game from steamspy
    '''
    r = requests.get(config_dict['steamspy_url'])
    dic_app_user = r.json()
    with open(path_app_user, 'wb') as f:
        json.dump(dic_app_user, f)
    lst_app_id = dic_app_user.keys()
    total_count = len(lst_app_id)
    current_count = 0

    with open(path_app_info, 'wb') as f:
        for app_id in lst_app_id:
            url_app_detail = config_dict['steampower_url'].replace('[app_id]', app_id)
            result = None
            # try no more than x times for each app id
            for i in range(repeat):
                try:
                    r = requests.get(url_app_detail)
                    # we know the result is in JSON format, so use json() to parse it from text directly
                    result = r.json()
                    # if all scripts under try block was run successfully, we get what we need.
                    # Break the loop and move onto the next app id
                    break
                except:
                    time.sleep(5)
                    # otherwise, continue for loop and try to send the http request again
                    pass
            f.write(json.dumps(result))
            f.write('\n')
            current_count += 1
            if current_count % 200 == 0:
                # this API endpoint has a limit of 200 calls per 5 min
                time.sleep(300)
            else:
                time.sleep(.5)

    return lst_app_id

def get_game_page(lst_app_id, path_app_steamspy, config_dict, repeat=3):
    ''' crawler 3: get game's steamSpy page
        :param lst_app_id: list of app ids
        :param path_app_steamspy: app info from steamspy page
        :return: app tagging info
    '''
    current_count = 0
    with open(path_app_steamspy, 'wb') as f:
        for app_id in lst_app_id:
            url_app_steamspy = config_dict['steamspy_app'].replace('[app_id]', 'app_id')
            for i in range(repeat):
                try:
                    r = requests.get(url_app_steamspy)
                    # because the result is a html page, we save it as text for now
                    f.write(json.dumps({app_id: r.text}))
                    f.write('\n')
                    break
                except:
                    time.sleep(5)
                    pass

            current_count += 1
            if current_count % 200 == 0:
                time.sleep(10)
            else:
                # it's highly recommended to put a latency after each http request,
                # which helps to reduce burdon on the server and avoid your IP being blocked
                time.sleep(1)


def main():

    now = datetime.now().strftime('%Y%m%d-%H%M%S')
    # parse commandline parameters
    args = proc_args()
    path_user_id = args.input_file
    if not os.path.isfile(path_user_id):
        logging.exception('Exit. Invalid input file: %s' % path_user_id)
        sys.exit(-1)
    out_path = args.output_path

    # parse config
    logging.info('Parsing config file...')
    config_dict = yaml.load(open(args.config))
    config = config_dict['crawler']

    # setup output files
    logging.info('Setup output file...')
    path_user_inventory = os.path.join(out_path, config['path_user_inventory'].replace('[timestamp]', now))
    path_app_user = os.path.join(out_path, config['path_app_user'].replace('[timestamp]', now))
    path_app_info = os.path.join(out_path, config['path_app_info'].replace('[timestamp]', now))
    path_app_steamspy = os.path.join(out_path, config['path_app_steamspy'].replace('[timestamp]', now))

    # step 1: get game inventory of each steam user id
    logging.info('Getting game inventory of each steam user id...')
    total_ct = get_inventory_for_user(path_user_id, path_user_inventory, config)
    logging.info('  ...processed %s steam user ids.' % total_ct)

    # step 2: get app details
    logging.info('Getting game app details...')
    lst_app_id = get_app_details(path_app_info, path_app_user, config, repeat=config['repeat_num'])
    logging.info('  ...returned %s app ids.' % len(lst_app_id))

    # step 3: get game's steamspy page
    logging.info('Getting game page...')
    get_game_page(lst_app_id, path_app_steamspy, config, repeat=config['repeat_num'])
    logging.info('Crawler Done.')


if __name__ == '__main__':
    main()






