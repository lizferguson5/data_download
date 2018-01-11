#!/usr/bin/env python
"""
Created on Jan 9 2018
@author: lgarzio@marine.rutgers.edu
@brief: This script sends data requests for all urls contained in data_request_urls.csv (output from
get_data_request_urls.py) and provides a summary output that contains the links to the THREDDS data server.

@usage:
sDir: directory where data_request_urls.csv is located, also location to save summary output
fname: csv file containing urls (e.g. data_request_urls.csv)
username: OOI API username
token: OOI API password
"""

import datetime as dt
import os
import requests
import pandas as pd


def define_status_outputUrl(r):
    response = r.json()
    if r.status_code == 200:
        print 'Data request successful'

        try:
            status = response['status']
        except KeyError:
            status = 'Data available for request'

        try:
            outputUrl = response['outputURL']
        except KeyError:
            outputUrl = 'no_output_url'
    else:
        print 'Data request failed'
        outputUrl = 'no_output_url'

        try:
            status = response['message']['status']
        except TypeError:
            status = 'No status provided'

    return status, outputUrl


def main(sDir, username, token, now):
    print '\nNOTICE: use this data request tool with caution. DO NOT send too many data requests at once!'

    fname = 'data_request_urls_%s.csv' %now
    url_file = pd.read_csv(os.path.join(sDir, fname), header=None)
    cont = raw_input('\nThere are {} requests to send, are you sure you want to continue? y/<n>: '.format(len(url_file))) or 'n'

    if 'y' in cont:
        print '\nSending data requests'
        summary = {}
        for i,j in url_file.iterrows():
            summary[i] = {}
            summary[i]['request_url'] = j[0]
            session = requests.session()
            r = session.get(j[0], auth=(username, token))  #send request

            status, outputUrl = define_status_outputUrl(r)

            summary[i]['status'] = status
            summary[i]['outputUrl'] = outputUrl

        summary_df = pd.DataFrame.from_dict(summary,orient='index')
        filename = 'data_request_summary_%s.csv' %now
        summary_df.to_csv(os.path.join(sDir, filename), index=False)
    else:
        print '\nCancelling data requests.'


if __name__ == '__main__':
    sDir = '/Users/lgarzio/Documents/OOI'
    username = 'api_username'
    token = 'api_token'
    now = dt.datetime.now().strftime('%Y%m%dT%H%M')
    main(sDir, username, token, now)