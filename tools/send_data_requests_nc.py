#!/usr/bin/env python
"""
Created on Jan 9 2018
@author: Lori Garzio
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
import csv
import time


def define_status_outputUrl(r):
    response = r.json()
    if r.status_code == 200:
        print 'Data request sent'

        try:
            status = response['status']
        except KeyError:
            status = 'Data available for request'
        print status

        try:
            outputUrl = response['outputURL']
        except KeyError:
            outputUrl = 'no_output_url'
    else:
        print 'Data request failed'
        outputUrl = 'no_output_url'

        print 'Error: {} {}'.format(r.status_code,response['message'])

        try:
            status = response['message']['status']
            print status
        except TypeError:
            status = 'Data request failed: no uFrame status provided'

    return status, outputUrl


def main(sDir, username, token, now):
    fname = 'data_request_urls_%s.csv' %now
    url_file = pd.read_csv(os.path.join(sDir, fname), header=None)
    url_list = url_file[0].tolist()
    cont = raw_input('\nThere are {} requests to send, are you sure you want to continue? y/<n>: '.format(len(url_file))) or 'n'

    if 'y' in cont:
        stime = time.time()
        summary_file = os.path.join(sDir, 'data_request_summary_{}.csv'.format(now))
        with open(summary_file,'a') as summary:
            writer = csv.writer(summary)
            writer.writerow(['status','request_url','outputUrl'])

        for i in range(len(url_list)):
            req = i+1
            url = url_list[i]
            print '\nRequest url {} of {}: {}'.format(req,len(url_list),url)
            session = requests.session()
            r = session.get(url, auth=(username, token))

            while r.status_code == 400:
                print 'Data request failed'
                print 'Status from uFrame: %s' %r.json()['message']['status']
                print 'Trying request again in 1 minute'
                time.sleep(60)
                print 'Re-sending request: %s' %url
                r = session.get(url, auth=(username, token))

            status, outputUrl = define_status_outputUrl(r)

            wformat = '%s,%s,%s\n'
            newline = (status,url,outputUrl)
            with open(summary_file,'a') as summary:
                summary.write(wformat % newline)

            u = len(url_list) - (i+1)
            if u == 0:
                pd.DataFrame(['Attempted to send all requests']).to_csv(os.path.join(sDir,'urls_not_sent_{}.csv'.format(now)), index=False, header=False)
            else:
                urls_left = url_list[-u:]
                pd.DataFrame(urls_left).to_csv(os.path.join(sDir,'urls_not_sent_{}.csv'.format(now)), index=False, header=False)

        etime = time.time() - stime
        if etime < 60:
            print '\nTime elapsed sending data requests: %.2f seconds' % etime
        else:
            mins = etime/60
            print '\nTime elapsed sending data requests: %.2f minutes' % mins

    else:
        print '\nCancelling data requests.'


if __name__ == '__main__':
    sDir = '/Users/lgarzio/Documents/OOI'
    username = 'api_username'
    token = 'api_token'
    now = dt.datetime.now().strftime('%Y%m%dT%H%M')
    main(sDir, username, token, now)