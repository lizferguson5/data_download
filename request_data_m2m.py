#!/usr/bin/env python
"""
Created on Jan 11 2018
@author: lgarzio@marine.rutgers.edu
@brief: This script imports tools that compare the QC Database to the OOI GUI data catalog, builds netCDF data request
urls, and sends those requests (if prompted).

@usage:
sDir: directory where outputs are saved
username: OOI API username
token: OOI API password
"""

import pandas as pd
import datetime as dt
from tools import get_data_request_urls, send_data_requests_nc, data_request_tools

sDir = '/Users/lgarzio/Documents/OOI'
username = 'api_username'
token = 'api_token'

now = dt.datetime.now().strftime('%Y%m%dT%H%M')
qcdb = get_data_request_urls.get_database()
subsite_list = qcdb['reference_designator'].str.split('-').str[0].unique().tolist()
subsite_list.sort()
refdes_list = qcdb['reference_designator'].unique().tolist()
refdes_list.sort()

inst_list = qcdb['reference_designator'].str.split('-').str[3].str[0:5].unique().tolist()
inst_list.sort()
print 'These subsites are listed in the QC Database:'
print subsite_list

subsites = raw_input('\nPlease select fully-qualified subsites. Must be comma separated (if choosing multiple) or press enter to select all: ') or ''
subsite = data_request_tools.format_inputs(subsites)

if len(subsite) == 0:
    subsite = ''
    node_list = qcdb['reference_designator'].str.split('-').str[1].unique().tolist()
    node_list.sort()
    refdes_filtered_bysubsite = pd.DataFrame(refdes_list)
else:
    for s in subsite:
        if s not in subsite_list:
            raise Exception('Selected subsite (%s) not found in QC Database. Please choose from available subsites.' %s)
    refdes_filtered_bysubsite = data_request_tools.filter_refdes(refdes_list, subsite)
    node_list = refdes_filtered_bysubsite[0].str.split('-').str[1].unique().tolist()
    node_list.sort()

print '\nThese nodes are listed in the QC Database for the selected subsite(s):'
print node_list

nodes = raw_input('\nPlease select fully-qualified nodes. Must be comma separated (if choosing multiple) or press enter to select all: ') or ''
node = data_request_tools.format_inputs(nodes)

if len(node) == 0:
    node = ''
    inst_list = refdes_filtered_bysubsite[0].str.split('-').str[3].str[0:5].unique().tolist()
    inst_list.sort()
else:
    for n in node:
        if n not in node_list:
            raise Exception('Selected node (%s) not found in QC Database. Please choose from available nodes.' %n)
    refdes_filtered_bynode = data_request_tools.filter_refdes(refdes_filtered_bysubsite[0].tolist(), node)
    inst_list = refdes_filtered_bynode[0].str.split('-').str[3].str[0:5].unique().tolist()
    inst_list.sort()

print '\nThese instruments are listed in the QC Database for the selected subsite(s) and node(s):'
print inst_list

insts = raw_input('\nPlease select instruments (can be partial, e.g. CTD). Must be comma separated (if choosing multiple) or press enter to select all: ') or ''
inst = data_request_tools.format_inputs(insts)

if len(inst) == 0:
    inst = ''

delivery_meths = raw_input('\nPlease select valid delivery methods [recovered, telemetered, streamed]. Must be comma separated (if choosing multiple) or press enter to select all: ') or ''
delivery_methods = data_request_tools.format_inputs(delivery_meths)

if len(delivery_methods) == 0:
    delivery_methods = ''
else:
    for d in delivery_methods:
        if d not in ['recovered', 'telemetered', 'streamed']:
            raise Exception('Selected delivery_method is not valid. Please choose from valid delivery methods.' %d)

begin = raw_input('Please enter a start date for your data requests with format: 2014-01-01T00:00:00.000Z or press enter to request all available data: ') or ''
end = raw_input('Please enter an end date for your data requests with format: 2014-01-01T00:00:00.000Z or press enter to request all available data: ') or ''

get_data_request_urls.main(sDir, subsite, node, inst, delivery_methods, begin, end, now)
send_data_requests_nc.main(sDir, username, token, now)