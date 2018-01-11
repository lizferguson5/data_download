#!/usr/bin/env python
"""
Created on Jan 9 2018
@author: lgarzio@marine.rutgers.edu
@brief: This script is used to compare the reference designators, methods and streams in the QC database to those
available in the GUI data catalog ('https://ooinet.oceanobservatories.org/api/uframe/stream'), and build data
request urls (for netCDF files) for the science streams of the instruments input by the user

@usage:
sDir: location to save output
subsite: optional list of subsites, or an empty list if requesting all (e.g. [] or ['GI01SUMO','GA01SUMO','GS01SUMO'])
node: optional list of nodes, or an empty list if requesting all (e.g. [] or ['SBD11','SBD12'])
inst: optional list of instruments (can be partial), or an empty list if requesting all (e.g. [] or ['FLOR','CTD'])
delivery_methods: optional list of methods, or an empty list if requesting all (e.g. []  or ['streamed','telemetered','recovered'])
begin: optional start date for data request (e.g. '' or 2014-05-15T00:00:00.000Z)
end: optional end date for data request  (e.g. '' or 2015-01-01T00:00:00.000Z)
"""

import datetime as dt
import os
import requests
import pandas as pd


def data_request_urls(df, begin, end):
    '''
    :return urls for data requests of science streams that are found in the QC database
    '''
    base_url = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv'
    url_list = []
    for i,j in df.iterrows():
        if j['source'] == 'qcdb_and_gui_catalog' and j['stream_type'] == 'Science':

            refdes = j['reference_designator']
            rd = refdes.split('-')
            inst_req = '{:s}/{:s}/{:s}-{:s}/'.format(rd[0],rd[1],rd[2],rd[3])
            method = j['method']
            stream = j['stream_name']

            sys_beginTime = j['beginTime']
            sys_endTime = j['endTime']

            # check times
            if not begin:
                beginTime = sys_beginTime
            else:
                if sys_beginTime < begin < sys_endTime:
                    beginTime = begin
                else:
                    print '{:s}-{:s}-{:s}: begin time entered ({:s}) is not within time ranges available in the system: ' \
                          '{:s} to {:s}'.format(refdes, method, stream, begin, sys_beginTime, sys_endTime)
                    print 'using system beginTime'
                    beginTime = sys_beginTime

            if not end:
                endTime = sys_endTime
            else:
                if end > beginTime:
                    endTime = end
                else:
                    print '{:s}-{:s}-{:s}: end time entered ({:s}) is before beginTime ' \
                          '({:s})'.format(refdes, method, stream, end, sys_beginTime)
                    print 'using system endTime'
                    endTime = sys_endTime

            url = '{:s}/{:s}{:s}/{:s}?beginDT={:s}&endDT={:s}'.format(base_url, inst_req, method, stream, beginTime, endTime)
            url_list.append(url)
    return url_list


def define_methods(delivery_method):
    valid_inputs = ['streamed', 'telemetered', 'recovered']
    dmethods = []
    if not delivery_method:
        dmethods = ['streamed','telemetered','recovered_host','recovered_inst','recovered_wfp','recovered_cspp']

    for d in delivery_method:
        if d not in valid_inputs:
            raise Exception('Invalid delivery_method: %s' %d)
        else:
            if d == 'recovered':
                dmethods.extend(['recovered_host','recovered_inst','recovered_wfp','recovered_cspp'])
            else:
                methods = d
                dmethods.append(methods)
    return dmethods


def define_source(df):
    df['source'] = 'x'
    df['source'][(df['in_qcdb']=='yes') & (df['in_gui_catalog']=='yes')] = 'qcdb_and_gui_catalog'
    df['source'][(df['in_qcdb']=='yes') & (df['in_gui_catalog'].isnull())] = 'qcdb_only'
    df['source'][(df['in_qcdb'].isnull()) & (df['in_gui_catalog']=='yes')] = 'gui_catalog_only'
    return df


def filter_dataframe(df, subsite, node, inst, dmethods):
    df_filtered = pd.DataFrame()

    df['subsite'] = df['reference_designator'].str.split('-').str[0]
    df['node'] = df['reference_designator'].str.split('-').str[1]
    df['sensor'] = df['reference_designator'].str.split('-').str[2] + '-' + df['reference_designator'].str.split('-').str[3]

    if not subsite:
        subsite = df['subsite'].unique().tolist()  # get all subsites
        subsite.sort()
    for s in subsite:
        dbs = df.loc[df.reference_designator.str.contains(s)]
        if dbs.empty:
            continue

        if not node:
            node = dbs['node'].unique().tolist() # get all nodes for above selected subsites
            node.sort()
        for n in node:
            dbn = dbs.loc[dbs.reference_designator.str.contains(n)]
            if dbn.empty:
                continue

            if not inst:
                inst = dbn['sensor'].unique().tolist() # get all sensors for above selected subsites/nodes
                inst.sort()
            for i in inst:
                dbi = dbn.loc[dbn.reference_designator.str.contains(i)]
                if dbi.empty:
                    continue

                for d in dmethods:
                    dbd = dbi.loc[dbi.method == d]
                    if dbd.empty:
                        continue
                    df_filtered = df_filtered.append(dbd,ignore_index=True)

    return df_filtered


def get_database():
    '''
    :return dataframe containing the science streams from the Data Team Database for the reference designator(s) and
    delivery method(s) of interest
    '''
    db_inst_stream = pd.read_csv('https://raw.githubusercontent.com/seagrinch/data-team-python/master/infrastructure/data_streams.csv')
    db_stream_desc = pd.read_csv('https://raw.githubusercontent.com/seagrinch/data-team-python/master/infrastructure/stream_descriptions.csv')

    db_inst_stream = db_inst_stream[['reference_designator','method','stream_name']]
    db_stream_desc = db_stream_desc.rename(columns={'name':'stream_name'})
    db_stream_desc = db_stream_desc[['stream_name','stream_type']]
    db = pd.merge(db_inst_stream,db_stream_desc,on='stream_name',how='outer')
    db['in_qcdb'] = 'yes'
    db = db[db.reference_designator.notnull()]

    return db


def gui_stream_list():
    r = requests.get('https://ooinet.oceanobservatories.org/api/uframe/stream')
    response = r.json()['streams']

    gui_dict_all = {}
    for i in range(len(response)):
        try:
            method = response[i]['stream_method'].replace('-','_')
        except AttributeError:  # skip if there is no method defined
            method = 'na'

        if not response[i]['stream']:
            stream = 'no_stream'
        else:
            stream = response[i]['stream']

        gui_dict_all[i] = {}
        gui_dict_all[i]['reference_designator'] = response[i]['reference_designator']
        gui_dict_all[i]['method'] = method
        gui_dict_all[i]['stream_name'] = stream
        gui_dict_all[i]['beginTime'] = response[i]['start']
        gui_dict_all[i]['endTime'] = response[i]['end']

    gui_df_all = pd.DataFrame.from_dict(gui_dict_all,orient='index')
    gui_df_all['in_gui_catalog'] = 'yes'

    return gui_df_all


def inst_format(refdes):
    rd = refdes.split('-')
    inst_format = '{:s}/{:s}/{:s}-{:s}/'.format(rd[0],rd[1],rd[2],rd[3])
    return inst_format


def main(sDir, subsite, node, sensor, delivery_methods, begin, end, now):
    if begin > end:
        raise Exception('begin date entered ({:s}) is after end date ({:s})'.format(begin, end))
    else:
        #valid_methods = ['streamed','telemetered','recovered_host','recovered_inst','recovered_wfp','recovered_cspp']

        dmethods = define_methods(delivery_methods)

        db = get_database()
        dbf = filter_dataframe(db, subsite, node, sensor, dmethods)

        if dbf.empty:
            raise Exception('\nThe selected instruments/delivery_methods are not found in the QC Database.')
        else:
            gui_df_all = gui_stream_list()
            gui_df = filter_dataframe(gui_df_all, subsite, node, sensor, dmethods)

            if gui_df.empty:
                dbf['in_gui_catalog'] = ''
                dbf['source'] = 'qcdb_only'
                dbf_fname = 'compare_qcdb_gui_catalog_%s.csv' %now
                dbf.to_csv(os.path.join(sDir, dbf_fname), index=False)
                print '\nQC Database to GUI data catalog comparison complete: %s' %os.path.join(sDir, dbf_fname)
                raise Exception('\nThe selected instruments are not listed in the GUI data catalog. No data requests to send.')
            else:
                merge_on = ['subsite','node','sensor','reference_designator','method','stream_name']
                compare = pd.merge(dbf,gui_df,on=merge_on,how='outer').sort_values(by=['reference_designator','method','stream_name'])
                compare_df = define_source(compare)
                compare_fname = 'compare_qcdb_gui_catalog_%s.csv' %now
                compare_df.to_csv(os.path.join(sDir, compare_fname), index=False)
                print '\nQC Database to GUI data catalog comparison complete: %s' %os.path.join(sDir, compare_fname)

                print '\nGetting data request urls'
                url_list = data_request_urls(compare_df, begin, end)
                urls = pd.DataFrame(url_list)
                url_fname = 'data_request_urls_%s.csv' %now
                urls.to_csv(os.path.join(sDir, url_fname), index=False, header=False)
                print '\nData request urls complete: %s' %os.path.join(sDir, url_fname)


if __name__ == '__main__':
    sDir = '/Users/lgarzio/Documents/OOI'
    subsite = [] #['GI03FLMA,GI03FLMB']
    node = []
    inst = [] #['CTDMO,FLOR']
    delivery_methods = []  #['streamed','telemetered,'recovered']
    begin = ''  #2014-01-01T00:00:00.000Z
    end = ''  #2015-01-01T00:00:00.000Z
    now = dt.datetime.now().strftime('%Y%m%dT%H%M')
    main(sDir, subsite, node, inst, delivery_methods, begin, end, now)