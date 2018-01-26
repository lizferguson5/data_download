#!/usr/bin/env python
"""
Created on Jan 9 2018
@author: lgarzio@marine.rutgers.edu
@brief: This script is used to compare the reference designators, methods and streams in the QC database to those
available in the GUI data catalog ('https://ooinet.oceanobservatories.org/api/uframe/stream'), and build data
request urls (for netCDF files) for the science streams of the instruments input by the user

@usage:
sDir: location to save output
array: optional list of arrays, or an empty list if requesting all (e.g. [] or ['CP','CE'])
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
    annos = '&include_annotations=true'
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

            url = '{:s}/{:s}{:s}/{:s}?beginDT={:s}&endDT={:s}{:s}'.format(base_url, inst_req, method, stream, beginTime, endTime, annos)
            url_list.append(url)
    return url_list


def define_methods(delivery_method):
    valid_inputs = ['streamed', 'telemetered', 'recovered']
    dmethods = []
    if not delivery_method:
        dmethods = ['streamed','telemetered','recovered_host','recovered_inst','recovered_wfp','recovered_cspp','no_method']

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


def filter_dataframe(df, array, subsite, node, inst, dmethods):
    df_filtered = pd.DataFrame()

    if not array:
        dba = df
    else:
        dba = df[df['array_code'].isin(array)]

    if not subsite:
        dbs = dba
    else:
        dbs = dba[dba['subsite'].isin(subsite)]

    if not node:
        dbn = dbs
    else:
        dbn = dbs[dbs['node'].isin(node)]

    if not dmethods:
        dbd = dbn
    else:
        dbd = dbn[dbn['method'].isin(dmethods)]

    if not inst:
        dbi = dbd
        df_filtered = dbi
    else:
        for i in inst:
            dbi = dbd.loc[dbd.sensor.str.contains(i)]
            if dbi.empty:
                continue
            df_filtered = df_filtered.append(dbi,ignore_index=True)

    return df_filtered


def get_database():
    db_inst_stream = pd.read_csv('https://raw.githubusercontent.com/seagrinch/data-team-python/master/infrastructure/data_streams.csv')
    db_stream_desc = pd.read_csv('https://raw.githubusercontent.com/seagrinch/data-team-python/master/infrastructure/stream_descriptions.csv')
    db_regions = pd.read_csv('https://raw.githubusercontent.com/seagrinch/data-team-python/master/infrastructure/regions.csv')

    db_inst_stream = db_inst_stream[['reference_designator','method','stream_name']]
    db_stream_desc = db_stream_desc.rename(columns={'name':'stream_name'})
    db_stream_desc = db_stream_desc[['stream_name','stream_type']]
    db_merged = pd.merge(db_inst_stream,db_stream_desc,on='stream_name',how='outer')
    db_merged['in_qcdb'] = 'yes'
    db_merged = db_merged[db_merged.reference_designator.notnull()]
    db_merged['array_code'] = db_merged['reference_designator'].str[0:2]
    db_regions = db_regions.rename(columns={'reference_designator':'array_code', 'name':'array_name'})
    db = pd.merge(db_merged,db_regions,on='array_code',how='outer')
    db = db.rename(columns={'name':'stream_name'})

    db = db.fillna(value={'method':'no_method'})

    db['subsite'] = db['reference_designator'].str.split('-').str[0]
    db['node'] = db['reference_designator'].str.split('-').str[1]
    db['sensor'] = db['reference_designator'].str.split('-').str[2] + '-' + db['reference_designator'].str.split('-').str[3]

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
        gui_dict_all[i]['array_name'] = response[i]['array_name']
        refdes = response[i]['reference_designator']
        gui_dict_all[i]['array_code'] = refdes[0:2]
        gui_dict_all[i]['reference_designator'] = refdes
        gui_dict_all[i]['subsite'] = refdes.split('-')[0]
        gui_dict_all[i]['node'] = refdes.split('-')[1]
        gui_dict_all[i]['sensor'] = refdes.split('-')[2] + '-' + refdes.split('-')[3]
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


def main(sDir, array, subsite, node, sensor, delivery_methods, begin, end, now):
    if begin > end:
        raise Exception('begin date entered ({:s}) is after end date ({:s})'.format(begin, end))
    else:
        dmethods = define_methods(delivery_methods)

        db = get_database()
        dbf = filter_dataframe(db, array, subsite, node, sensor, dmethods)

        if dbf.empty:
            raise Exception('\nThe selected instruments/delivery_methods are not found in the QC Database.')
        else:
            gui_df_all = gui_stream_list()
            gui_df = filter_dataframe(gui_df_all, array, subsite, node, sensor, dmethods)

            if gui_df.empty:
                dbf['in_gui_catalog'] = ''
                dbf['source'] = 'qcdb_only'
                dbf.to_csv(os.path.join(sDir, 'compare_qcdb_gui_catalog_{}.csv'.format(now)), index=False)
                print '\nQC Database to GUI data catalog comparison complete: %s' %os.path.join(sDir, 'compare_qcdb_gui_catalog_{}.csv'.format(now))
                raise Exception('\nThe selected instruments are not listed in the GUI data catalog. No data requests to send.')
            else:
                merge_on = ['array_name','array_code','subsite','node','sensor','reference_designator','method','stream_name']
                compare = pd.merge(dbf,gui_df,on=merge_on,how='outer').sort_values(by=['reference_designator','method','stream_name'])
                compare_df = define_source(compare)
                compare_df.to_csv(os.path.join(sDir, 'compare_qcdb_gui_catalog_{}.csv'.format(now)), index=False)
                print '\nQC Database to GUI data catalog comparison complete: %s' %os.path.join(sDir, 'compare_qcdb_gui_catalog_{}.csv'.format(now))

                url_list = data_request_urls(compare_df, begin, end)
                urls = pd.DataFrame(url_list)
                urls.to_csv(os.path.join(sDir, 'data_request_urls_{}.csv'.format(now)), index=False, header=False)
                print '\nData request urls complete: %s' %os.path.join(sDir, 'data_request_urls_{}.csv'.format(now))


if __name__ == '__main__':
    sDir = '/Users/lgarzio/Documents/OOI'
    array = [] #['CP','CE']
    subsite = [] #['GI03FLMA','GI03FLMB']
    node = []
    inst = [] #['CTDMO,FLOR']
    delivery_methods = []  #['streamed','telemetered,'recovered']
    begin = ''  #2014-01-01T00:00:00.000Z
    end = ''  #2015-01-01T00:00:00.000Z
    now = dt.datetime.now().strftime('%Y%m%dT%H%M')
    main(sDir, array, subsite, node, inst, delivery_methods, begin, end, now)