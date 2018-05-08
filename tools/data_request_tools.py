#!/usr/bin/env python
"""
Created on Jan 11 2018
@author: Lori Garzio
"""

import itertools
import pandas as pd


def format_inputs(input_str):
    if len(input_str) == 0:
        formatted_input = []
    elif ',' in input_str:
        input_str = input_str.replace(" ","") #remove any whitespace
        formatted_input = input_str.split(',')
    else:
        formatted_input = [input_str]

    return formatted_input


def filter_refdes(refdes_list, filter_by):
    alist = []
    for item in filter_by:
        filter = [x for x in refdes_list if item in x]
        alist.append(filter)
    flist = list(itertools.chain(*alist))
    db = pd.DataFrame(flist)
    return db