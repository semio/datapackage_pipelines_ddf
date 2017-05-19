# -*- coding: utf-8 -*-

# process: add_dataset
# load a dataset into the pipeline
# parameters:
# - run: ddf.add_dataset
#   parameters:
#       datapackage: $path_to_datapackage_json


import os.path as osp
import json
from datapackage_pipelines.wrapper import spew, ingest
from itertools import islice
import pandas as pd
# import logging

parameters, datapackage, resource_iterator = ingest()

dp = parameters['datapackage']
basedir = parameters['basedir']

if osp.isdir(osp.join(basedir, dp)):
    source_dp_path = osp.join(basedir, dp)
    dp = json.load(open(osp.join(basedir, dp, 'datapackage.json')))
else:
    source_dp_path = osp.dirname(osp.join(basedir, dp))
    dp = json.load(open(osp.join(basedir, dp)))

if datapackage is None:
    datapackage = dp
else:
    datapackage.update(dp)

new_resource_iterator = []

for r in datapackage['resources']:
    df = pd.read_csv(osp.join(source_dp_path, r['path']))
    # adding type in datapackage, because pipeline will check
    # if data match the type descriptor.
    for c in df.columns:
        for i, f in enumerate(r['schema']['fields']):
            if f['name'] == c:
                idx = i
                break
        if df.dtypes[c] == 'object':
            r['schema']['fields'][idx]['type'] = 'string'
        elif c == 'year':
            r['schema']['fields'][idx]['type'] = 'integer'
        else:
            r['schema']['fields'][idx]['type'] = 'number'
    new_resource_iterator.append(islice(df.to_dict('records'), None))

# logging.info(datapackage)
# logging.info(source_dp_path)

spew(datapackage, new_resource_iterator)
