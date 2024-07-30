#!/usr/bin/env python
# coding: utf-8


import os
import io
from io import StringIO  # python3; python2: BytesIO

import numpy as np
import pandas as pd
import boto3
pre_months = 12
post_months = 12

version = 'v12'


# role = sagemaker.get_execution_role()
# bucket_name = 'cb-analytics-us-east-2-prd'
# prefix = f'sagemaker/{version}'
# file_name = f'member_periods_{version}.parquet'
# obj = boto3.Session().resource('s3').Bucket(bucket_name).Object(os.path.join(prefix, file_name)).get()
# member_periods = pd.read_parquet(io.BytesIO(obj['Body'].read()))


member_periods = pd.read_parquet('./notebooks/data/member_periods_v12.parquet')


months = sorted(member_periods.eom.unique())
n_months = len(months)
last_valid_pre_start = n_months - pre_months - post_months - 1  # 42
print('last valid_pre_start', last_valid_pre_start)
months_df = pd.DataFrame(months, columns=['eom'])


target_cols = ['ip_tc', 'er_tc', 'snf_tc', 'amb_tc']
tc_cols = [c for c in member_periods.columns if '_tc' in c]
ddos_cols = [c for c in member_periods.columns if '_ddos' in c]
top_level_feats = ['age', 'is_male', 'is_female', 'state', 'ggroup', 'line_of_business_id']

all_ddos_tc_cols_wide = [f'{c}_{i}' for i in range(pre_months) for c in ddos_cols + tc_cols]

# not currently used
mcos = member_periods.mco_name.unique().tolist()
mco_cols = [f'is_{m.lower().replace(" ", "_")}' for m in mcos]
n_mcos = len(mcos)
def encode_mco(mco_str):
    one_hot = np.zeros(n_mcos, dtype=int)
    one_hot[mcos.index(mco_str)] = 1
    return one_hot

lobs = member_periods.line_of_business_id.unique().tolist()
lob_cols = [f'is_lob_{l}' for l in lobs]
n_lobs = len(lobs)
def encode_lob(lob):
    one_hot = np.zeros(n_lobs, dtype=int)
    one_hot[lobs.index(lob)] = 1
    return one_hot

groups = member_periods.ggroup.unique().tolist()
group_cols = [f'is_group_{l}' for l in groups]
n_groups = len(groups)
def encode_group(group):
    one_hot = np.zeros(n_groups, dtype=int)
    one_hot[groups.index(group)] = 1
    return one_hot

states = sorted(member_periods.state.unique().tolist())
state_cols = [f'is_state_{l}' for l in states]
n_states = len(states)
def encode_state(state):
    one_hot = np.zeros(n_states, dtype=int)
    one_hot[states.index(state)] = 1
    return one_hot

def build_member_features(mdf, months_range):
    # mdf = member_periods.loc[(member_periods.pre_0) & (member_periods.pre_full_0) & (member_periods.member_id == 102)].sort_values('eom')
    if len(mdf) == 0:
        return mdf

    demographic_data = mdf[top_level_feats + ['member_id']].iloc[-1]

    mdf = months_range.merge(mdf, on='eom', how='left')
    mdf = mdf.sort_values('eom')[ddos_cols + tc_cols]
    mdf = mdf.fillna(0)

    ddos_tc_data = mdf.to_numpy().reshape([1, -1])

    state_data = encode_state(demographic_data.state)
    lob_data = encode_lob(demographic_data.line_of_business_id)
    group_data = encode_group(demographic_data.ggroup)
    data = np.concatenate((ddos_tc_data[0], state_data, lob_data, group_data, np.array([demographic_data.is_male, demographic_data.is_female, demographic_data.age, demographic_data.member_id])), axis=0)
    cols = all_ddos_tc_cols_wide + state_cols + lob_cols + group_cols + ['is_male', 'is_female', 'age', 'member_id']

    return pd.DataFrame([data], columns=cols)

def build_member_targets(mdf):
    if len(mdf) == 0:
        return pd.DataFrame([], columns=['member_id', 'target'])
    tc = mdf[target_cols].sum().sum()
    #     pmpm = tc / mdf.cpmm.sum()
    return pd.DataFrame([[mdf.iloc[0].member_id, tc]], columns=['member_id', 'target'])


def build_targets(post_df):
    return post_df.groupby('member_id', as_index=False).apply(build_member_targets)


def build_features(pre_df, months_range):
    return pre_df.groupby('member_id', as_index=False).apply(lambda x: build_member_features(x, months_range))

from multiprocessing.pool import Pool
# build features and targets for each period
period_dfs = []


def build_period_df(i):
    print('Building period: ', i)
    elg = member_periods.loc[member_periods[f'pre_post_elg_{i}']]
    pre = elg.loc[elg[f'pre_{i}']]

    post = elg.loc[elg[f'post_{i}']]
    x = build_features(pre, months_df.loc[i:i+11])
    # if i < 42:
    y = build_targets(post)
    final = x.merge(y, how='left', left_on='member_id', right_on='member_id').assign(period=i)
    # else:
    # final = x.assign(period=i)
    # period_dfs.append(final)
    final.to_parquet(f'./data/final_wide_df_{version}_{i}.parquet')

    return final

if __name__ == '__main__':
    with Pool(processes=28) as p:
        period_dfs = p.map(build_period_df, range(last_valid_pre_start))
        print('finished map')
        master_df = pd.concat(period_dfs)
        print('finished concat')
        master_df.to_parquet(f'./data/final_wide_df_{version}_all.parquet')
        print('finished local parquet write')

#         bucket = 'cb-analytics-exports-us-east-2-prd' # already created on S3
#         csv_buffer = StringIO()
#         master_df.to_csv(csv_buffer)
#         s3_resource = boto3.resource('s3')
#         s3_resource.Object(bucket, f'master_wide_df_{version}.csv').put(Body=csv_buffer.getvalue())

#         print('mission complete')
