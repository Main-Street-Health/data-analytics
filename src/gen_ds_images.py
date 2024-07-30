#!/usr/bin/env python
# coding: utf-8

from time import time
from pathlib import Path
import warnings
import argparse
import multiprocessing
from tqdm import tqdm
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns

import cb_utils
import graphing
import image_upload

# configuration
sns.set(style="darkgrid")
pd.options.display.max_columns = 500
warnings.filterwarnings("ignore")


def pull_tall_wide(ds_batch_id: int = None, mco_id: int = None, use_cache=False):
    if ds_batch_id is None:
        if mco_id is None:
            raise ValueError("ds_batch_id or mco_id is required. Both cannot be None")
        query = f"SELECT dtw.* FROM cb.ds_tall_wide dtw JOIN cb.mcos m ON m.id = {mco_id} AND m.ds_batch_id = dtw.ds_batch_id;"
    else:
        query = f"SELECT dtw.* FROM cb.ds_tall_wide dtw JOIN cb.mcos m ON m.id = 1 AND m.ds_batch_id = dtw.ds_batch_id and m.ds_batch_id = {ds_batch_id};"

    ds_tall_wide = cb_utils.sql_query_to_df(query, use_cache=use_cache)
    ds_tall_wide = ds_tall_wide.drop(columns=['created_at'])
    ds_tall_wide.transplant_ddos = [0 if r is None else r for r in ds_tall_wide.transplant_ddos]
    ds_tall_wide.fillna(0, inplace=True)
    return ds_tall_wide


def pull_vap(ds_batch_id: int = None, mco_id: int = None, use_cache=False):
    if ds_batch_id is None:
        if mco_id is None:
            raise ValueError("ds_batch_id or mco_id is required. Both cannot be None")
        query = f"SELECT v.* FROM cb.ds_vaps v  JOIN cb.mcos m ON m.id = {mco_id} AND m.ds_batch_id = v.ds_batch_id;"
    else:
        query = f"SELECT v.* FROM cb.ds_vaps v WHERE v.ds_batch_id = {ds_batch_id};"
    vap = cb_utils.sql_query_to_df(query, use_cache=use_cache)
    vap.fillna(0, inplace=True)
    return vap


def pull_auths(ds_batch_id: int = None, mco_id: int = None, use_cache=False):
    query = f"""
    SELECT
        dapu.member_id
      , dapu.period_start              period_end
      , dapu.period_end                period_start
      , dapu.visit_util_attd_pcs * 100 utilized_percentage
      , dapu.visit_per_wk              utilized_hours_per_week
      , dapu.auth_per_wk               total_hours_per_week
      , CASE
            WHEN dapu.visit_util_attd IS NOT NULL AND dapu.visit_util_pcs IS NOT NULL THEN 'S5125, T1019'
            WHEN dapu.visit_util_attd IS NOT NULL                                     THEN 'S5125'
            WHEN dapu.visit_util_pcs IS NOT NULL                                      THEN 'T1019'
            END                        service_codes
    FROM
        cb.members m
        JOIN cb.mcos mco ON m.mco_id = mco.id
        JOIN cb.ds_auth_periods_utilization dapu ON m.id = dapu.member_id AND dapu.ds_batch_id = mco.ds_batch_id
    """

    if ds_batch_id is None:
        if mco_id is None:
            raise ValueError("ds_batch_id or mco_id is required. Both cannot be None")

        query += f'WHERE m.mco_id = {mco_id};'
    else:
        query += f'WHERE dapu.ds_batch_id = {ds_batch_id};'

    auth_util = cb_utils.sql_query_to_df(query, use_cache=use_cache)
    auth_util.service_codes = auth_util.service_codes.fillna('')
    auth_util.fillna(0, inplace=True)
    auth_util = auth_util.assign(label=auth_util.service_codes + ': ' + auth_util.period_start.astype(str) + ' to ' + auth_util.period_end.astype(
        str) + ' ' + auth_util.utilized_percentage.astype(str))

    return auth_util


def melt_tall_wide(ds_tall_wide):
    tmelt = ds_tall_wide.melt(id_vars=['member_id', 'period', 'forward_period', 'bom'])
    tmelt = tmelt.rename(columns={'bom': 'Month'})
    tmelt.Month = tmelt.Month.astype(str)
    tmelt.fillna(0, inplace=True)
    return tmelt


def build_title_string(member):
    member_vals = member[['age_yr', 'grp_yr', 'grp_end_yr', 'grp_2_days_yr', 'grp_3_days_yr', 'member_id']].values[0]
    [age_yr, grp, grp_end, grp_2_days, grp_3_days, member_id] = member_vals
    title = f'MemberID: ({member_id}) Age:({age_yr}) Group:({grp}), @end:({grp_end}). Group Days two:({grp_2_days}), three:({grp_3_days})'

    # if member has any care $$ add it to the title
    if member[
        ['care_cls_pmpm_yr', 'care_aclf_pmpm_yr', 'care_alfgc_pmpm_yr', 'care_adult_day_pmpm_yr']].sum().sum() > 0:
        care_vals = member[
            ['care_cls_pmpm_yr', 'care_aclf_pmpm_yr', 'care_alfgc_pmpm_yr', 'care_adult_day_pmpm_yr']].sum()
        care_cls_pmpm, care_aclf_pmpm, care_alfgc_pmpm, care_adult_day_pmpm = care_vals
        title += f'\nCare PMPMs CLS(${care_cls_pmpm}), ACLF(${care_aclf_pmpm}), ALFGC(${care_alfgc_pmpm}), Adult Day(${care_adult_day_pmpm})'

    return title


def build_hours_line_graph_data(melted_member):
    hrs_cols = [
        'appropriate_hrs',
        'auth_attd_pcs_hrs',
        'attd_pcs_visit_hrs',
        'pcs_visit_hrs',
        'auth_pcs_hrs',
        'auth_attd_hrs',
        'attd_visit_hrs',
    ]
    lines_df = melted_member[melted_member.variable.isin(hrs_cols)].query("variable in @hrs_cols")
    lines_df = lines_df.rename(columns={"value": "Hours", "variable": 'Type of Hours'})
    lines_df['Type of Hours'] = lines_df['Type of Hours'].map({
        'appropriate_hrs': 'Adjusted Hrs',
        'auth_attd_pcs_hrs': 'Auth Hrs',
        'attd_pcs_visit_hrs': 'Visit Hrs',
        'pcs_visit_hrs': 'PCS Visit',
        'auth_pcs_hrs': 'PCS Auth',
        'auth_attd_hrs': 'Attd Auth',
        'attd_visit_hrs': 'Attd Visit',
    })

    lines_df.Hours = lines_df.Hours.astype(int)
    return lines_df


def build_spend_bar_graph_data(melted_member):
    bars_df = melted_member.query("variable not in ['auth_attd_pcs_hrs', 'attd_pcs_visit_hrs']")
    bars_df = bars_df.rename(columns={"value": "Days of Service", 'variable': 'Event'})
    bars_df.Event = bars_df.Event.map({
        'ed_ddos': 'ED',
        'ip_ddos': 'IP',
        'nf_ddos': 'NF',
        #         'snf_ddos': 'SNF',
        #         'icf_ddos': 'ICF',
        'hh_ddos': 'HH',
        'pro_ddos': 'Pro',
        'out_ddos': 'Out',
        'hcbs_respite_ddos': 'HCBS Respite',
        'fall_ddos': 'Falls'
    })
    return bars_df


def graph_member(args):
    member, melted_member, member_visits, member_auths, save, save_path = args

    member_id = member.member_id.values[0]
    if not save:
        print('Graphing: ', member_id)

    title = build_title_string(member)

    lines_df = build_hours_line_graph_data(melted_member)
    bars_df = build_spend_bar_graph_data(melted_member)

    fig = plt.figure(tight_layout=True, figsize=(18, 24))
    gs = gridspec.GridSpec(4, 3)  # 4 rows, 3 columns, graphs can take up 1, 2 or 3 columns

    #####
    # Hrs
    #####
    ax = fig.add_subplot(gs[0, :])
    graphing.plot_hours_line_graph(ax, title, lines_df)

    #####
    # DDOS Spend Type
    #####
    ax2 = ax.twinx()
    graphing.plot_spend_type_bar_graph(ax2, bars_df)

    #####
    # DX DDOS
    #####
    ax = fig.add_subplot(gs[1, :])
    graphing.plot_dx_ddos_heatmap(ax, member)

    #####
    # PMPMs
    #####
    ax = fig.add_subplot(gs[3, :])
    graphing.plot_pmpm_line_graph(ax, melted_member)

    #####
    # Schedule
    #####
    ax = fig.add_subplot(gs[2, 0])
    graphing.plot_visit_minutes_heatmap(ax, member_visits)

    ax = fig.add_subplot(gs[2, 1])
    graphing.plot_missed_minutes_heatmap(ax, member_visits)

    #######
    # Auths
    #######
    ax = fig.add_subplot(gs[2, 2])
    graphing.plot_auths(ax, member_auths)

    if save:
        Path(f'outputs/{save_path}/large').mkdir(parents=True, exist_ok=True)
        plt.savefig(f'outputs/{save_path}/large/{member_id}_ds.png')
        plt.close(fig)


def graph_member_small_hrs(args):
    melted_member, save_path, save = args

    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(16, 8))
    graphing.plot_member_small(ax, melted_member)

    if save:
        Path(f'outputs/{save_path}/small').mkdir(parents=True, exist_ok=True)
        plt.savefig(f'outputs/{save_path}/small/{melted_member.member_id.iloc[0]}_small.png', bbox_inches='tight',
                    pad_inches=0)
        plt.close(fig)


def graph_member_small_pmpms(args):
    melted_member, save_path, save = args

    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(16, 8))
    graphing.plot_member_small_pmpms(ax, melted_member)

    if save:
        Path(f'outputs/{save_path}/small_pmpms').mkdir(parents=True, exist_ok=True)
        plt.savefig(f'outputs/{save_path}/small_pmpms/{melted_member.member_id.iloc[0]}_small.png', bbox_inches='tight',
                    pad_inches=0)
        plt.close(fig)


def graph_all_members(ds_batch_id=None, mco_id=None, save_path='', member_ids=None,  run_small_pmpms=True, run_small_hrs=True, run_big=True, use_cache=False):
    ds_tall_wide = pull_tall_wide(ds_batch_id=ds_batch_id, mco_id=mco_id, use_cache=use_cache)
    vap = pull_vap(ds_batch_id=ds_batch_id, mco_id=mco_id, use_cache=use_cache)
    auths = pull_auths(ds_batch_id=ds_batch_id, mco_id=mco_id, use_cache=use_cache)
    tmelt = melt_tall_wide(ds_tall_wide)
    tmelt.sort_values(['member_id', 'Month'], inplace=True)

    if member_ids is None:
        member_ids = ds_tall_wide.member_id.unique()

    small = []
    big = []
    n_members = len(member_ids)
    for member_id in tqdm(member_ids, desc='Building Data'):
        member = ds_tall_wide.loc[ds_tall_wide.member_id == member_id]
        melted_member = tmelt.query("member_id == @member_id")
        member_visits = vap.query('member_id == @member_id and procedure_code == "all"')
        member_visits = member_visits.replace(0, np.nan)
        member_auths = auths.query("member_id == @member_id")
        mem = (member, melted_member, member_visits, member_auths, True, save_path)
        if run_big:
            big.append(mem)
        if run_small_hrs or run_small_pmpms:
            small.append((melted_member, save_path, True))

    if run_small_hrs:
        with multiprocessing.Pool() as p:
            with tqdm(total=n_members, desc=f'Generating Small Images') as pbar:
                for i, _ in enumerate(p.imap_unordered(graph_member_small_hrs, small)):
                    pbar.update()

    if run_small_pmpms:
        with multiprocessing.Pool() as p:
            with tqdm(total=n_members, desc=f'Generating Small Images') as pbar:
                for i, _ in enumerate(p.imap_unordered(graph_member_small_pmpms, small)):
                    pbar.update()

    with multiprocessing.Pool() as p:
        with tqdm(total=n_members, desc=f'Generating Big Images') as pbar:
            for i, _ in enumerate(p.imap_unordered(graph_member, big)):
                pbar.update()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate DS graphs for members and optionally upload to the DB")

    parser.add_argument('--mco_id', type=int, help='MCO ID', required=False)
    parser.add_argument('--ds_batch_id', type=int, help='DS Batch ID', required=False)
    parser.add_argument('--member_ids', nargs="*", help='Limit to Member IDs')
    parser.add_argument('--small_hrs', help='Generate only small', action='store_true')
    parser.add_argument('--small_pmpms', help='Generate only small', action='store_true')
    parser.add_argument('--big', help='Generate only big', action='store_true')
    parser.add_argument('--use_cache', help='Use Cached Data', action='store_true')
    parser.add_argument('--output_path', help='Where to save images ("outputs/" is auto prepended)', default=False)
    parser.add_argument('--upload', help='Upload images to DB', action='store_true')

    args = parser.parse_args()
    big = True
    small_hrs = True
    small_pmpms = True

    if args.big or args.small_hrs or args.small_pmpms:
        big = args.big
        small_hrs = args.small_hrs
        small_pmpms = args.small_pmpms

    s = time()
    graph_all_members(
        ds_batch_id=args.ds_batch_id,
        mco_id=args.mco_id,
        save_path=args.output_path,
        member_ids=args.member_ids,
        run_small_hrs=small_hrs,
        run_small_pmpms=small_pmpms,
        run_big=big,
        use_cache=args.use_cache
    )
    e = time()
    print(f'Finished generating images')
    cb_utils.printt(e - s)

    if args.upload:
        print('Starting to update db images')
        s = time()
        image_upload.upload_images(args.output_path)
        e = time()
        print(f'Finished uploading images')
        cb_utils.printt(e - s)

