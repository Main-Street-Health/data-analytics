import re
import os
from pathlib import Path
import datetime
import json
import psycopg2
import pymysql
import pyarrow
from sqlalchemy import create_engine
import pandas as pd
import hashlib
from joblib import dump, load
import numpy as np
import subprocess
import boto3



CACHE_DIR = '../cache'
MODEL_DIR = '../models'


def upload_file_to_s3(file_name, bucket, object_name):
    secrets = get_secrets('secrets.json')
    client = boto3.client(
    's3',
        aws_access_key_id=secrets['aws_access_key_id'],
        region_name='us-east-2',
        aws_secret_access_key=secrets['aws_secret_access_key']
    )
    client.upload_file(
        file_name,
        bucket,
        object_name,
        ExtraArgs={'ServerSideEncryption':'AES256'},
    )



def printt(duration):
    print(f'Elapsed Time: {duration:.2f}s')


def df_col_format_icd10(df, col):
    df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.lower().str.strip()
    return df


def df_format_columns(df):
    df.columns = [re.sub('\(|\)| |\/', '_', c.strip()).lower() for c in df.columns]
    return df


def df_add_inserted_at_col(df):
    return df.assign(inserted_at=datetime.datetime.now())

def df_trim_all_str_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)


def get_conn(connection_string=None):
    connection_string = get_connection_string() if connection_string is None else connection_string
    return psycopg2.connect(connection_string)


def get_member_doc_conn(secrets=None):
    secrets = get_secrets('secrets.json') if secrets is None else secrets
    conn_str = secrets['member_doc']
    conn = get_conn(connection_string=conn_str)
    return conn


def get_secrets(path='secrets.json'):
    i = 0
    while i < 3:
        try:
            with open('../' * i + path) as f:
                secrets = json.load(f)
            break
        except FileNotFoundError:
            i += 1
            continue

    return secrets


def get_connection_string(secrets=None):
    secrets = get_secrets() if secrets is None else secrets
    return secrets['db_connection_string']


def get_db_url(secrets=None, source='analytics'):
    secrets = get_secrets() if secrets is None else secrets
    return secrets[source + '_db_url']


def get_engine(connection_string=None, source='analytics'):
    connection_string = get_db_url(source=source) if connection_string is None else connection_string
    if source[:9] == 'postgres':
        return create_engine(
            connection_string,
            executemany_mode='values',
            executemany_values_page_size=10000,
            executemany_batch_page_size=500
        )
    return create_engine(connection_string)


def get_table(table_name, use_cache=True, schema='cb', source='analytics'):
    Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
    file_name = f'{CACHE_DIR}/{source}_{table_name}.parquet'
    try:
        if not use_cache:
            raise Exception("Pulling fresh")

        df = pd.read_parquet(file_name)
        print(f'Pulling {table_name} from cache')

    except:
        print(f'Pulling {table_name} from db')
        df = pd.read_sql_table(table_name, get_engine(source=source), schema=schema)
        df.to_parquet(file_name, allow_truncated_timestamps=True)

    return df


def hash_query(query):
    m = hashlib.sha1()
    m.update(bytes(query, 'utf8'))
    return m.hexdigest()


def sql_query_to_df(query, use_cache=True, source='analytics', verbose=True):
    Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
    query_hash = hash_query(query+source)
    file_name = f'{CACHE_DIR}/{source}_{query_hash}.parquet'
    try:
        if not use_cache:
            raise Exception("Pulling fresh")

        df = pd.read_parquet(file_name)
        if verbose:
            print(f'Pulled query from cache')

    except:
        if verbose:
            print(f'Pulling query from db')
        df = pd.read_sql(query, get_engine(source=source))
#         df.to_parquet(file_name, allow_truncated_timestamps=True)

    return df


def merge_member_month_dfs(*dfs):
    df = dfs[0]

    for d in dfs[1:]:
        df = df.merge(d, how='left', on=['member_id', 'bom'])

    assert dfs[0].shape[0] == df.shape[0]
    return df


def publish_model(model, name, description, family, meta_data, inserted_by=None ):
    conn = get_conn()
    query = """
    INSERT
    INTO
        cb.models (name, description, family, inserted_by)
    VALUES
        (%s,%s,%s,%s)
    RETURNING id;
    """
    with conn.cursor() as cur:
        cur.execute(query, [ name, description, family, inserted_by])
        result = cur.fetchone()
    conn.commit()
    conn.close()

    model_id = result[0]

    save_model(model, name, meta_data)

    command = "aws-vault exec cb-stg -- aws s3 sync --sse AES256 ./models/ s3://cb-analytics-us-east-2-stg/models/"
    subprocess.run(command, shell=True, check=True)
    return model_id



def save_model(model, name, meta_data={}):
    Path(MODEL_DIR).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    file_name = f'{timestamp}_{name}'
    dump(model, f'{MODEL_DIR}/{file_name}.model')

    if meta_data:
        with open(f'{MODEL_DIR}/{file_name}.json', 'w') as f:
            json.dump(meta_data, f)

    return file_name


def load_model(name):
    meta = {}

    if os.path.isfile(f'{MODEL_DIR}/{name}.json'):
        with open(f'{MODEL_DIR}/{name}.json', 'r') as f:
            meta = json.load(f)

    return load(f'{MODEL_DIR}/{name}.model'), meta


def create_scoring_run(mab_id, model_id, description=None, inserted_by='API'):
    conn = get_conn()
    query = """
    INSERT INTO cb.scoring_runs(mab_id, model_id, description, started_at, inserted_by)
    VALUES (%s, %s, %s, now(), %s)
    RETURNING id;
    """
    with conn.cursor() as cur:
        cur.execute(query, [mab_id, model_id, description, inserted_by])
        result = cur.fetchone()
    conn.commit()
    conn.close()
    return result[0]


def complete_scoring_run(is_successful, scoring_run_id):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE cb.scoring_runs SET completed_at = now(), is_complete = %s WHERE id = %s;",
            [is_successful, scoring_run_id]
        )
    conn.commit()
    conn.close()


def save_scores(scores, scoring_run_id):
    conn = get_conn()
    placeholders = [f'(%s, %s, %s)' for _ in range(scores.shape[0])]
    values = [i for r in scores.itertuples() for i in (r.Index, r.pred, scoring_run_id)]

    with conn.cursor() as cur:
        query_str = f'INSERT INTO cb.member_scores (member_id, score, scoring_run_id) values {",".join(placeholders)};'
        query = cur.mogrify(query_str, values)
        cur.execute(query)

    conn.commit()
    conn.close()


########################################################################################################################
# Helpers from notebook
########################################################################################################################

# fully broken out month over month features
def features_mom(df, cols):
    #     print('building month over month features')
    df = df.fillna(0)
    pre = df.query("period < 0")
    pre = pre.pivot(index='member_id', columns='period', values=cols)
    pre.columns = [f'{period}-{name}' for (name, period) in pre.columns]
    return pre.fillna(0)


features_mom.name = 'MOM'


# agg semi yearly_features
def features_semi_annual(df, cols):
    #     print('building semi annual features')
    df = df.fillna(0)
    pre = df.query("period < 0")
    h1 = pre.query('period < -6').groupby('member_id')
    h2 = pre.query('period >= -6').groupby('member_id')

    h1 = h1[cols].sum()
    h2 = h2[cols].sum()

    features_h1 = np.divide(h1[cols], h1[['p_mm']])
    features_h2 = np.divide(h2[cols], h2[['p_mm']])
    res = features_h2.merge(features_h1, left_index=True, right_index=True, suffixes=('_h2', '_h1'))
    return res.fillna(0)


features_semi_annual.name = 'Semi Annual'


# agg yearly_features
def features_annual(df, cols):
    #     print('building annual features')
    df = df.fillna(0)
    pre = df.query("period < 0").groupby('member_id')
    pre_sums = pre[cols].sum()
    res = np.divide(pre_sums[cols], pre_sums[['p_mm']])
    return res.fillna(0)


features_annual.name = 'Annual'


def print_feature_importance(regr, cols, max_cols=20):
    print('Feature Importance')
    i = 0
    for imp, feat in sorted([(b, a) for a, b in zip(cols, regr.feature_importances_)], reverse=True):
        if imp > 0.001:
            print('%0.3f: %s' % (imp, feat))
            i += 1
        if i > max_cols:
            break


def print_coef_importance(regr, cols, max_cols=20):
    print('Feature Importance')
    i = 0
    for imp, feat in sorted([(b, a) for a, b in zip(cols, regr.coef_)], reverse=True):
        if imp > 0.001:
            print('%0.3f: %s' % (imp, feat))
            i += 1
        if i > max_cols:
            break


def get_miss_ided(X_test, y_test, preds, verbose=True, id_pop_size=100):
    # test split is 20%, 20% of 500 == 100
    test_df = X_test.assign(target=y_test, pred=preds)

    pre_rule_id = test_df.sort_values('savings_ft', ascending=False).iloc[:id_pop_size]
    perf_id = test_df.sort_values('target', ascending=False).iloc[:id_pop_size]
    pred_id = test_df.sort_values('pred', ascending=False).iloc[:id_pop_size]

    pred_misses = perf_id.index.difference(pred_id.index).shape[0]
    rule_misses = perf_id.index.difference(pre_rule_id.index).shape[0]

    if verbose:
        print(f'Miss IDed: {pred_misses * 100.0 / id_pop_size}%')
        print(f'Rule Miss IDed: {rule_misses * 100.0 / id_pop_size}%')
    return pred_misses, rule_misses
