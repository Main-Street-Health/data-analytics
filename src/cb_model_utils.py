import pandas as pd
import numpy as np
import cb_utils


def build_member_periods(df, pre_months=12, post_months=6):
    """
    Flags what periods each month is valid for
    For example pre_post_elg_0=True means the member is valid for period 0
      and then one of the pre_0 or post_0 should be true to signify if the
      month is in the period pre or post period
    returns df with the flags and the months df
    """
    pre_post_months = pre_months + post_months

    months = sorted(df.eom.unique())
    n_months = len(months)
    last_valid_pre_start = n_months - pre_post_months  # 42

    flags = [f'{prefix}_{i}' for prefix in ['pre', 'post', 'pre_post_elg']
             for i in range(n_months) if i < last_valid_pre_start]

    new_cols = pd.DataFrame(np.full((df.shape[0], len(flags)), False), columns=flags)
    df = pd.concat([df, new_cols], axis=1)

    periods = []
    for i in range(last_valid_pre_start):
        # Build date anchor points relative to start month
        pre_start = months[i]
        pre_end = months[i + pre_months - 1]
        post_start = None
        post_end = None
        if i + pre_post_months + 1 < n_months:  # specific to current dataset, post period runs out
            post_start = months[i + pre_months]
            post_end = months[i + pre_post_months + 1]

        periods.append([i, pre_start, pre_end, post_start, post_end])

        # Determine elg members
        pre_elg = df.loc[(df.eom == pre_end) & (df.is_cb_eligible)].member_id.unique()
        post_elg = df.loc[(df.eom == post_start) & (df.is_cb_eligible)].member_id.unique()

        pre_post_elg_mems = np.intersect1d(pre_elg, post_elg)

        # Flag elg members for period i
        is_pre = (df.eom >= pre_start) & (df.eom <= pre_end)
        is_post = (df.eom >= post_start) & (df.eom <= post_end)
        is_pre_post = (df.eom >= pre_start) & (df.eom <= post_end)

        df.loc[is_pre & (df.member_id.isin(pre_elg)), f'pre_{i}'] = True
        df.loc[is_post & (df.member_id.isin(post_elg)), f'post_{i}'] = True
        df.loc[is_pre_post & (df.member_id.isin(pre_post_elg_mems)), f'pre_post_elg_{i}'] = True

    months_df = pd.DataFrame(months, columns=['eom'])

    # fill na's
    df.is_cb_eligible = df.is_cb_eligible.fillna(False)
    df.is_unaligned = df.is_unaligned.fillna(False)
    df = df.fillna(0)

    # add gender, state
    df = df.assign(is_male=np.where(df.gender == 'm', 1, 0))

    # assign state
    df = df.assign(state=df.mco_state)

    return df, months_df


# Feature Generation

service_types = ['ip', 'er', 'out', 'snf', 'icf', 'hh', 'amb', 'hsp', 'pro', 'spc_fac', 'dme', 'cls', 'hha']
all_demographic_cols = ['is_state_az',
                         'is_state_dc',
                         'is_state_fl',
                         'is_state_ia',
                         'is_state_ks',
                         'is_state_ma',
                         'is_state_mn',
                         'is_state_oh',
                         'is_state_tn',
                         'is_state_tx',
                         'is_state_va',
                         'is_lob_1',
                         'is_lob_3',
                         'is_lob_2',
                         'is_lob_8',
                         'is_group_0',
                         'is_group_3',
                         'is_group_2',
                         'is_group_1',
                         'is_group_-1',
                         'is_group_5',
                         'is_group_8',
                         'is_group_4',
                         'is_group_6',
                         'is_group_7',
                         'is_group_10',
                         'is_group_9',
                         'is_group_11',
                         'is_male',
                         'is_female',
                         'age']

all_disease_cols = ['oxygen', 'hosp_bed', 'chf', 'heart', 'copd', 'pulmonar', 'cancer', 'ckd', 'esrd', 'lipidy', 'diab',
                    'alzh', 'demented', 'stroke', 'hyper', 'fall', 'trans', 'liver', 'hippy', 'depressed', 'psycho',
                    'druggy', 'boozy', 'paralyzed', 'mono', 'mono_dom', 'hemi', 'hemi_dom', 'para', 'quad', 'tbi',
                    'obese', 'pressure_ulcer', 'hemophilia']
ddos_service_type_cols = [f'{st}_ddos' for st in service_types]
ddos_disease_cols = [f'{d}_ddos' for d in all_disease_cols]
tc_service_type_cols = [f'{st}_tc' for st in service_types]
exclusion_cols = ['member_id', 'target', 'period']


def build_yearly_stddos_dem(df, demographic_cols=all_demographic_cols):
    feature_cols = ddos_service_type_cols + demographic_cols
    n_features = len(feature_cols)
    n_columns = n_features + 2  # member_id + target
    st_matrix = np.zeros((df.shape[0], n_columns))

    for i in range(len(ddos_service_type_cols)):
        st = ddos_service_type_cols[i]
        st_cols = [f'{st}_{i}' for i in range(12)]
        st_matrix[:, i] = df[st_cols].sum(axis=1)

    st_matrix[:, len(ddos_service_type_cols):] = df[demographic_cols + ['member_id', 'target']]
    d = pd.DataFrame(st_matrix, columns=feature_cols + ['member_id', 'target'])
    return d


def build_yearly_sttc_dem(df, demographic_cols=all_demographic_cols):
    feature_cols = tc_service_type_cols + demographic_cols
    n_features = len(feature_cols)
    n_columns = n_features + 2  # member_id + target
    st_matrix = np.zeros((df.shape[0], n_columns))

    for i in range(len(tc_service_type_cols)):
        st = tc_service_type_cols[i]
        st_cols = [f'{st}_{i}' for i in range(12)]
        st_matrix[:, i] = df[st_cols].sum(axis=1)

    st_matrix[:, len(tc_service_type_cols):] = df[demographic_cols + ['member_id', 'target']]
    d = pd.DataFrame(st_matrix, columns=feature_cols + ['member_id', 'target'])
    return d


def build_mom_stddos_dem(df, demographic_cols=all_demographic_cols):
    mom_ddos_service_type_cols = [f'{c}_{i}' for c in ddos_service_type_cols for i in range(12)]
    feature_cols = mom_ddos_service_type_cols + demographic_cols
    n_features = len(feature_cols)
    n_columns = n_features + 2  # member_id + target
    st_matrix = np.zeros((df.shape[0], n_columns))

    st_matrix[:, :len(mom_ddos_service_type_cols)] = df[mom_ddos_service_type_cols]
    st_matrix[:, len(mom_ddos_service_type_cols):] = df[demographic_cols + ['member_id', 'target']]
    d = pd.DataFrame(st_matrix, columns=feature_cols + ['member_id', 'target'])
    return d


def build_mom_sttc_dem(df, demographic_cols=all_demographic_cols):
    mom_tc_service_type_cols = [f'{c}_{i}' for c in tc_service_type_cols for i in range(12)]
    feature_cols = mom_tc_service_type_cols + demographic_cols
    n_features = len(feature_cols)
    n_columns = n_features + 2  # member_id + target
    st_matrix = np.zeros((df.shape[0], n_columns))

    st_matrix[:, :len(mom_tc_service_type_cols)] = df[mom_tc_service_type_cols]
    st_matrix[:, len(mom_tc_service_type_cols):] = df[demographic_cols + ['member_id', 'target']]
    d = pd.DataFrame(st_matrix, columns=feature_cols + ['member_id', 'target'])
    return d


def build_yearly_dxddos_dem(df, demographic_cols=all_demographic_cols):
    feature_cols = ddos_disease_cols + demographic_cols
    n_features = len(feature_cols)
    n_columns = n_features + 2  # member_id + target
    st_matrix = np.zeros((df.shape[0], n_columns))

    for i in range(len(ddos_disease_cols)):
        st = ddos_disease_cols[i]
        st_cols = [f'{st}_{i}' for i in range(12)]
        st_matrix[:, i] = df[st_cols].sum(axis=1)

    st_matrix[:, len(ddos_disease_cols):] = df[demographic_cols + ['member_id', 'target']]
    d = pd.DataFrame(st_matrix, columns=feature_cols + ['member_id', 'target'])
    return d


def build_yearly_stdxddos_dem(df, demographic_cols=all_demographic_cols):
    feature_cols = ddos_disease_cols + ddos_service_type_cols + demographic_cols
    n_features = len(feature_cols)
    n_columns = n_features + 2  # member_id + target
    st_matrix = np.zeros((df.shape[0], n_columns))

    for i in range(len(ddos_disease_cols)):
        st = ddos_disease_cols[i]
        st_cols = [f'{st}_{i}' for i in range(12)]
        st_matrix[:, i] = df[st_cols].sum(axis=1)

    for i in range(len(ddos_service_type_cols)):
        st = ddos_service_type_cols[i]
        st_cols = [f'{st}_{i}' for i in range(12)]
        st_matrix[:, i + len(ddos_disease_cols)] = df[st_cols].sum(axis=1)

    st_matrix[:, len(ddos_disease_cols) + len(ddos_service_type_cols):] = df[demographic_cols + ['member_id', 'target']]
    d = pd.DataFrame(st_matrix, columns=feature_cols + ['member_id', 'target'])
    return d


def build_mom_stdxddos_dem(df, demographic_cols=all_demographic_cols, ddos_disease_cols=ddos_disease_cols):
    mom_ddos_service_type_cols = [f'{c}_{i}' for c in ddos_service_type_cols for i in range(12)]
    mom_ddos_dx_cols = [f'{c}_{i}' for c in ddos_disease_cols for i in range(12)]
    feature_cols = mom_ddos_dx_cols + mom_ddos_service_type_cols + demographic_cols
    n_features = len(feature_cols)
    n_columns = n_features + 2  # member_id + target
    st_matrix = np.zeros((df.shape[0], n_columns))

    st_matrix[:, :len(mom_ddos_dx_cols)] = df[mom_ddos_dx_cols]
    st_matrix[:, len(mom_ddos_dx_cols):len(mom_ddos_dx_cols) + len(mom_ddos_service_type_cols)] = df[
        mom_ddos_service_type_cols]
    st_matrix[:, len(mom_ddos_dx_cols) + len(mom_ddos_service_type_cols):] = df[
        demographic_cols + ['member_id', 'target']]
    d = pd.DataFrame(st_matrix, columns=feature_cols + ['member_id', 'target'])
    return d


# train test splitting
def train_val_test_split(df, file_suffix='', save_member_id=False, sm_format=True, pct_train=.8, pct_val=.15,
                         upload=True, train_mems=None, val_mems=None, test_mems=None, return_wo_saving=False):
    if train_mems is None or val_mems is None or test_mems is None:
        member_ids = df.member_id.unique()
        n_members = len(member_ids)

        train_n = int(n_members * .8)
        val_n = int(n_members * .15)
        test_n = n_members - train_n - val_n

        np.random.shuffle(member_ids)

        train_mems, val_mems, test_mems = np.split(member_ids, [train_n, train_n + val_n])

        assert train_mems.shape[0] == train_n
        assert val_mems.shape[0] == val_n
        assert test_mems.shape[0] == test_n

    training_df = df.loc[df.member_id.isin(train_mems)]
    val_df = df.loc[df.member_id.isin(val_mems)]
    test_df = df.loc[df.member_id.isin(test_mems)]

    if return_wo_saving:
        return training_df, val_df, test_df

    if not save_member_id:
        training_df = training_df.drop(columns=['member_id'])
        val_df = val_df.drop(columns=['member_id'])
        test_df = test_df.drop(columns=['member_id'])

    if sm_format:
        cols = ['target'] + [c for c in training_df.columns if c != 'target']  # sm target always first
        training_df[cols].to_csv(f'data/training_df_{file_suffix}.csv', header=False, index=False)
        val_df[cols].to_csv(f'data/val_df_{file_suffix}.csv', header=False, index=False)
        test_df[cols].to_csv(f'data/test_df_{file_suffix}.csv', header=False, index=False)

        with open(f'data/columns_{file_suffix}.txt', 'w') as f:
            f.write(','.join(cols))

        if upload:
            cb_utils.upload_file_to_s3(
                f'data/training_df_{file_suffix}.csv',
                'cb-analytics-us-east-2-prd',
                'sagemaker/data/' + file_suffix + '/train.csv')

            cb_utils.upload_file_to_s3(
                f'data/val_df_{file_suffix}.csv',
                'cb-analytics-us-east-2-prd',
                'sagemaker/data/' + file_suffix + '/val.csv')

            cb_utils.upload_file_to_s3(
                f'data/test_df_{file_suffix}.csv',
                'cb-analytics-us-east-2-prd',
                'sagemaker/data/' + file_suffix + '/test.csv')

            cb_utils.upload_file_to_s3(
                f'data/columns_{file_suffix}.txt',
                'cb-analytics-us-east-2-prd',
                'sagemaker/data/' + file_suffix + '/columns.txt')


    else:
        training_df.to_parquet(f'data/training_df_{file_suffix}.parquet')
        val_df.to_parquet(f'data/val_df_{file_suffix}.parquet')
        test_df.to_parquet(f'data/test_df_{file_suffix}.parquet')


def get_xy(df):
    x_cols = [c for c in df.columns if c not in ['member_id', 'target', 'period']]
    # x_cols = [c for c in df.columns if c not in ['member_id', 'target', 'period']]
    x = df[x_cols]
    y = df.target
    return x, y


def get_model_performance(model, train_df, val_df):
    x, y = get_xy(train_df)
    val_x, val_y = get_xy(val_df)
    train_preds = model.predict(x)
    val_preds = model.predict(val_x)
    return {
        'train_score': model.score(x, y),
        'val_score': model.score(val_x, val_y),
        'train_mae': np.abs(train_preds - y).mean(),
        'val_mae': np.abs(val_preds - val_y).mean()
    }


def feature_importance(model, val_df):
    from sklearn.inspection import permutation_importance
    val_x, val_y = get_xy(val_df)
    r = permutation_importance(model, val_x, val_y, n_repeats=10, random_state=0)

    for i in r.importances_mean.argsort()[::-1]:
        if r.importances_mean[i] - 2 * r.importances_std[i] > 0:
            print(f"{val_x.columns[i]:<8}"
                  f"{r.importances_mean[i]:.3f}"
                  f" +/- {r.importances_std[i]:.3f}")
