#!/usr/bin/env python
# coding: utf-8

import random
from multiprocessing import Pool
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.inspection import permutation_importance

# sys.path.append('../../src')
# import ds_matching


def train_func(l2, lr, mi, mln, md, msl, x_train, y_train, x_val, y_val, categorical_feature_indexes):
    aa_est = HistGradientBoostingRegressor(categorical_features=categorical_feature_indexes,
                                           learning_rate=lr,
                                           max_iter=mi,
                                           max_leaf_nodes=mln,
                                           max_depth=md,
                                           min_samples_leaf=msl,
                                           l2_regularization=l2,
                                           ).fit(x_train, y_train)

    aa_preds = aa_est.predict(x_val)
    score = aa_est.score(x_val, y_val)
    mae = np.mean(np.abs(y_val - aa_preds))
    return [score, mae, l2, lr, mi, mln, md, msl]


if __name__ == '__main__':
    df = pd.read_csv('/Users/bp/workspace/cb/data-analytics/notebooks/data/ds_rec_hrs_modelling_w_claims_20221205.csv',
                     low_memory=False)
    print(f'{df.shape[0]} samples')

    categorical_features = ['payer_id_ft', 'dressing_ft', 'bathing_ft', 'eating_ft', 'grooming_ft', 'toileting_ft',
                            'meal_prep_ft', 'housework_ft', 'transportation_ft', 'alert_oriented_self_ft',
                            'alert_oriented_day_time_ft', 'has_dementia_ft']
    cont_features = ['age_ft', 'rheumatoid_arthritis_ddos_ft', 'hyperlipidemia_ddos_ft', 'diabetes_ddos_ft',
                     'hypertension_ddos_ft', 'neurocognitive_ddos_ft']

    features = categorical_features + cont_features
    target = 'reporting_rec_hrs_tg'
    categorical_feature_indexes = [i for i in range(len(categorical_features))]

    # test set: all sferes submitted in last 30d plus any other sferes for thos patients
    test_df = df.loc[df.submitted_last_30d]
    test_df_patient_ids = test_df.patient_id.unique()
    test_df = df.loc[df.patient_id.isin(test_df_patient_ids)]
    print(f'{test_df.shape[0]} test samples, for {test_df_patient_ids.shape[0]} unique patients')

    training_df = df.loc[~df.patient_id.isin(test_df_patient_ids)]
    training_df_patient_ids = training_df.patient_id.unique()
    print(f'{training_df.shape[0]} training samples, for {training_df_patient_ids.shape[0]} unique patients')

    val_pct = 0.15
    val_df_patient_ids = random.sample(list(training_df_patient_ids), int(training_df.shape[0] * val_pct))
    val_df = training_df.loc[training_df.patient_id.isin(val_df_patient_ids)]
    print(f'{val_df.shape[0]} val samples, for {len(val_df_patient_ids)} unique patients')

    train_no_val_df = training_df.loc[~training_df.patient_id.isin(val_df_patient_ids)]
    print(
        f'{train_no_val_df.shape[0]} train samples, for {train_no_val_df.patient_id.unique().shape[0]} unique patients')

    x_train = train_no_val_df[features]
    y_train = train_no_val_df[target]

    x_val = val_df[features]
    y_val = val_df[target]

    # ### Tune Combo model with best features
    l2s = [0, .01, .05, .1]
    learning_rates = [0.01, 0.05, 0.1, 0.2, 0.5]
    max_iters = [1000]
    max_leaf_nodes = [10, 20, 30, 50, None]
    max_depth = [5, 10, 20, 30, None]
    min_samples_leaf = [10, 20, 30, 40, 50]
    combos = [(l2, lr, mi, mln, md, msl, x_train, y_train, x_val, y_val, categorical_feature_indexes )
              for l2 in l2s
              for lr in learning_rates
              for mi in max_iters
              for mln in max_leaf_nodes
              for md in max_depth
              for msl in min_samples_leaf]
    print(f'{len(combos)} hyperparam combos')

    with Pool() as p:
        results = p.starmap(train_func, combos)

    results_df = pd.DataFrame(results, columns=['score', 'mae', 'l2', 'lr', 'mi', 'mln', 'md', 'msl'])
    results_df.sort_values('score', inplace=True)
    print(results_df.head())
    results_df.to_csv('combo_model_tuning_results_20221205.csv', index=False)

# # Set lower and upper quantile
# LOWER_ALPHA = 0.05
# UPPER_ALPHA = 0.95
#
# lower_model = HistGradientBoostingRegressor(categorical_features=categorical_feature_indexes,
#                                             loss="quantile",
#                                             quantile=LOWER_ALPHA
#                                            ).fit(q_data.x_train, q_data.y_train)
#
# mid_model = HistGradientBoostingRegressor(categorical_features=categorical_feature_indexes,
#                                           loss="quantile",
#                                           quantile=.5
#                                          ).fit(q_data.x_train, q_data.y_train)
#
# upper_model = HistGradientBoostingRegressor(categorical_features=categorical_feature_indexes,
#                                             loss="quantile",
#                                             quantile=UPPER_ALPHA
#                                            ).fit(q_data.x_train, q_data.y_train)
#
#
# # In[35]:
#
#
# l_preds = lower_model.predict(q_data.x_test)
# m_preds = mid_model.predict(q_data.x_test)
# h_preds = upper_model.predict(q_data.x_test)
#
#
# # In[36]:
#
#
# ax = sns.relplot(x=q_data.y_test, y=m_preds, marker='+', height=8)
# ax.set(title=f'All Assessor Model, All Features, median error; R2:{mid_model.score(q_data.x_test, q_data.y_test):.02f} MAE: {np.mean(np.abs(q_data.y_test - m_preds)):.02f}')
#
#
# # In[37]:
#
#
# vals = []
# for i, p in enumerate(l_preds):
#     vals.append([q_data.y_test.values[i], p, m_preds[i], h_preds[i]])
#
# bounds_df = pd.DataFrame(vals, columns=['actual', 'low', 'med','high'])
# bounds_df['range'] = bounds_df.high - bounds_df.low
#
# # consider it in range if it is within 1 hr
# bounds_df['in_range'] = np.where((bounds_df.actual - bounds_df.low > -1) &
#                                  (bounds_df.actual - bounds_df.high < 1), 1, 0)
# # exact in bounds
# bounds_df['exact_in_range'] = np.where((bounds_df.actual >= bounds_df.low) & (bounds_df.actual <= bounds_df.high), 1, 0)
#
# print(f'Percent in +/- 1 range: {bounds_df.in_range.sum() * 100 / bounds_df.shape[0]}')
# print(f'Percent exact in range: {bounds_df.exact_in_range.sum() * 100 / bounds_df.shape[0]}')
# bounds_df.head()
#
#
# # In[38]:
#
#
# bounds_tall_df = pd.melt(bounds_df, id_vars=['actual', 'in_range'], value_vars=['low', 'med', 'high'])
# bounds_tall_df.head()
#
#
# # In[39]:
#
#
# sns.relplot(data=bounds_tall_df, x='actual', y='value', col='variable', hue='in_range')
#
#
# # In[40]:
#
#
# bounds_df.range.describe()
#
#
# # In[41]:
#
#
# bounds_df.loc[bounds_df.range <=0]
#
#
# # In[ ]:
#
#
#
#
#
# # ### Patient Similarity model
# # - Find the k (in this case 10) most similar patients to a single patient within the context of the payer
# # - Can compare signle patient to mean/median min/max of 10 "neighbors"
# # - Can also look at the 10 neighbors, see how they are similar
#
# # In[24]:
#
#
# from sklearn import preprocessing
# from sklearn.pipeline import Pipeline
# from sklearn.impute import SimpleImputer
# from sklearn.neighbors import NearestNeighbors, KNeighborsRegressor
#
#
# # #### These are the dimensions over which we calc similarity
# # Currently just adls and age
#
# # In[25]:
#
#
# adls = [
#     'transfer_bed_to_chair',
#     'transfer_chair_to_standing',
#     'mobility',
#     'dressing',
#     'bathing',
#     'eating',
#     'grooming',
#     'toileting',
#     'turn_change_position'
# ]
# similarity_features = adls + ['age']
#
#
# # In[26]:
#
#
# pid = df.payer_id.unique()[0]
# payers = df.payer_id.unique()
# neighbors = {}
# k = 10
#
# for pid in payers:
#     payer_df = df.loc[~df[target].isna()]
#     payer_df = payer_df.loc[payer_df.payer_id == pid]
#     x_train = payer_df[similarity_features]
#
#     pipe = Pipeline([('imputer', SimpleImputer(strategy='most_frequent')), ('scaler', preprocessing.StandardScaler())])
#     pipe.fit(x_train)
#     x = pipe.transform(x_train)
#     nn = NearestNeighbors(n_neighbors=k).fit(x)
#     distances, nn_idxs = nn.kneighbors()
#
#     # get patient_ids
#     for i, nn_idx in enumerate(nn_idxs):
#         neighbors[payer_df.iloc[i].patient_id] = payer_df.iloc[nn_idx].patient_id.values
#
#
# # In[27]:
#
#
# rows = []
# for pid in df.loc[~df[target].isna()].patient_id.unique():
#     pid_neighbors = df.loc[df.patient_id.isin(neighbors[pid])]
#     p = df.loc[df.patient_id == pid]
#
#     rows.append([pid, p.reporting_rec_hrs.mean(), pid_neighbors.reporting_rec_hrs.mean(), pid_neighbors.reporting_rec_hrs.median(), pid_neighbors.reporting_rec_hrs.min(), pid_neighbors.reporting_rec_hrs.max()])
# nn_df = pd.DataFrame(rows, columns=['patient_id', 'rec_hrs', 'mean_nn', 'median_nn', 'min_nn', 'max_nn'])
# nn_df.head()
#
#
# # In[132]:
#
#
# # calc ranges and mean abs error
# nn_df = nn_df.assign(exact_in_range=np.where((nn_df.rec_hrs >= nn_df.min_nn) & (nn_df.rec_hrs <= nn_df.max_nn), 1, 0),
#                      hrs_range=nn_df.max_nn - nn_df.min_nn,
#                      in_range=np.where((nn_df.rec_hrs - nn_df.min_nn > -1) & (nn_df.rec_hrs - nn_df.max_nn < 1), 1, 0))
#
# nn_df = nn_df.assign(mean_err=np.abs(nn_df.mean_nn - nn_df.rec_hrs), median_err=np.abs(nn_df.median_nn - nn_df.rec_hrs))
#
#
# # In[133]:
#
#
# nn_df.head()
#
#
# # In[129]:
#
#
# print(f'Percent rec hrs in neighbors range: {nn_df.in_range.sum() * 100 / nn_df.shape[0]}')
# print(f'Percent rec hrs in exact neighbors range: {nn_df.exact_in_range.sum() * 100 / nn_df.shape[0]}')
#
#
# # In[134]:
#
#
# nn_df.describe()
#
#
# # ### Check random patient's neighbors
# # In practice could show top 10 most similar patients
#
# # In[175]:
#
#
# pid = nn_df.patient_id.sample().values[0]
# pid_neighbors = df.loc[df.patient_id.isin(neighbors[pid])]
# print(f'Patient id {pid}, neighbor mean/range: {pid_neighbors.reporting_rec_hrs.mean()}; {pid_neighbors.reporting_rec_hrs.min()} - {pid_neighbors.reporting_rec_hrs.max()}')
# pid_neighbors
#
#
# # In[ ]:
#
#
#
#
#
# # ### Chronic conditions model
# # - Model based on chronic conditions found in claims data in the year leading up to the sfere submission
# # - Claims lag was used in feature generation
# # - This model could potentially be improved by including adls but keeping it just chronic conditions makes it easier to explain/interpret
#
# # In[33]:
#
#
# base_cc_features = [
#     'glaucoma',
#     'cataract',
#     'behavioral_health',
#     'osteoporosis',
#     'hiv',
#     'transplants',
#     'obesity',
#     'cancer',
#     'hip_pelvic_fracture',
#     'sclerosis',
#     'rheumatoid_arthritis',
#     'ckd',
#     'hyperlipidemia',
#     'diabetes',
#     'hypothyroidism',
#     'pressure_ulcer',
#     'weight_loss',
#     'heart',
#     'peptic_ulcer',
#     'anemia',
#     'substance_abuse',
#     'liver',
#     'disabled',
#     'fall',
#     'stroke',
#     'paralysis',
#     'hypertension',
#     'peripheral_vascular',
#     'coagulation',
#     'fluid',
#     'benign_prostatic_hyperplasia',
#     'tbi',
#     'neurocognitive',
#     'pulmonary'
# ]
# cc_ddos_features = [f'{f}_ddos' for f in base_cc_features]
# cc_tc_features = [f'{f}_tc' for f in base_cc_features]
# cc_features = ['payer_id', 'age'] + cc_tc_features
# cc_features = ['payer_id', 'age'] + cc_ddos_features
#
#
# # In[34]:
#
#
# cc_data = ds_matching.tt_split_by_sfere_ids(df, cc_features, target, test_sfere_ids=aa_data.test_df.sfere_id.values)
#
#
# # In[35]:
#
#
# # payer_id is only categorical feature
# cc_est = HistGradientBoostingRegressor(categorical_features=[0]).fit(cc_data.x_train, cc_data.y_train)
# cc_preds = cc_est.predict(cc_data.x_test)
#
#
# # In[36]:
#
#
# # preds vs rec
# ax = sns.relplot(x=cc_data.y_test, y=cc_preds, marker='+', height=8)
# ax.set(title=f'All Assessor Chronic Conditions Model; R2:{cc_est.score(cc_data.x_test, cc_data.y_test):.02f} MAE: {np.mean(np.abs(cc_data.y_test - cc_preds)):.02f}')
#
#
# # ### Feature Importance
# # See what is driving each of the models
# #
# # - Note: similarity model treats all features equally
#
# # In[37]:
#
#
# from sklearn.inspection import permutation_importance
#
#
# # In[38]:
#
#
# print('Best in class model features')
# r = permutation_importance(bic_est, bic_data.x_test, bic_data.y_test,
#                            n_jobs=-1,
#                            n_repeats=30,
#                            random_state=0)
#
# for i in r.importances_mean.argsort()[::-1]:
#     if r.importances_mean[i] - 2 * r.importances_std[i] > 0:
#         print(f"{bic_data.features[i]:<8} "
#               f"{r.importances_mean[i]:.3f} "
#               f" +/- {r.importances_std[i]:.3f}")
#
#
# # In[39]:
#
#
# print('All assessor model features')
# r = permutation_importance(aa_est, aa_data.x_test, aa_data.y_test,
#                            n_jobs=-1,
#                            n_repeats=30,
#                            random_state=0)
#
# for i in r.importances_mean.argsort()[::-1]:
#     if r.importances_mean[i] - 2 * r.importances_std[i] > 0:
#         print(f"{aa_data.features[i]:<8} "
#               f"{r.importances_mean[i]:.3f} "
#               f" +/- {r.importances_std[i]:.3f}")
#
#
# # In[40]:
#
#
# print('All assesor, median quantile model with current hrs feature')
# r = permutation_importance(mid_model, q_data.x_test, q_data.y_test,
#                            n_jobs=-1,
#                            n_repeats=30,
#                            random_state=0)
#
# for i in r.importances_mean.argsort()[::-1]:
#     if r.importances_mean[i] - 2 * r.importances_std[i] > 0:
#         print(f"{q_data.features[i]:<8} "
#               f"{r.importances_mean[i]:.3f} "
#               f" +/- {r.importances_std[i]:.3f}")
#
#
# # In[41]:
#
#
# print('Chronic conditions model')
# r = permutation_importance(cc_est, cc_data.x_test, cc_data.y_test,
#                            n_jobs=-1,
#                            n_repeats=30,
#                            random_state=0)
#
# for i in r.importances_mean.argsort()[::-1]:
#     if r.importances_mean[i] - 2 * r.importances_std[i] > 0:
#         print(f"{cc_data.features[i]:<8} "
#               f"{r.importances_mean[i]:.3f} "
#               f" +/- {r.importances_std[i]:.3f}")
#
#
# # ### Build outputs file
# # For each build predictions for test and training set
#
# # In[42]:
#
#
# models = [
#     ('best_in_class_assessor_model', bic_est, bic_data),
#     ('all_assessor_model', aa_est, aa_data),
#     ('lower_quantile_model', lower_model, q_data),
#     ('mid_quantile_model', mid_model, q_data),
#     ('upper_quantile_model', upper_model, q_data),
#     ('chronic_condition_model', cc_est, cc_data)
# ]
# # similarity model has slightly different api
# # nn_df
#
#
# # In[84]:
#
#
# # (name, model, data) = ('best_in_class_assessor_model', bic_est, bic_data)
# result_dfs = []
#
# for (name, model, data) in models:
#     train_preds = model.predict(data.training_df[data.features])
#     test_preds = model.predict(data.test_df[data.features])
#     data.training_df = data.training_df.assign(**{name: train_preds})
#     data.test_df = data.test_df.assign(**{name: test_preds})
#     comb = pd.concat([data.training_df[['sfere_id', name]],  data.test_df[['sfere_id', name]]])
#     comb.set_index('sfere_id', inplace=True)
#
#     result_dfs.append(comb)
#
#
# # In[135]:
#
#
# results_df = pd.concat(result_dfs, axis=1).reset_index()
# # add patient ids and other info
# results_df = results_df.merge(df[['sfere_id', 'patient_id', 'payer_id', 'reporting_rec_hrs', 'reporting_current_hrs']], on='sfere_id')
# results_df = results_df.assign(is_test_set=np.where(results_df.sfere_id.isin(aa_data.test_df.sfere_id), 1, 0))
#
#
# # In[136]:
#
#
# # add nearest neighbors
# results_df = results_df.merge(nn_df[['patient_id', 'median_nn', 'min_nn', 'max_nn']], on='patient_id')
#
#
# # In[137]:
#
#
# # add ensemble
# non_cur_hrs_models = [
#  'best_in_class_assessor_model',
#  'all_assessor_model',
#  'chronic_condition_model',
#  'median_nn',
# ]
# results_df = results_df.assign(ensemble=results_df[non_cur_hrs_models].mean(axis=1))
# results_df = results_df.assign(
#     ensemble_err=np.abs(results_df.ensemble - results_df.reporting_rec_hrs),
#     best_in_class_err=np.abs(results_df.best_in_class_assessor_model - results_df.reporting_rec_hrs),
#     all_assessor_err=np.abs(results_df.all_assessor_model - results_df.reporting_rec_hrs),
#     mid_quantile_err=np.abs(results_df.mid_quantile_model - results_df.reporting_rec_hrs),
#     chronic_condition_err=np.abs(results_df.chronic_condition_model - results_df.reporting_rec_hrs),
#     median_nn_err=np.abs(results_df.median_nn - results_df.reporting_rec_hrs),
# )
#
#
# # In[138]:
#
#
# output_cols = [
#  'patient_id',
#  'payer_id',
#  'sfere_id',
#  'reporting_rec_hrs',
#  'reporting_current_hrs',
#  'is_test_set',
#  'best_in_class_assessor_model',
#  'all_assessor_model',
#  'lower_quantile_model',
#  'mid_quantile_model',
#  'upper_quantile_model',
#  'chronic_condition_model',
#  'median_nn',
#  'min_nn',
#  'max_nn',
#  'ensemble',
#  'ensemble_err',
#  'best_in_class_err',
#  'all_assessor_err',
#  'mid_quantile_err',
#  'chronic_condition_err',
#  'median_nn_err'
# ]
# results_df[output_cols].head()
#
#
# # In[139]:
#
#
# results_df[output_cols].describe()
#
#
# # In[140]:
#
#
# results_df.loc[results_df.is_test_set == 1, output_cols].describe()
#
#
# # In[141]:
#
#
# results_df[output_cols].to_csv('/Users/bp/Downloads/ds_patient_matching_20221123.csv', index=False)
#
#
# # ### Look at test set outlier detection
#
# # In[167]:
#
#
# test_results = results_df.loc[results_df.is_test_set == 1, output_cols]
# n_test = test_results.shape[0]
# print(f'N test set: {n_test}')
#
#
# # In[178]:
#
#
# # get chronic condition model std by payer
# cc_err_std_by_payer = test_results.groupby('payer_id').chronic_condition_err.std().to_dict()
# test_results = test_results.assign(cc_err_std_gt_2=test_results.apply(lambda x: x.chronic_condition_err > cc_err_std_by_payer[x.payer_id] * 1, axis=1))
#
#
# # In[180]:
#
#
# # Flage some outliers
# outliers = test_results.loc[
#     (test_results.reporting_rec_hrs > test_results.upper_quantile_model) &
#     (test_results.reporting_rec_hrs > test_results.max_nn) &
#     (test_results.cc_err_std_gt_2)]
#
# print(f'N outliers: {outliers.shape[0]}, {int(outliers.shape[0] * 10000 / n_test) / 100 }%')
# outliers.head(20)
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # # Old
# # Can ignore below, saved for ease of pulling code snippets for future use
#
# # In[43]:
#
#
# raise "Stop"
#
#
# # ### Tuning
#
# # In[ ]:
#
#
# parameters = {
#     'max_iter': [1000],
#     'max_depth': [3,5,20,25],
#     'min_samples_leaf': [10, 20, 50, 75, 100],
#     'l2_regularization': [0, .1, .5]
# }
# parameters
#
#
# # In[ ]:
#
#
# est = HistGradientBoostingRegressor(categorical_features=[0])
#
# from sklearn.experimental import enable_halving_search_cv
# from sklearn.model_selection import HalvingGridSearchCV
#
# clf = HalvingGridSearchCV(est, parameters, factor=2, n_jobs=-1, verbose=1)
# clf.fit(X_train, y_train)
#
# clf.best_score_, clf.best_params_
#
#
# # ### Compare to DL
#
# # In[ ]:
#
#
# lm_preds = pd.read_csv('/Users/bp/workspace/cb/data-analytics/notebooks/data/ds_hrs_lm_sfere_preds_2.csv')
# print(lm_preds.shape[0])
# lm_preds.head()
#
#
# # In[ ]:
#
#
# comb = tdf.merge(lm_preds, on='sfere_id')
# print(comb.shape[0])
# comb.head()
#
#
# # In[ ]:
#
#
# ax = sns.relplot(data=comb, x='reporting_rec_hrs', y='pred', marker='+', height=8)
# ax.set(title=f'All Assessor Model, last 30d; R2:{est.score(X_test, y_test):.02f} MAE: {np.mean(np.abs(comb.reporting_rec_hrs - comb.pred)):.02f}')
#
#
# # In[ ]:
#
#
# melted = pd.melt(comb, id_vars=['reporting_rec_hrs'], value_vars=['predicted_hours', 'pred'])
# ax = sns.relplot(data=melted, x='reporting_rec_hrs', y='value', hue='variable', marker='+', height=8)
#
#
# # In[ ]:
#
#
# comb = comb.assign(ensemble=comb.predicted_hours * .5 + comb.pred * .5)
#
#
# # In[ ]:
#
#
# comb = comb.assign(rec_minus_ensemble=comb.reporting_rec_hrs - comb.ensemble)
#
#
# # In[ ]:
#
#
# comb[['rec_minus_ensemble', 'rec_minus_pred']].abs().mean()
#
#
# # ### Check out embeddings
#
# # In[ ]:
#
#
# emb = pd.read_csv('data/ds_hrs_lm_sfere_encodings_20221116.csv')
#
#
# # In[ ]:
#
#
# emb.head()
#
#
# # In[ ]:
#
#
# comb = comb.merge(emb, on='sfere_id')
# comb.head()
#
#
# # In[ ]:
#
#
# comb.encoding.dtype
# a = [1,2,3]
# b = np.array(a)
# b
#
#
# # In[ ]:
#
#
# # need to transform list str into actual list
# import ast
# comb.encoding = comb.encoding.apply(ast.literal_eval)
# comb.encoding = comb.encoding.apply(np.array)
#
#
# # In[ ]:
#
#
# vectors = np.zeros((comb.shape[0], 400))
# for i, r in enumerate(comb.encoding):
#     vectors[i] = r
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
# from scipy.spatial import distance
#
#
# # In[ ]:
#
#
# target = comb.encoding.values[1]
# # distances = distance.cdist([target], vectors, "cosine")[0]
# distances = distance.cdist([target], vectors, "euclidean")[0]
# min_index = np.argmin(distances)
# min_distance = distances[min_index]
# # print("Most similar vector to target is index %s with %s" % (max_index, max_similarity))
#
#
# # In[ ]:
#
#
# sim_idx = [si for si in np.argsort(distances)[0:11]]
# comb.loc[sim_idx]
#
#
# # ### Reduce dimensions of emb to feed to boosted tree model
#
# # In[ ]:
#
#
# from sklearn.decomposition import PCA
# pca = PCA(n_components=2)
# pca.fit(vectors)
#
#
# # In[ ]:
#
#
# xy = pca.transform(vectors)
# xy
#
#
# # In[ ]:
#
#
# sns.relplot(x=xy[:, 0], y=xy[:,1])
#
#
# # ### Look at similarity based just on adl distances
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
#
# # In[ ]:
#
#
#
#
