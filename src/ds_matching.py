from dataclasses import dataclass
import pandas as pd
from sklearn.model_selection import train_test_split


@dataclass
class Dataset:
    x_train: pd.DataFrame
    x_test: pd.DataFrame
    y_train: pd.DataFrame
    y_test: pd.DataFrame
    features: list
    # these dataframe's have all columns not just features/target but should be limited to test or training
    training_df: pd.DataFrame
    test_df: pd.DataFrame


def add_total_impairment_and_ranks(df):
    iadls = [
        'transfer_bed_to_chair_ft',
        'transfer_chair_to_standing_ft',
        'mobility_ft',
        'dressing_ft',
        'bathing_ft',
        'eating_ft',
        'grooming_ft',
        'toileting_ft',
        'turn_change_position_ft',
        'has_incontinence_ft',
        'incontinence_type_ft',
        'incontinence_frequency_ft',
        'calling_friends_and_family_ft',
        'articulating_needs_ft',
        'meal_prep_ft',
        'shopping_ft',
        'medication_management_ft',
        'finances_ft',
        'housework_ft',
        'transportation_ft',
        'daily_routine_decisions_ft',
        'comprehension_ft',
        'member_opinion_ft',
        'cleaning_ft',
        'laundry_ft',
        'change_bed_ft',
        'clean_kitchen_ft',
        'clean_home_ft',
        'medical_appointments_ft',
        'work_school_socialize_ft',
        'driving_ft',
    ]
    df = df.assign(total_impairment=df[iadls].sum(axis=1))
    df = df.assign(impairment_rank=df['total_impairment'].rank(pct=True),
                   cur_hrs_rank=df.reporting_current_hrs_ft.rank(pct=True),
                   rec_hrs_rank=df.reporting_rec_hrs_tg.rank(pct=True)
                   )
    return df


def tt_split_by_sfere_ids(df, features, target, test_sfere_ids=None):
    training_df = df.loc[~df[target].isna()]

    test_df = training_df.loc[training_df.sfere_id.isin(test_sfere_ids)]
    training_df = training_df.loc[~training_df.sfere_id.isin(test_df.sfere_id)]

    x_train = training_df[features]
    y_train = training_df[target]

    x_test = test_df[features]
    y_test = test_df[target]

    ds = Dataset(features=features,
                 x_train=x_train,
                 x_test=x_test,
                 y_train=y_train,
                 y_test=y_test,
                 training_df=training_df,
                 test_df=test_df)
    return ds


def tt_split(df, features, target, drop_zeros=False, test_size=0.2, best_assessor_training=False, test_set_30d=False):
    # can't have nan targets
    training_df = df.loc[~df[target].isna()]

    if drop_zeros:
        print('Dropping 0 targets')
        training_df = training_df.loc[training_df[target] > 0]

    if test_set_30d:
        print('Using sferes from last 30 days as test set')
        test_df = training_df.loc[~training_df.submitted_last_30d.isna()]
        test_df = test_df.loc[test_df.submitted_last_30d]
        training_df = training_df.loc[~training_df.sfere_id.isin(test_df.sfere_id)]

        if best_assessor_training:
            training_df = training_df.loc[training_df.best_in_class_assessor]

        x_train = training_df[features]
        y_train = training_df[target]
        x_test = test_df[features]
        y_test = test_df[target]

    elif best_assessor_training:
        print('Using best assessor samples for training')
        train = training_df.loc[training_df.best_in_class_assessor]
        test_df = training_df.loc[~training_df.best_in_class_assessor]

        x_train = train[features]
        x_test = test_df[features]
        y_train = train[target]
        y_test = test_df[target]

    else:
        x = training_df[features]
        y = training_df[target]
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=test_size)
        test_df = training_df.loc[x_test.index]

    ds = Dataset(features=features,
                 x_train=x_train,
                 x_test=x_test,
                 y_train=y_train,
                 y_test=y_test,
                 training_df=training_df.loc[x_train.index],
                 test_df=test_df)
    return ds


def tt_split_last_30d_test(df, features, target):
    training_df = df.loc[~df[target].isna()]

    test_df = training_df.loc[~training_df.submitted_last_30d.isna()]
    test_df = test_df.loc[test_df.submitted_last_30d]

    training_df = training_df.loc[~training_df.sfere_id.isin(test_df.sfere_id)]

    x_train = training_df[features]
    y_train = training_df[target]

    x_test = test_df[features]
    y_test = test_df[target]

    ds = Dataset(features=features,
                 x_train=x_train,
                 x_test=x_test,
                 y_train=y_train,
                 y_test=y_test,
                 training_df=training_df,
                 test_df=test_df)
    return ds
