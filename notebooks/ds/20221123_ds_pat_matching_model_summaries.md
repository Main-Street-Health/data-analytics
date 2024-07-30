# Model Summaries

### Best in Class (BIC) Assessor Model

- model type: boosted tree moddel
- features: ADLs + IADLs + payer_id + age + 'alert_oriented_self', 'alert_oriented_place', 'alert_oriented_day_time',
- training data: only sferes from best in class assessors:
  - ('Danielle', 'Spangler'), ('Patricia', 'Gilmore'), ('Beth', 'Saunders'),
  - ('Karen', 'Kruse'), ('Billi Jo', 'Ehrenberger'), ('Megan', 'Laher'),
  - ('Kelly', 'Nash'), ('Brandy', 'Richardson'), ('Lauren', 'Jezek')
- Test set Mean Absolute Error (MAE): 7.99
- R2: 0.52

Feature Importance:

    - payer_id 0.441  +/- 0.010
    - dressing 0.045  +/- 0.004
    - toileting 0.037  +/- 0.003
    - transfer_chair_to_standing 0.023  +/- 0.002
    - meal_prep 0.022  +/- 0.003
    - grooming 0.018  +/- 0.002
    - bathing  0.015  +/- 0.002
    - transfer_bed_to_chair 0.014  +/- 0.002
    - transportation 0.010  +/- 0.001
    - age      0.010  +/- 0.003
    - housework 0.009  +/- 0.002
    - shopping 0.008  +/- 0.001
    - eating   0.008  +/- 0.001
    - medication_management 0.006  +/- 0.001
    - finances 0.005  +/- 0.002
    - calling_friends_and_family 0.004  +/- 0.001
    - comprehension 0.003  +/- 0.000
    - alert_oriented_self 0.003  +/- 0.001
    - alert_oriented_day_time 0.002  +/- 0.001
    - laundry  0.002  +/- 0.001

### All Assessor (AA) Model

- This is the model used in the first two iterrations
- Same as Best in Class Model but using random draw for training sample
- Test set Mean absolute error: 7.1
- R2: 0.64

Feature Importance:

    - payer_id 0.501  +/- 0.024
    - toileting 0.050  +/- 0.005
    - meal_prep 0.030  +/- 0.005
    - dressing 0.021  +/- 0.004
    - housework 0.016  +/- 0.003
    - grooming 0.015  +/- 0.003
    - transfer_bed_to_chair 0.014  +/- 0.003
    - eating   0.013  +/- 0.003
    - age      0.011  +/- 0.002
    - transportation 0.009  +/- 0.002
    - bathing  0.009  +/- 0.002
    - transfer_chair_to_standing 0.008  +/- 0.004
    - alert_oriented_day_time 0.007  +/- 0.002
    - medication_management 0.006  +/- 0.002
    - laundry  0.002  +/- 0.001
    - finances 0.002  +/- 0.001

### Quantile Models

- I think of this as the guardrails model, uses all information and creates bounds
- Trains 3 boosted tree models with differing error weightings:
  - Upperbound: prediction should be higher than actual 95% of the time
  - Lowerbound: prediction should be lower than actual 95% of the time
  - Middle: prediction should be as close as possible to correct answer
- Features: same as BIC and AA BUT adds current hours
- Bounds results
  - 86% of recommended values were within range of bounds
  - 94% were within 1 hr
  - Mean range size: 16hrs
  - Median range: 13hrs
- Mid Model results
  - R2: .84
  - MAE: 3.19

Feature Importance:

    - reporting_current_hrs 1.383  +/- 0.037
    - payer_id 0.045  +/- 0.005
    - toileting 0.010  +/- 0.003
    - meal_prep 0.009  +/- 0.002
    - dressing 0.009  +/- 0.002
    - bathing  0.006  +/- 0.001
    - eating   0.003  +/- 0.001
    - housework 0.002  +/- 0.001
    - daily_routine_decisions 0.000  +/- 0.000
    - change_bed 0.000  +/- 0.000

### Patient Similarity Model

- KNN Model calculates euclidean distance between patients using only ADLs and age (equal weighting of all features)
- Used separate models for each payer
- Evaluated the 10 closest neighbors for each patient
- 86% of rec hours fell between min and max of neighbors
- The median of the neighbors was on average 7.7hrs from the rec hours
- Mean range size is 29hrs, median range size is 26hrs
- Could be interesting to display the neighbor group

### Chronic Conditions Model

- Boosted Tree Model using claims based chronic condition spend features with age and payer id
- R2: .39
- MAE: 9.68

Feature Importance:

    - payer_id 0.604  +/- 0.020
    - paralysis_ddos 0.075  +/- 0.009
    - age      0.070  +/- 0.006
    - neurocognitive_ddos 0.047  +/- 0.008
    - pressure_ulcer_ddos 0.011  +/- 0.004
    - diabetes_ddos 0.006  +/- 0.003
    - behavioral_health_ddos 0.006  +/- 0.002
    - hypertension_ddos 0.005  +/- 0.002
    - sclerosis_ddos 0.005  +/- 0.002
    - cataract_ddos 0.005  +/- 0.002
    - rheumatoid_arthritis_ddos 0.004  +/- 0.002
    - benign_prostatic_hyperplasia_ddos 0.002  +/- 0.001
    - fall_ddos 0.002  +/- 0.001
    - substance_abuse_ddos 0.001  +/- 0.001

### Deep Learning Model

- Used pretrained diagnosis code embeddings
- Only a sequence of diagnosis codes on non hcbs claims in the year leading up to the sfere are fed into the model (included claims lag)
- MAE: 9.22 (particularly impressive since it didn't use payer id or age, two of the most important features in the other models)
- high lift training and operationalization, may want to get other models in place and work on this for a v2
- hypothetically could be used for patient similarity but still needs some work
