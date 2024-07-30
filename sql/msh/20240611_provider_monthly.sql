-- create superset of glp codes from hedis docs and our value sets
-- DROP TABLE IF EXISTS _glps;
-- CREATE TEMP TABLE _glps AS
SELECT *
FROM
    ( VALUES
          ('00002245780'),
          ('00002246080'),
          ('00002247180'),
          ('00002248480'),
          ('00002249580'),
          ('00002250661'),
          ('00002250680'),
          ('00169291115'),
          ('00169291197'),
          ('00169450114'),
          ('00169450514'),
          ('00169451714'),
          ('00169452414'),
          ('00169452514'),
          ('00169452594'),
          ('50090582400'),
          ('00169406012'),
          ('00169406013'),
          ('00169406090'),
          ('00169406099'),
          ('50090450300'),
          ('50090285300'),
          ('00002143361'),
          ('00002143380'),
          ('00002143461'),
          ('00002143480'),
          ('50090348300'),
          ('50090348400'),
          ('00002223661'),
          ('00002223680'),
          ('00002318261'),
          ('00002318280'),
          ('50090546700'),
          ('50090645300'),
          ('50090645600'),
          ('50090657100'),
          ('00024576105'),
          ('00024576101'),
          ('00169280015'),
          ('00169280090'),
          ('00169280097'),
          ('50090425700'),
          ('00169430713'),
          ('00169431413'),
          ('00169430313'),
          ('00169430393'),
          ('00169430330'),
          ('00169430399'),
          ('00169430730'),
          ('00169431430'),
          ('00169413212'),
          ('00169413297'),
          ('00169413602'),
          ('70518214300'),
          ('00169413013'),
          ('50090513800'),
          ('50090513900'),
          ('00169418113'),
          ('00169418197'),
          ('00169477212'),
          ('00169477297'),
          ('50090594900'),
          ('50090605100'),
          ('00002145780'),
          ('00002146080'),
          ('00002147180'),
          ('00002148480'),
          ('00002149580'),
          ('00002150661'),
          ('00002150680'),
          ('00002115201'),
          ('00002124301'),
          ('00002221401'),
          ('00002234001'),
          ('00002242301'),
          ('00002300201'),
          ('00310652401'),
          ('00310651201'),
          ('00310651285') ) x(ndc);

-- CREATE UNIQUE INDEX ON _glps(ndc);

INSERT
INTO
    _glps (ndc)
;

WITH
    codes AS ( SELECT *
               FROM
                   analytics.ref.med_adherence_value_sets m
               WHERE
                     value_set_id = 'DIABETES_MEDICATIONS'
                 AND value_set_subgroup = 'GIP/GLP-1 RECEPTOR AGONISTS'
                 AND thru_date >= _boy(NOW()::DATE) )
SELECT *
FROM
    fdw_member_doc.qm_pm_med_adh_metrics m
WHERE
      NOT EXISTS( SELECT
                      1
                  FROM
                      fdw_member_doc.patient_medication_fills mf
                      LEFT JOIN codes glp ON glp.code = mf.ndc
                  WHERE
                        mf.patient_id = m.patient_id
                    AND EXTRACT('year' FROM mf.start_date) = 2024
                    AND mf.measure_key = 'med_adherence_diabetes'
                    AND glp.code ISNULL )
      -- glp exists
  AND EXISTS( SELECT
                  1
              FROM
                  fdw_member_doc.patient_medication_fills mf
                  JOIN codes glp ON glp.code = mf.ndc
              WHERE
                    mf.patient_id = m.patient_id
                AND EXTRACT('year' FROM mf.start_date) = 2024
                AND mf.measure_key = 'med_adherence_diabetes' );





