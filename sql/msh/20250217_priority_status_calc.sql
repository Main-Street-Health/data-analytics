CREATE OR REPLACE FUNCTION stage.calculate_priority_status(
    metric record,
    mco_pdc decimal(5, 2)
)
RETURNS text AS $$
DECLARE
    failed_last_yr_high_priority_cutoff date;
    calculated_pdc decimal(5, 2);
    priority_pdc decimal(5, 2);
BEGIN
    SELECT (date_trunc('year', now()) + '4 months'::interval)::date
    INTO failed_last_yr_high_priority_cutoff
    ;

    -- Calculate PDC
    calculated_pdc := CASE
        WHEN (metric.days_covered_to_period_end + metric.days_not_covered) = 0 THEN 0
        ELSE metric.days_covered_to_period_end::decimal(5, 2) / NULLIF((metric.days_covered_to_period_end + metric.days_not_covered), 0)
    END;

    -- Use the lower of calculated PDC and MCO PDC
    priority_pdc := LEAST(
        calculated_pdc,
        metric.pdc_to_date,
        COALESCE(mco_pdc, calculated_pdc)
    );

    -- Replicate the CASE statement logic with the lower PDC
    RETURN CASE
        WHEN metric.next_fill_date >= now()::date then metric.priority_status
        WHEN metric.failed_last_year AND now()::date < failed_last_yr_high_priority_cutoff THEN 'high'
        WHEN metric.failed_last_year AND priority_pdc < 0.9 THEN 'high'
        WHEN metric.days_supply <= 27 AND priority_pdc < 0.9 THEN 'high'
        WHEN priority_pdc < 0.85 THEN 'high'
        WHEN priority_pdc < 0.9 THEN 'medium'
        ELSE 'low'
    END;
END;
$$ LANGUAGE plpgsql;

-- verification
SELECT
    m.measure_source_key
  , pm.measure_status_key
  , pm.is_active
  , failed_last_year
  , pdc_to_date
  , priority_pdc
  , (CASE
    WHEN (days_covered_to_period_end + days_not_covered) = 0 THEN 0
    ELSE days_covered_to_period_end::NUMERIC / NULLIF((days_covered_to_period_end + days_not_covered), 0)
    END)::DECIMAL(16, 2) case_pdc
  , next_fill_date
  , days_supply
  , priority_status
  , stage.calculate_priority_status(
            m.*,
            m.pdc_to_date
    )
FROM
    qm_pm_med_adh_metrics m
    JOIN qm_patient_measures pm ON m.patient_measure_id = pm.id
WHERE
      measure_year = 2025
  AND priority_status !=
      stage.calculate_priority_status(
              m.*,
              m.pdc_to_date
      )
;


-- analytics
CREATE OR REPLACE FUNCTION public.med_adh_calculate_priority_status(
    metric record,
    mco_pdc decimal(5, 2)
)
RETURNS text AS $$
DECLARE
    failed_last_yr_high_priority_cutoff date;
    calculated_pdc decimal(5, 2);
    priority_pdc decimal(5, 2);
BEGIN
    SELECT (date_trunc('year', now()) + '4 months'::interval)::date
    INTO failed_last_yr_high_priority_cutoff
    ;

    -- Calculate PDC
    calculated_pdc := CASE
        WHEN (metric.days_covered_to_period_end + metric.days_not_covered) = 0 THEN 0
        ELSE metric.days_covered_to_period_end::decimal(5, 2) / NULLIF((metric.days_covered_to_period_end + metric.days_not_covered), 0)
    END;

    -- Use the lower of calculated PDC and MCO PDC
    priority_pdc := LEAST(
        calculated_pdc,
        metric.pdc_to_date,
        COALESCE(mco_pdc, calculated_pdc)
    );

    -- Replicate the CASE statement logic with the lower PDC
    RETURN CASE
        WHEN metric.next_fill_date >= now()::date then metric.priority_status
        WHEN metric.failed_last_year AND now()::date < failed_last_yr_high_priority_cutoff THEN 'high'
        WHEN metric.failed_last_year AND priority_pdc < 0.9 THEN 'high'
        WHEN metric.days_supply <= 27 AND priority_pdc < 0.9 THEN 'high'
        WHEN priority_pdc < 0.85 THEN 'high'
        WHEN priority_pdc < 0.9 THEN 'medium'
        ELSE 'low'
    END;
END;
$$ LANGUAGE plpgsql;