select * from (
(select *
from raw.centene_tcoc_inpatient_census ctic
where inbound_file_id = 15531308 -- 7/16 file (good)
limit 5)
union
(select *
from raw.centene_tcoc_inpatient_census ctic
where inbound_file_id = 15543861 -- 7/17 file (bad) 3 files after this are bad as well
limit 5)) x
order by inbound_file_id;

select * from (
(select *
from raw.centene_tcoc_discharges ctic
where inbound_file_id = 15531308 -- 7/16 file (good)
limit 5)
union
(select *
from raw.centene_tcoc_discharges ctic
where inbound_file_id = 15543861 -- 7/17 file (bad) 3 files after this are bad as well
limit 5)) x
order by inbound_file_id;

SELECT
    *
--     meta
--   , meta ->> 'file_name'
FROM
    raw.patient_rx_adherence_roster_uhc u;

;