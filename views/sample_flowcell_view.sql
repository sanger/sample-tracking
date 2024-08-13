CREATE OR REPLACE VIEW sample_flowcell_view AS

-- limit to studies that have something submitted recently
WITH study_uuids AS (
  SELECT BIN_TO_UUID(s.uuid) AS uuid
  FROM [events].role_types rt
    JOIN [events].roles r ON (r.role_type_id=rt.id)
    JOIN [events].subjects s ON (r.subject_id=s.id)
    JOIN [events].events e ON (r.event_id=e.id)
    JOIN [events].event_types et ON (e.event_type_id=et.id)
  WHERE rt.key='study'
    AND et.key='sample_manifest.updated'
    AND e.occured_at >= NOW() - INTERVAL 2 YEAR
  GROUP BY s.id
)
SELECT
  iseq_flowcell.id_iseq_flowcell_tmp,
  iseq_product_metrics.id_iseq_product,
  iseq_product_metrics.id_run,
  study.id_study_tmp,
  study.name study_name,
  study.id_study_lims,
  study.data_access_group,
  UUID_TO_BIN(sample.uuid_sample_lims) AS sample_uuid,
  iseq_flowcell.pipeline_id_lims,
  iseq_flowcell.cost_code AS sequencing_cost_code,
  iseq_run_lane_metrics.instrument_model,
  stock_resource.id_stock_resource_tmp,
  stock_resource.labware_human_barcode,
  MIN(qc_result.recorded_at) qc_early,
  MAX(qc_result.recorded_at) qc_late
  
FROM study_uuids
  
JOIN [warehouse].study ON study.uuid_study_lims = study_uuids.uuid
JOIN [warehouse].stock_resource ON stock_resource.id_study_tmp = study.id_study_tmp
JOIN [warehouse].sample ON sample.id_sample_tmp = stock_resource.id_sample_tmp

LEFT JOIN [warehouse].qc_result ON (
  qc_result.id_sample_tmp = sample.id_sample_tmp
  AND qc_result.assay = 'Working Dilution - Plate Reader v1.0'
)
  
LEFT JOIN [warehouse].iseq_flowcell ON iseq_flowcell.id_sample_tmp = sample.id_sample_tmp
LEFT JOIN [warehouse].iseq_product_metrics ON iseq_product_metrics.id_iseq_flowcell_tmp = iseq_flowcell.id_iseq_flowcell_tmp
LEFT JOIN [warehouse].iseq_run_lane_metrics ON iseq_run_lane_metrics.id_run = iseq_product_metrics.id_run
  
WHERE
-- allow pipelines where no QC result is measured OR where it is measured recently
  qc_result.id_qc_result_tmp IS NULL
  OR
  qc_result.recorded_at >= NOW() - INTERVAL 2 YEAR
 
 -- by grouping here we are assuming sequencing only happens once
GROUP BY sample.id_sample_tmp
;
