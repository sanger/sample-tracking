CREATE OR REPLACE VIEW sample_flowcell_view AS
  
 -- limit to studies that have something submitted in the last six months
WITH study_names AS (
  SELECT DISTINCT(subject_friendly_name) study_name
  FROM [events].flat_events_view
  WHERE role_type = 'study'
  AND event_type = 'sample_manifest.updated'
  AND occured_at >=  DATE_SUB(NOW(), INTERVAL 6 MONTH)
)
  
SELECT
  iseq_flowcell.id_iseq_flowcell_tmp,
  iseq_product_metrics.id_iseq_product,
  iseq_product_metrics.id_run,
  study.id_study_tmp,
  study.name study_name,
  study.id_study_lims,
  UNHEX(REPLACE(sample.uuid_sample_lims, '-', '')) sample_uuid,
  iseq_flowcell.pipeline_id_lims,
  iseq_flowcell.cost_code,
  iseq_run_lane_metrics.instrument_model,
  stock_resource.id_stock_resource_tmp,
  stock_resource.labware_human_barcode,
  MIN(qc_result.recorded_at) qc_early,
  MAX(qc_result.recorded_at) qc_late
  
FROM study_names
  
JOIN [warehouse].study ON study.name = study_names.study_name
JOIN [warehouse].stock_resource ON stock_resource.id_study_tmp = study.id_study_tmp
JOIN [warehouse].sample ON sample.id_sample_tmp = stock_resource.id_sample_tmp
  
LEFT JOIN [warehouse].qc_result ON qc_result.id_sample_tmp = sample.id_sample_tmp
  
LEFT JOIN [warehouse].iseq_flowcell ON iseq_flowcell.id_sample_tmp = sample.id_sample_tmp
LEFT JOIN [warehouse].iseq_product_metrics ON iseq_product_metrics.id_iseq_flowcell_tmp = iseq_flowcell.id_iseq_flowcell_tmp
LEFT JOIN [warehouse].iseq_run_lane_metrics ON iseq_run_lane_metrics.id_run = iseq_product_metrics.id_run
  
WHERE
-- allow pipelines where no QC result is measure OR where it is measured in the last 6 months
  [warehouse].qc_result.assay IS NULL
  OR
    ([warehouse].qc_result.assay = 'Working Dilution - Plate Reader v1.0'
      AND [warehouse].qc_result.recorded_at >=  DATE_SUB(NOW(), INTERVAL 6 MONTH) )
 
 -- by grouping here we are assuming sequencing only happens once
GROUP BY sample_uuid
;
