CREATE OR REPLACE VIEW [reporting].sample_tracking_view AS

WITH sample_events AS (
    SELECT event_type, occured_at, subject_uuid_bin
    FROM [events].flat_events_view
    WHERE role_type = 'sample'
      AND event_type IN ('sample_manifest.updated', 'labware.received', 'library_start', 'library_complete', 'sequencing_start', 'sequencing_complete', 'order_made')
      AND occured_at >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
)

SELECT
    sample_flowcell_view.study_name,
    sample_flowcell_view.id_study_lims AS study_id,
    MIN(IF(sample_events.event_type = 'sample_manifest.updated', sample_events.occured_at, NULL)) manifest_uploaded,
    sample_flowcell_view.labware_human_barcode manifest_plate_barcode,
    GROUP_CONCAT(DISTINCT sample_flowcell_view.pipeline_id_lims SEPARATOR ';') AS library_type,
    GROUP_CONCAT(DISTINCT sample_flowcell_view.sequencing_cost_code SEPARATOR ';') AS sequencing_cost_code,
    GROUP_CONCAT(DISTINCT sample_flowcell_view.instrument_model SEPARATOR ';') AS platform,
    MIN(IF(sample_events.event_type = 'labware.received', sample_events.occured_at, NULL)) labware_received,
    COUNT(DISTINCT(IF(sample_events.event_type = 'order_made', sample_events.subject_uuid_bin, NULL))) order_made_count,
    MIN(IF(sample_events.event_type = 'order_made', sample_events.occured_at, NULL)) order_made_first,
    MAX(IF(sample_events.event_type = 'order_made', sample_events.occured_at, NULL)) order_made_last,
    COUNT(DISTINCT(IF(sample_flowcell_view.qc_early IS NOT NULL, sample_flowcell_view.sample_uuid, NULL))) working_dilution_count, -- Count number of unique samples for this plate that have non-null QC timestamps for dilution
    MIN(sample_flowcell_view.qc_early) working_dilution_first,
    MAX(sample_flowcell_view.qc_late) working_dilution_last,
    COUNT(DISTINCT(IF(sample_events.event_type = 'library_start', sample_events.subject_uuid_bin, NULL))) library_start_count,
    MIN(IF(sample_events.event_type = 'library_start', sample_events.occured_at, NULL)) library_start_first,
    MAX(IF(sample_events.event_type = 'library_start', sample_events.occured_at, NULL)) library_start_last,
    COUNT(DISTINCT(IF(sample_events.event_type = 'library_complete', sample_events.subject_uuid_bin, NULL))) library_complete_count,
    MIN(IF(sample_events.event_type = 'library_complete', sample_events.occured_at, NULL)) library_complete_first,
    MAX(IF(sample_events.event_type = 'library_complete', sample_events.occured_at, NULL)) library_complete_last,
    COUNT(DISTINCT(IF(sample_events.event_type = 'sequencing_start', sample_events.subject_uuid_bin, NULL))) sequencing_run_start_count,
    MIN(IF(sample_events.event_type = 'sequencing_start', sample_events.occured_at, NULL)) sequencing_run_start_first,
    MAX(IF(sample_events.event_type = 'sequencing_start', sample_events.occured_at, NULL)) sequencing_run_start_last,
    COUNT(DISTINCT(IF(sample_events.event_type = 'sequencing_complete', sample_events.subject_uuid_bin, NULL))) sequencing_qc_complete_count,
    MIN(IF(sample_events.event_type = 'sequencing_complete', sample_events.occured_at, NULL)) sequencing_qc_complete_first,
    MAX(IF(sample_events.event_type = 'sequencing_complete', sample_events.occured_at, NULL)) sequencing_qc_complete_last,
    GROUP_CONCAT(DISTINCT irods.irods_root_collection ORDER BY irods.irods_root_collection SEPARATOR '; ' ) AS irods_root_collections


FROM [reporting].sample_flowcell_view
    LEFT JOIN sample_events ON (sample_events.subject_uuid_bin = sample_flowcell_view.sample_uuid)
    LEFT JOIN [reporting].iseq_product_metrics AS product_metrics ON product_metrics.id_iseq_flowcell_tmp = sample_flowcell_view.id_iseq_flowcell_tmp
    LEFT JOIN [reporting].seq_product_irods_locations irods ON irods.id_product=product_metrics.id_iseq_product

GROUP BY manifest_plate_barcode
-- filter out plates where we have no real information, but leave in rows where there is something for future debugging / smoke testing
HAVING manifest_uploaded IS NOT NULL
    OR labware_received IS NOT NULL
    OR library_start_count != 0
    OR library_complete_count != 0
    OR sequencing_run_start_count != 0
    OR sequencing_qc_complete_count !=0
;
