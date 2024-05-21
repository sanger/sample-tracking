CREATE OR REPLACE VIEW `${REPORTING_DB}`.`sample_tracking_view` AS


WITH sample_events AS (
    SELECT event_type, occured_at, subject_uuid_bin FROM `${EVENTS_DB}`.flat_events_view
    WHERE role_type = 'sample'
      AND event_type IN ('sample_manifest.updated', 'labware.received', 'library_start', 'library_complete', 'sequencing_start', 'sequencing_complete')
      AND occured_at >= DATE_SUB(NOW(), INTERVAL 6 MONTH)
)

SELECT
    sample_flowcell_view.study_name,
    MIN(IF(sample_events.event_type = 'sample_manifest.updated', sample_events.occured_at, NULL)) manifest_uploaded,
    sample_flowcell_view.labware_human_barcode manifest_plate_barcode,
    sample_flowcell_view.pipeline_id_lims library_type,
    sample_flowcell_view.cost_code,
    sample_flowcell_view.instrument_model platform,
    MIN(IF(sample_events.event_type = 'labware.received', sample_events.occured_at, NULL)) labware_received,
    COUNT(DISTINCT(IF(sample_flowcell_view.qc_early IS NOT NULL, sample_flowcell_view.sample_uuid, NULL))) work_started_count, -- Count number of unique samples for this plate that have non-null QC timestamps for dilution
    MIN(sample_flowcell_view.qc_early) work_started_first,
    MAX(sample_flowcell_view.qc_late) work_started_last,
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
    MAX(IF(sample_events.event_type = 'sequencing_complete', sample_events.occured_at, NULL)) sequencing_qc_complete_last


FROM `${REPORTING_DB}`.sample_flowcell_view

         LEFT JOIN sample_events
                   ON sample_events.subject_uuid_bin = sample_flowcell_view.sample_uuid


GROUP BY plate_barcode
-- filter out plates where we have no real information, but leave in rows where there is something for future debugging / smoke testing
HAVING manifest_created IS NOT NULL
    OR manifest_submitted IS NOT NULL
    OR library_start_count != 0
    OR library_complete_count != 0
    OR sequencing_start_count != 0
    OR sequencing_complete_count !=0;
