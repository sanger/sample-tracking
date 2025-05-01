-- EVENT: OFFSET 60 MINUTE
-- TABLE: seq_ops_tracking_per_plate

SET @_cutoff = DATE_SUB(NOW(), INTERVAL 2 YEAR);
SET @_rt_sample = (SELECT id FROM [events].role_types WHERE `key`='sample');
SET @_rt_project = (SELECT id FROM [events].role_types WHERE `key`='project');
SET @_et_order_made = (SELECT id FROM [events].event_types WHERE `key`='order_made');

CREATE TEMPORARY TABLE _order_made (
  event_id INT NOT NULL PRIMARY KEY
, project_subject_id INT NULL
, library_type VARCHAR(255) NULL
, INDEX ix_order_made_project (project_subject_id)
);

INSERT INTO _order_made (event_id)
SELECT e.id
FROM [events].events e
WHERE e.event_type_id=@_et_order_made
  AND e.occured_at >= @_cutoff
;

UPDATE _order_made om
  JOIN [events].roles r ON (r.event_id=om.event_id AND r.role_type_id=@_rt_project)
SET om.project_subject_id = r.subject_id
;

UPDATE _order_made om
  JOIN [events].metadata md ON (md.event_id=om.event_id AND md.`key`='library_type')
SET om.library_type = md.value
WHERE md.value IS NOT NULL
;

CREATE TEMPORARY TABLE _sample_order_made (
  subject_id INT NOT NULL
, event_id INT NOT NULL
, PRIMARY KEY (subject_id, event_id)
);

INSERT INTO _sample_order_made
SELECT DISTINCT r.subject_id, om.event_id
FROM _order_made om
  JOIN [events].roles r ON (om.event_id=r.event_id AND r.role_type_id=@_rt_sample)
;

INSERT INTO [reporting].seq_ops_tracking_per_plate (
  study_name
, study_id
, programme
, faculty_sponsor
, data_access_group
, manifest_created
, manifest_uploaded
, manifest_plate_barcode
, library_type_ordered
, library_type
, bait_names
, project_name
, sequencing_cost_code
, platform
, labware_received
, order_made_samples
, order_made_first
, order_made_last
, working_dilution_samples
, working_dilution_first
, working_dilution_last
, library_start_samples
, library_start_first
, library_start_last
, library_complete_samples
, library_complete_first
, library_complete_last
, sequencing_run_start_samples
, sequencing_run_start_first
, sequencing_run_start_last
, sequencing_qc_fail_samples
, sequencing_qc_pass_samples
, sequencing_qc_complete_first
, sequencing_qc_complete_last
, irods_root_collections
)
WITH sample_events AS (
    SELECT wh_event_id, event_type, occured_at, subject_uuid_bin
    FROM [events].flat_events_view
    WHERE role_type = 'sample'
      AND event_type IN ('sample_manifest.updated', 'labware.received', 'library_start', 'library_complete', 'sequencing_start', 'sequencing_complete', 'order_made')
      AND occured_at >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
    GROUP BY wh_event_id, subject_uuid_bin
),
labware_manifest_created_event AS (
    SELECT MIN(occured_at) AS occured_at, subject_friendly_name
    FROM [events].flat_events_view
    WHERE role_type = 'labware'
      AND event_type =  'sample_manifest.created'
      AND occured_at >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
    GROUP BY subject_friendly_name
)

SELECT
    sample_flowcell_view.study_name,
    sample_flowcell_view.id_study_lims AS study_id,
    sample_flowcell_view.programme,
    sample_flowcell_view.faculty_sponsor,
    sample_flowcell_view.data_access_group,
    labware_manifest_created_event.occured_at AS manifest_created,
    MIN(IF(sample_events.event_type = 'sample_manifest.updated', sample_events.occured_at, NULL)) manifest_uploaded,
    sample_flowcell_view.labware_human_barcode manifest_plate_barcode,
    GROUP_CONCAT(DISTINCT om.library_type SEPARATOR '; ') AS library_type_ordered,
    GROUP_CONCAT(DISTINCT sample_flowcell_view.pipeline_id_lims SEPARATOR '; ') AS library_type,
    GROUP_CONCAT(DISTINCT sample_flowcell_view.bait_name SEPARATOR '; ') AS bait_names,
    GROUP_CONCAT(DISTINCT project_subject.friendly_name SEPARATOR '; ') AS project_name,
    GROUP_CONCAT(DISTINCT sample_flowcell_view.sequencing_cost_code SEPARATOR '; ') AS sequencing_cost_code,
    GROUP_CONCAT(DISTINCT sample_flowcell_view.instrument_model SEPARATOR '; ') AS platform,
    MIN(IF(sample_events.event_type = 'labware.received', sample_events.occured_at, NULL)) labware_received,
    COUNT(DISTINCT(IF(sample_events.event_type = 'order_made', sample_events.subject_uuid_bin, NULL))) order_made_samples,
    MIN(IF(sample_events.event_type = 'order_made', sample_events.occured_at, NULL)) order_made_first,
    MAX(IF(sample_events.event_type = 'order_made', sample_events.occured_at, NULL)) order_made_last,
    COUNT(DISTINCT(IF(sample_flowcell_view.qc_early IS NOT NULL, sample_flowcell_view.sample_uuid, NULL))) working_dilution_samples, -- Count number of unique samples for this plate that have non-null QC timestamps for dilution
    MIN(sample_flowcell_view.qc_early) working_dilution_first,
    MAX(sample_flowcell_view.qc_late) working_dilution_last,
    COUNT(DISTINCT(IF(sample_events.event_type = 'library_start', sample_events.subject_uuid_bin, NULL))) library_start_samples,
    MIN(IF(sample_events.event_type = 'library_start', sample_events.occured_at, NULL)) library_start_first,
    MAX(IF(sample_events.event_type = 'library_start', sample_events.occured_at, NULL)) library_start_last,
    COUNT(DISTINCT(IF(sample_events.event_type = 'library_complete', sample_events.subject_uuid_bin, NULL))) library_complete_samples,
    MIN(IF(sample_events.event_type = 'library_complete', sample_events.occured_at, NULL)) library_complete_first,
    MAX(IF(sample_events.event_type = 'library_complete', sample_events.occured_at, NULL)) library_complete_last,
    COUNT(DISTINCT(IF(sample_events.event_type = 'sequencing_start', sample_events.subject_uuid_bin, NULL))) sequencing_run_start_samples,
    MIN(IF(sample_events.event_type = 'sequencing_start', sample_events.occured_at, NULL)) sequencing_run_start_first,
    MAX(IF(sample_events.event_type = 'sequencing_start', sample_events.occured_at, NULL)) sequencing_run_start_last,
    COUNT(DISTINCT(IF(sample_events.event_type = 'sequencing_complete' AND md.value='failed', sample_events.subject_uuid_bin, NULL))) sequencing_qc_fail_samples,
    COUNT(DISTINCT(IF(sample_events.event_type = 'sequencing_complete' AND md.value='passed', sample_events.subject_uuid_bin, NULL))) sequencing_qc_pass_samples,
    MIN(IF(sample_events.event_type = 'sequencing_complete', sample_events.occured_at, NULL)) sequencing_qc_complete_first,
    MAX(IF(sample_events.event_type = 'sequencing_complete', sample_events.occured_at, NULL)) sequencing_qc_complete_last,
    GROUP_CONCAT(DISTINCT irods.irods_root_collection ORDER BY irods.irods_root_collection SEPARATOR '; ' ) AS irods_root_collections

FROM [reporting].sample_flowcell_view
    LEFT JOIN sample_events ON (sample_events.subject_uuid_bin = sample_flowcell_view.sample_uuid)
    LEFT JOIN labware_manifest_created_event ON (labware_manifest_created_event.subject_friendly_name = sample_flowcell_view.labware_human_barcode)
    LEFT JOIN [warehouse].iseq_product_metrics AS product_metrics ON product_metrics.id_iseq_flowcell_tmp = sample_flowcell_view.id_iseq_flowcell_tmp
    LEFT JOIN [warehouse].seq_product_irods_locations irods ON irods.id_product=product_metrics.id_iseq_product
    LEFT JOIN [events].metadata md ON (
        sample_events.event_type='sequencing_complete'
        AND sample_events.wh_event_id=md.event_id
        AND md.key='result'
    )
    LEFT JOIN [events].subjects sample_subject ON (sample_flowcell_view.sample_uuid=sample_subject.uuid)
    LEFT JOIN _sample_order_made som ON (som.subject_id=sample_subject.id)
    LEFT JOIN _order_made om ON (som.event_id=om.event_id)
    LEFT JOIN [events].subjects project_subject ON (om.project_subject_id=project_subject.id)

GROUP BY manifest_plate_barcode
-- filter out plates where we have no real information, but leave in rows where there is something for future debugging / smoke testing
HAVING manifest_uploaded IS NOT NULL
    OR labware_received IS NOT NULL
    OR library_start_samples != 0
    OR library_complete_samples != 0
    OR sequencing_run_start_samples != 0
    OR sequencing_qc_complete_last IS NOT NULL
;

DROP TEMPORARY TABLE _order_made;
DROP TEMPORARY TABLE _sample_order_made;
