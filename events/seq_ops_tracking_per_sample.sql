-- In DBVis this would be @delimiter $$;
DELIMITER $$

CREATE EVENT [reporting].seq_ops_tracking_per_sample_event
ON SCHEDULE
  EVERY 1 DAY
  STARTS (TIMESTAMP(CURRENT_DATE) + INTERVAL 1 DAY + INTERVAL 1 HOUR) -- 1 AM
COMMENT 'Populates the seq_ops_tracking_per_sample table'
DO BEGIN
  TRUNCATE TABLE [reporting].seq_ops_tracking_per_sample;
  INSERT INTO [reporting].seq_ops_tracking_per_sample (
    id_sample_lims_composite
    , id_lims
    , id_sample_lims
    , sanger_sample_id
    , supplier_name
    , submitted_study_name
    , sequenced_study_name
    , manifest_plate_barcode
    , study_id
    , programme
    , faculty_sponsor
    , data_access_group
    , library_type
    , bait_names
    , sequencing_cost_code
    , platform
    , manifest_created
    , manifest_uploaded
    , labware_received
    , order_made
    , working_dilution
    , library_start
    , library_complete
    , sequencing_run_start
    , sequencing_qc_complete
  )
  WITH
  -- Query filtering for the events we are interested in in the given period
  sample_events AS (
      SELECT wh_event_id, event_type, occured_at, subject_uuid_bin
      FROM [events].flat_events_view
      WHERE role_type = 'sample'
        AND event_type IN ('sample_manifest.updated', 'labware.received', 'library_start', 'library_complete', 'sequencing_start', 'sequencing_complete', 'order_made')
        AND occured_at >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
      GROUP BY wh_event_id, subject_uuid_bin
  ),
  -- Query filtering for the earliest sample submissions we are interested in in the given period
  labware_manifest_created_event AS (
      SELECT subject_friendly_name AS labware_human_barcode, MIN(occured_at) AS occured_at
      FROM [events].flat_events_view
      WHERE role_type = 'labware'
        AND event_type =  'sample_manifest.created'
        AND occured_at >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
      GROUP BY subject_friendly_name
  ),
  -- Query of study ids that have had something happen in the time interval given
  studies_of_interest AS (
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
  ),
  -- Query of samples of interest from studies of interest
  samples_of_interest AS (
      SELECT
      sample.id_sample_tmp AS id_sample_tmp,
      sample.id_lims AS id_lims,
      sample.id_sample_lims AS id_sample_lims,
      sample.sanger_sample_id AS sanger_sample_id,
      sample.supplier_name AS supplier_name,
      UUID_TO_BIN(sample.uuid_sample_lims) AS sample_uuid_bin, -- convert to same uuid format used in events
      studies_of_interest.uuid AS uuid_study_lims,
      study.id_study_tmp,
      study.name AS submitted_study_name,
      study.programme,
      study.faculty_sponsor,
      study.id_study_lims,
      study.data_access_group,
      stock_resource.labware_human_barcode
      FROM studies_of_interest
      JOIN [warehouse].study ON study.uuid_study_lims = studies_of_interest.uuid
      JOIN [warehouse].stock_resource ON stock_resource.id_study_tmp = study.id_study_tmp
      JOIN [warehouse].sample ON sample.id_sample_tmp = stock_resource.id_sample_tmp
  ),
  -- Query getting working dilution timestamps per sample
  dilution_timestamps AS (
      SELECT
      samples_of_interest.id_sample_tmp,
      -- QC working dilution timestamps earliest and latest
      MIN(qc_result.recorded_at) qc_early,
      MAX(qc_result.recorded_at) qc_late
      FROM samples_of_interest
      LEFT JOIN [warehouse].qc_result ON (
                      qc_result.id_sample_tmp = samples_of_interest.id_sample_tmp
                      AND qc_result.assay = 'Working Dilution - Plate Reader v1.0'
      )

      WHERE
      -- allow pipelines where no QC result is measured OR where it is measured recently
      qc_result.id_qc_result_tmp IS NULL
      OR
      qc_result.recorded_at >= NOW() - INTERVAL 2 YEAR
  ),
  -- Query linking samples to qc results and flowcell/run information.
  -- This will have multiple rows per sequencing attempt
  sample_flowcell AS (
    SELECT
      samples_of_interest.id_sample_tmp,
      samples_of_interest.id_lims AS id_lims,
      samples_of_interest.id_sample_lims AS id_sample_lims,
      samples_of_interest.sanger_sample_id AS sanger_sample_id,
      samples_of_interest.supplier_name AS supplier_name,
      samples_of_interest.sample_uuid_bin AS sample_uuid_bin, -- convert to same uuid format used in events
      samples_of_interest.id_study_tmp,
      samples_of_interest.submitted_study_name AS submitted_study_name,
      study.name AS sequenced_study_name,
      samples_of_interest.programme,
      samples_of_interest.faculty_sponsor,
      samples_of_interest.id_study_lims,
      samples_of_interest.data_access_group,
      samples_of_interest.labware_human_barcode,
      iseq_flowcell.id_iseq_flowcell_tmp,
      iseq_product_metrics.id_iseq_product,
      iseq_product_metrics.id_run,
      iseq_flowcell.pipeline_id_lims,
      iseq_flowcell.bait_name,
      iseq_flowcell.cost_code AS sequencing_cost_code,
      iseq_run_lane_metrics.instrument_model

    FROM samples_of_interest

    -- Left join to flowcell so we don't lose samples that have not yet been sequenced
    LEFT JOIN [warehouse].iseq_flowcell ON iseq_flowcell.id_sample_tmp = samples_of_interest.id_sample_tmp
    LEFT JOIN [warehouse].iseq_product_metrics ON iseq_product_metrics.id_iseq_flowcell_tmp = iseq_flowcell.id_iseq_flowcell_tmp
    LEFT JOIN [warehouse].iseq_run_lane_metrics ON iseq_run_lane_metrics.id_run = iseq_product_metrics.id_run
    LEFT JOIN [warehouse].study ON iseq_flowcell.id_study_tmp = study.id_study_tmp
  )
  -- Desired report grouped by sample.
  -- Select the first timestamp for any mulitple timestamps for a given sample; concat any other fields
  SELECT
      -- the combination of id_lims and id_sample_lims is unique and we will group on this combination
      CONCAT(sample_flowcell.id_lims, '_', sample_flowcell.id_sample_lims) AS id_sample_lims_composite,
      sample_flowcell.id_lims, -- assumed unique by grouping  on id_sample_lims_composite
      sample_flowcell.id_sample_lims, -- assumed unique by grouping  on id_sample_lims_composite
      sample_flowcell.sanger_sample_id, -- assumed unique by grouping  on id_sample_lims_composite
      sample_flowcell.supplier_name, -- assumed unique by grouping  on id_sample_lims_composite
      sample_flowcell.submitted_study_name, -- assumed unique by grouping  on id_sample_lims_composite
      GROUP_CONCAT(DISTINCT sample_flowcell.sequenced_study_name SEPARATOR '; ') AS sequenced_study_name,
      GROUP_CONCAT(DISTINCT sample_flowcell.labware_human_barcode SEPARATOR '; ') AS manifest_plate_barcode,
      GROUP_CONCAT(DISTINCT sample_flowcell.id_study_lims SEPARATOR '; ') AS study_id,
      GROUP_CONCAT(DISTINCT sample_flowcell.programme SEPARATOR '; ') AS programme,
      GROUP_CONCAT(DISTINCT sample_flowcell.faculty_sponsor SEPARATOR '; ') AS faculty_sponsor,
      GROUP_CONCAT(DISTINCT sample_flowcell.data_access_group SEPARATOR '; ') AS data_access_group,
      GROUP_CONCAT(DISTINCT sample_flowcell.pipeline_id_lims SEPARATOR '; ') AS library_type,
      GROUP_CONCAT(DISTINCT sample_flowcell.bait_name SEPARATOR '; ') AS bait_names,
      GROUP_CONCAT(DISTINCT sample_flowcell.sequencing_cost_code SEPARATOR '; ') AS sequencing_cost_code,
      GROUP_CONCAT(DISTINCT sample_flowcell.instrument_model SEPARATOR '; ') AS platform,
      MIN(labware_manifest_created_event.occured_at) AS manifest_created,
      MIN(IF(sample_events.event_type = 'sample_manifest.updated', sample_events.occured_at, NULL)) AS manifest_uploaded,
      MIN(IF(sample_events.event_type = 'labware.received', sample_events.occured_at, NULL)) AS labware_received,
      MIN(IF(sample_events.event_type = 'order_made', sample_events.occured_at, NULL)) order_made,
      MIN(dilution_timestamps.qc_early) working_dilution,
      MIN(IF(sample_events.event_type = 'library_start', sample_events.occured_at, NULL)) library_start,
      MIN(IF(sample_events.event_type = 'library_complete', sample_events.occured_at, NULL)) library_complete,
      MIN(IF(sample_events.event_type = 'sequencing_start', sample_events.occured_at, NULL)) sequencing_run_start,
      MIN(IF(sample_events.event_type = 'sequencing_complete', sample_events.occured_at, NULL)) sequencing_qc_complete

  FROM sample_flowcell
  LEFT JOIN labware_manifest_created_event ON (labware_manifest_created_event.labware_human_barcode = sample_flowcell.labware_human_barcode)
  LEFT JOIN sample_events ON (sample_events.subject_uuid_bin = sample_flowcell.sample_uuid_bin)
  LEFT JOIN dilution_timestamps ON (dilution_timestamps.id_sample_tmp=sample_flowcell.id_sample_tmp)
  LEFT JOIN [events].metadata md ON (
              sample_events.event_type='sequencing_complete'
              AND sample_events.wh_event_id=md.event_id
              AND md.key='result'
          )

  -- We can speed up query by restricting to a given programme.
  -- With the filter it takes about 3 mins.
  -- WHERE programme IN ('Pathogen Variation', 'Malaria')

  GROUP BY id_sample_lims_composite


  ORDER BY id_sample_lims_composite
  ;
END $$

-- In DBVis this would be @delimiter ;$$
DELIMITER ;
