-- EVENT: OFFSET 105 MINUTE
-- TABLE: seq_ops_tracking_per_sample

SET @_cutoff = DATE_SUB(NOW(), INTERVAL 2 YEAR);

SET @_rt = (SELECT id FROM [events].role_types WHERE `key`='sample');
SET @_et_mu = (SELECT id FROM [events].event_types WHERE `key`='sample_manifest.updated');
SET @_et_lr = (SELECT id FROM [events].event_types WHERE `key`='labware.received');
SET @_et_ls = (SELECT id FROM [events].event_types WHERE `key`='library_start');
SET @_et_lc = (SELECT id FROM [events].event_types WHERE `key`='library_complete');
SET @_et_ss = (SELECT id FROM [events].event_types WHERE `key`='sequencing_start');
SET @_et_sc = (SELECT id FROM [events].event_types WHERE `key`='sequencing_complete');
SET @_et_om = (SELECT id FROM [events].event_types WHERE `key`='order_made');

SET @_lwrt = (SELECT id FROM [events].role_types WHERE `key`='labware');
SET @_et_mc = (SELECT id FROM [events].event_types WHERE `key`='sample_manifest.created');

TRUNCATE TABLE relevant_samples;

INSERT INTO relevant_samples (subject_id)
SELECT DISTINCT r.subject_id
FROM [events].roles r
  JOIN [events].events e ON (r.event_id = e.id)
WHERE r.role_type_id = @_rt
  AND e.event_type_id IN (@_et_mu, @_et_lr, @_et_ls, @_et_lc, @_et_ss, @_et_sc, @_et_om)
  AND e.occured_at >= @_cutoff
;

UPDATE relevant_samples rs
  JOIN [events].subjects s ON (rs.subject_id=s.id)
SET rs.uuid = BIN_TO_UUID(s.uuid)
;

CREATE TEMPORARY TABLE _sample_event_ids (
  event_id INT NOT NULL PRIMARY KEY
);

INSERT INTO _sample_event_ids
SELECT e.id
FROM
   [events].events e
WHERE e.event_type_id IN (@_et_mu, @_et_lr, @_et_ls, @_et_lc, @_et_ss, @_et_sc, @_et_om)
  AND e.occured_at >= @_cutoff
;

CREATE TEMPORARY TABLE _sample_events (
  event_id INT NOT NULL
, subject_id INT NOT NULL
, PRIMARY KEY (event_id, subject_id)
);

INSERT INTO _sample_events (event_id, subject_id)
SELECT DISTINCT se.event_id, rs.subject_id
FROM _sample_event_ids se
  JOIN [events].roles r on (r.event_id=se.event_id)
  JOIN relevant_samples rs on (r.subject_id=rs.subject_id)
WHERE r.role_type_id=@_rt
;

CREATE TEMPORARY TABLE _labware_events (
  event_id INT NOT NULL
, subject_id INT NOT NULL
, PRIMARY KEY (event_id, subject_id)
);

INSERT INTO _labware_events
SELECT r.event_id, r.subject_id
FROM [events].roles r
  JOIN [events].events e ON (r.event_id=e.id)
WHERE r.role_type_id=@_lwrt
  AND e.event_type_id=@_et_mc
  AND e.occured_at >= @_cutoff
;

CREATE TEMPORARY TABLE _event_min (
  subject_id INT NOT NULL
, event_type_id INT NOT NULL
, occured_at DATETIME NOT NULL
, PRIMARY KEY (subject_id, event_type_id)
);

INSERT INTO _event_min
SELECT se.subject_id, e.event_type_id, MIN(e.occured_at)
FROM _sample_events se
  JOIN [events].events e ON (se.event_id=e.id)
GROUP BY se.subject_id, e.event_type_id
;

CREATE TEMPORARY TABLE _specific_event_min (
  subject_id INT NOT NULL PRIMARY KEY
, manifest_uploaded DATETIME NULL
, labware_received DATETIME NULL
, order_made DATETIME NULL
, library_start DATETIME NULL
, library_complete DATETIME NULL
, sequencing_run_start DATETIME NULL
, sequencing_qc_complete DATETIME NULL
);

INSERT INTO _specific_event_min
SELECT subject_id
, MIN(IF(event_type_id = @_et_mu, occured_at, NULL))
, MIN(IF(event_type_id = @_et_lr, occured_at, NULL))
, MIN(IF(event_type_id = @_et_om, occured_at, NULL))
, MIN(IF(event_type_id = @_et_ls, occured_at, NULL))
, MIN(IF(event_type_id = @_et_lc, occured_at, NULL))
, MIN(IF(event_type_id = @_et_ss, occured_at, NULL))
, MIN(IF(event_type_id = @_et_sc, occured_at, NULL))
FROM _event_min
GROUP BY subject_id
;

-- Main query

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
-- Query filtering for the earliest sample submissions we are interested in in the given period
labware_manifest_created_event AS (
    SELECT s.friendly_name AS labware_human_barcode, MIN(occured_at) AS occured_at
    FROM _labware_events le
    JOIN [events].subjects s ON (le.subject_id=s.id)
      JOIN [events].events e ON (le.event_id=e.id)
    GROUP BY le.subject_id
),
-- Query of samples of interest from studies of interest
samples_of_interest AS (
    SELECT
    sample.id_sample_tmp AS id_sample_tmp,
    sample.id_lims AS id_lims,
    sample.id_sample_lims AS id_sample_lims,
    sample.sanger_sample_id AS sanger_sample_id,
    sample.supplier_name AS supplier_name,
    sample.uuid_sample_lims,
    study.uuid_study_lims,
    study.id_study_tmp,
    study.name AS submitted_study_name,
    study.programme,
    study.faculty_sponsor,
    study.id_study_lims,
    study.data_access_group,
    stock_resource.labware_human_barcode
    FROM relevant_samples
    JOIN [warehouse].sample ON sample.uuid_sample_lims=relevant_samples.uuid
    JOIN [warehouse].stock_resource ON sample.id_sample_tmp = stock_resource.id_sample_tmp
    JOIN [warehouse].study ON stock_resource.id_study_tmp = study.id_study_tmp
),
-- Query getting working dilution timestamps per sample
dilution_timestamps AS (
    SELECT
      samples_of_interest.id_sample_tmp,
      MIN(qc_result.recorded_at) AS qc_early
    FROM samples_of_interest
    LEFT JOIN [warehouse].qc_result ON (
      qc_result.id_sample_tmp = samples_of_interest.id_sample_tmp
      AND qc_result.assay = 'Working Dilution - Plate Reader v1.0'
    )
    -- allow pipelines where no QC result is measured OR where it is measured recently
    WHERE qc_result.id_qc_result_tmp IS NULL
      OR qc_result.recorded_at >=  @_cutoff
    GROUP BY samples_of_interest.id_sample_tmp
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
    samples_of_interest.uuid_study_lims,
    samples_of_interest.uuid_sample_lims,
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
    em.manifest_uploaded,
    em.labware_received,
    em.order_made,
    MIN(dilution_timestamps.qc_early) working_dilution,
    em.library_start,
    em.library_complete,
    em.sequencing_run_start,
    em.sequencing_qc_complete

FROM sample_flowcell
LEFT JOIN labware_manifest_created_event ON (labware_manifest_created_event.labware_human_barcode = sample_flowcell.labware_human_barcode)
LEFT JOIN relevant_samples ru ON (ru.uuid=sample_flowcell.uuid_sample_lims)
LEFT JOIN _specific_event_min em ON (ru.subject_id=em.subject_id)
LEFT JOIN dilution_timestamps ON (dilution_timestamps.id_sample_tmp=sample_flowcell.id_sample_tmp)

-- We can speed up query by restricting to a given programme.
-- With the filter it takes about 3 mins.
-- WHERE programme IN ('Pathogen Variation', 'Malaria')

GROUP BY id_sample_lims_composite
ORDER BY id_sample_lims_composite
;

DROP TEMPORARY TABLE _event_min;
DROP TEMPORARY TABLE _specific_event_min;
DROP TEMPORARY TABLE _sample_event_ids;
DROP TEMPORARY TABLE _sample_events;
DROP TEMPORARY TABLE _labware_events;
