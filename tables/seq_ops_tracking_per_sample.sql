CREATE TABLE seq_ops_tracking_per_sample (
  id_sample_lims_composite VARCHAR(266) NOT NULL PRIMARY KEY
, id_lims VARCHAR(10) NOT NULL
, id_sample_lims VARCHAR(255) NOT NULL
, sanger_sample_id VARCHAR(255)
, supplier_name VARCHAR(255)
, submitted_study_name VARCHAR(255)
, sequenced_study_name TEXT

, manifest_plate_barcode TEXT
, study_id TEXT
, programme TEXT
, faculty_sponsor TEXT
, data_access_group TEXT
, library_type_ordered TEXT
, library_type TEXT
, bait_names TEXT
, project_name TEXT
, sequencing_cost_code TEXT
, platform TEXT

, manifest_created DATETIME
, manifest_uploaded DATETIME
, labware_received DATETIME
, order_made DATETIME
, working_dilution DATETIME
, library_start DATETIME
, library_complete DATETIME
, sequencing_run_start DATETIME
, sequencing_qc_complete DATETIME

, INDEX ix_seq_ops_tracking_per_sample_id_sample_lims (id_sample_lims)
, INDEX ix_seq_ops_tracking_per_sample_sanger_sample_id (sanger_sample_id)
, INDEX ix_seq_ops_tracking_per_sample_supplier_name (supplier_name)
, INDEX ix_seq_ops_tracking_per_sample_submitted_study_name (submitted_study_name)
) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
;
