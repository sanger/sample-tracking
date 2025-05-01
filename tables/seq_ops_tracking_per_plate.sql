CREATE TABLE seq_ops_tracking_per_plate (
  study_name VARCHAR(255)
, study_id VARCHAR(20) NOT NULL
, programme VARCHAR(255)
, faculty_sponsor VARCHAR(255)
, data_access_group VARCHAR(255)
, manifest_created DATETIME
, manifest_uploaded DATETIME
, manifest_plate_barcode VARCHAR(255) NOT NULL PRIMARY KEY

, library_type_ordered TEXT
, library_type TEXT
, bait_names TEXT
, project_name TEXT
, sequencing_cost_code TEXT
, platform TEXT
, labware_received DATETIME

, order_made_samples INT NOT NULL
, order_made_first DATETIME
, order_made_last DATETIME
, working_dilution_samples INT NOT NULL
, working_dilution_first DATETIME
, working_dilution_last DATETIME
, library_start_samples INT NOT NULL
, library_start_first DATETIME
, library_start_last DATETIME
, library_complete_samples INT NOT NULL
, library_complete_first DATETIME
, library_complete_last DATETIME
, sequencing_run_start_samples INT NOT NULL
, sequencing_run_start_first DATETIME
, sequencing_run_start_last DATETIME
, sequencing_qc_fail_samples INT NOT NULL
, sequencing_qc_pass_samples INT NOT NULL
, sequencing_qc_complete_first DATETIME
, sequencing_qc_complete_last DATETIME
, irods_root_collections TEXT

, INDEX ix_seq_ops_tracking_per_plate_study_name (study_name)
, INDEX ix_seq_ops_tracking_per_plate_study_id (study_id)
);
