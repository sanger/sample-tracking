CREATE OR REPLACE VIEW [reporting].billing_report_view_v3 AS

-- Illumina Sequencing Billing Data
-- Combines Illumina (iseq_*) and Aviti (eseq_*) sequencing runs
-- Includes 4 additional columns for Aviti-specific metadata

WITH
-- CTE 1: qc_complete
-- Find the first QC-complete event for each Illumina run
qc_complete AS (
    SELECT
        rs.id_run,
        MIN(rs.date) AS qc_complete_date
    FROM [warehouse].iseq_run_status rs
    JOIN [warehouse].iseq_run_status_dict d
          ON d.id_run_status_dict = rs.id_run_status_dict
    WHERE d.description = 'qc complete'
      AND DATE(rs.date) >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
    GROUP BY rs.id_run
),
-- CTE 2: sample_lanes
-- Collect all samples on a lane (excluding controls)
sample_lanes AS (
    SELECT
        r.id_run
        , fc.entity_id_lims AS lane_id
        , fc.cost_code      AS project_cost_code
        , st.name           AS study_name
        , st.id_study_lims  AS study_id
        , lm.position       AS lane_position
        , sr.labware_human_barcode AS stock_plate_barcode -- stock plate if it exists
        , s.id_sample_tmp
        , fc.id_flowcell_lims AS batch_id
        , lm.instrument_model AS platform
        , IF(lm.instrument_model = 'MiSeq', SUBSTRING_INDEX(SUBSTRING_INDEX(lm.flowcell_barcode, '-', 2), '-', -1), null)
                                           AS reagent_kit_barcode
        , IF(lm.instrument_model = 'NovaSeq', EXTRACTVALUE(ri.run_parameters_xml, '//SbsCycleKit'), null)
                                           AS sbs_cycle_kit
        , IF(INSTR(EXTRACTVALUE(ri.run_parameters_xml, '//RecipeVersion'), '_CustomPrimer_') > 0, 'Yes', 'No') 
                                            AS custom_primer_used
        , SUBSTRING_INDEX(EXTRACTVALUE(ri.run_parameters_xml, '//ConsumableInfo[Type="Reagent"]/Name'), ' ', 1) 
                                            AS kit_type
        , SUBSTRING_INDEX(EXTRACTVALUE(ri.run_parameters_xml, '//ConsumableInfo[Type="Reagent"]/Name'), ' ', -1) 
                                            AS cycle_number
        , qc.qc_complete_date
        , IF(lm.qc_seq = 1, 'passed', IF(lm.qc_seq = '0', 'failed', lm.qc_seq ))
                                           AS qc_outcome
        , IF(r.rp__sbs_consumable_version = '1', 'v1', IF(r.rp__sbs_consumable_version = '3', 'v1.5', r.rp__sbs_consumable_version))
                                           AS 'v1/1.5'
        , IF(r.rp__workflow_type = 'NovaSeqXp', 'XP', IF(r.rp__workflow_type = 'NovaSeqStandard', 'No XP', r.rp__workflow_type) )
                                           AS xp
        , r.rp__flow_cell_mode              AS sp
        , r.rp__read1_number_of_cycles      AS read1
        , r.rp__read2_number_of_cycles      AS read2
        , fc.pipeline_id_lims               AS library
        , pm.q20_yield_kb_forward_read + pm.q20_yield_kb_reverse_read AS q20_yield
    FROM [warehouse].iseq_run r
    LEFT JOIN [warehouse].iseq_run_info ri ON ri.id_run = r.id_run  -- not all runs have run_info
    JOIN qc_complete qc ON qc.id_run = r.id_run
    JOIN [warehouse].iseq_product_metrics pm ON pm.id_run = r.id_run
    JOIN [warehouse].iseq_run_lane_metrics lm
        ON lm.id_run = pm.id_run
       AND lm.position = pm.position
    JOIN [warehouse].iseq_flowcell fc
        ON pm.id_iseq_flowcell_tmp = fc.id_iseq_flowcell_tmp
    LEFT JOIN [warehouse].sample s
        ON s.id_sample_tmp = fc.id_sample_tmp
    LEFT JOIN [warehouse].stock_resource sr
        ON sr.id_sample_tmp = s.id_sample_tmp
    LEFT JOIN [warehouse].study st
        ON fc.id_study_tmp = st.id_study_tmp
    -- don't include controls in the overall sample_lane set
    WHERE st.name NOT IN ('Heron PhiX', 'Illumina Controls', 'Comp PhiX')
),
-- CTE 3: lane_proportions
-- For each lane: count samples & compute 1/N
lane_proportions AS (
    SELECT
        lane_id
        , FORMAT(1 / COUNT(*), 10) AS proportion_of_lane_per_sample
    FROM sample_lanes
    GROUP BY lane_id
),
-- CTE 4: eseq_qc_complete
-- Find runs with OutcomeCompleted status for Aviti
eseq_qc_complete AS (
    SELECT
        r.id_eseq_run_tmp,
        r.date_completed AS qc_complete_date
    FROM [warehouse].eseq_run r
    WHERE r.outcome = 'OutcomeCompleted'
      AND DATE(r.date_completed) >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
),
-- CTE 5: eseq_sample_lanes
-- Collect all Aviti samples on a lane
eseq_sample_lanes AS (
    SELECT
        r.id_eseq_run_tmp
        , fc.id_flowcell_lims AS lane_id
        , fc.cost_code AS project_cost_code
        , st.name AS study_name
        , st.id_study_lims AS study_id
        , lm.lane AS lane_position
        , lm.instrument_model AS platform
        , sr.labware_human_barcode AS stock_plate_barcode
        , s.id_sample_tmp
        , fc.id_flowcell_lims AS batch_id
        , IF(fc.custom_primer_kit_used = 1, 'Yes', IF(fc.custom_primer_kit_used = 0, 'No', NULL)) AS custom_primer_used
        , IF(lm.qc_seq = 1, 'passed', IF(lm.qc_seq = 0, 'failed', lm.qc_seq)) AS qc_outcome
        , JSON_EXTRACT(r.run_parameters, '$.Cycles.R1') AS read1
        , JSON_EXTRACT(r.run_parameters, '$.Cycles.R2') AS read2
        , JSON_UNQUOTE(JSON_EXTRACT(r.run_parameters, '$.ThroughputSelection')) AS throughput_selection
        , JSON_UNQUOTE(JSON_EXTRACT(r.run_parameters, '$.KitConfiguration')) AS kit_configuration
        , fc.custom_primer_kit_used
        , fc.quant_method_used
        , fc.pipeline_id_lims AS library
        , qc.qc_complete_date
    FROM [warehouse].eseq_run r
    JOIN eseq_qc_complete qc ON qc.id_eseq_run_tmp = r.id_eseq_run_tmp
    JOIN [warehouse].eseq_run_lane_metrics lm ON r.folder_name = lm.run_folder_name
    JOIN [warehouse].eseq_product_metrics pm ON lm.id_run = pm.id_run
    JOIN [warehouse].eseq_flowcell fc ON pm.id_eseq_flowcell_tmp = fc.id_eseq_flowcell_tmp
    LEFT JOIN [warehouse].study st ON fc.id_study_tmp = st.id_study_tmp
    LEFT JOIN [warehouse].sample s ON fc.id_sample_tmp = s.id_sample_tmp
    LEFT JOIN [warehouse].stock_resource sr ON sr.id_sample_tmp = s.id_sample_tmp
),
-- CTE 6: eseq_lane_proportions
-- For each Aviti lane: count samples & compute 1/N
eseq_lane_proportions AS (
    SELECT
        lane_id
        , FORMAT(1 / COUNT(*), 10) AS proportion_of_lane_per_sample
    FROM eseq_sample_lanes
    GROUP BY lane_id
)
-- Main Illumina query with 4 NULL placeholder columns for Aviti-specific data
SELECT
    sample_lanes.platform
    , sample_lanes.project_cost_code
    , sample_lanes.study_id
    , sample_lanes.lane_position
    , sample_lanes.batch_id
    , GROUP_CONCAT(DISTINCT sample_lanes.study_name SEPARATOR ';') AS study_name
    , sample_lanes.stock_plate_barcode
    , GROUP_CONCAT(DISTINCT sample_lanes.reagent_kit_barcode SEPARATOR ';') AS reagent_kit_barcode
    , GROUP_CONCAT(DISTINCT sample_lanes.sbs_cycle_kit SEPARATOR ';')  AS sbs_cycle_kit
    , GROUP_CONCAT(DISTINCT sample_lanes.custom_primer_used SEPARATOR ';') AS custom_primer_used
    , GROUP_CONCAT(DISTINCT sample_lanes.kit_type SEPARATOR ';') AS kit_type
    , GROUP_CONCAT(DISTINCT sample_lanes.cycle_number SEPARATOR ';') AS cycle_number
    , MIN(sample_lanes.qc_complete_date) AS qc_complete_date
    , sample_lanes.qc_outcome
    , GROUP_CONCAT(DISTINCT sample_lanes.`v1/1.5` SEPARATOR ';') AS `v1/1.5`
    , sample_lanes.xp
    , sample_lanes.sp
    , GROUP_CONCAT(DISTINCT sample_lanes.read1 SEPARATOR ';') AS read1
    , GROUP_CONCAT(DISTINCT sample_lanes.read2 SEPARATOR ';') AS read2
    , SUM(lane_proportions.proportion_of_lane_per_sample) AS total
    , sample_lanes.library
    , COUNT(DISTINCT sample_lanes.id_sample_tmp) AS num_samples
    , AVG(sample_lanes.q20_yield) AS q20yield
    , NULL AS throughput_selection
    , NULL AS kit_configuration
    , NULL AS element_customer_primer_kit
    , NULL AS element_quant_method_used
FROM sample_lanes
JOIN lane_proportions ON lane_proportions.lane_id = sample_lanes.lane_id
GROUP BY
    sample_lanes.study_id
    , sample_lanes.project_cost_code
    , sample_lanes.platform
    , sample_lanes.qc_outcome
    , sample_lanes.xp
    , sample_lanes.sp
    , sample_lanes.batch_id
    , sample_lanes.library
    , sample_lanes.lane_position
    , sample_lanes.stock_plate_barcode

UNION ALL

-- Aviti Sequencing Billing Data
SELECT
    eseq_sample_lanes.platform
    , eseq_sample_lanes.project_cost_code
    , eseq_sample_lanes.study_id  -- id_study_lims for consistency with Illumina
    , eseq_sample_lanes.lane_position
    , eseq_sample_lanes.batch_id
    , GROUP_CONCAT(DISTINCT eseq_sample_lanes.study_name SEPARATOR ';') AS study_name
    , eseq_sample_lanes.stock_plate_barcode
    , NULL AS reagent_kit_barcode
    , NULL AS sbs_cycle_kit
    , GROUP_CONCAT(DISTINCT eseq_sample_lanes.custom_primer_used SEPARATOR ';') AS custom_primer_used
    , NULL AS kit_type
    , NULL AS cycle_number
    , MIN(eseq_sample_lanes.qc_complete_date) AS qc_complete_date
    , eseq_sample_lanes.qc_outcome
    , NULL AS `v1/1.5`
    , NULL AS xp
    , NULL AS sp
    , GROUP_CONCAT(DISTINCT eseq_sample_lanes.read1 SEPARATOR ';') AS read1
    , GROUP_CONCAT(DISTINCT eseq_sample_lanes.read2 SEPARATOR ';') AS read2
    , SUM(eseq_lane_proportions.proportion_of_lane_per_sample) AS total
    , eseq_sample_lanes.library
    , COUNT(DISTINCT eseq_sample_lanes.id_sample_tmp) AS num_samples
    , NULL AS q20yield
    , ANY_VALUE(eseq_sample_lanes.throughput_selection) AS throughput_selection
    , ANY_VALUE(eseq_sample_lanes.kit_configuration) AS kit_configuration
    , ANY_VALUE(eseq_sample_lanes.custom_primer_kit_used) AS element_customer_primer_kit
    , ANY_VALUE(eseq_sample_lanes.quant_method_used) AS element_quant_method_used
FROM eseq_sample_lanes
JOIN eseq_lane_proportions ON eseq_lane_proportions.lane_id = eseq_sample_lanes.lane_id
GROUP BY
    st.id_study_tmp  -- unique key (even though SELECT shows id_study_lims for user consistency)
    , eseq_sample_lanes.project_cost_code
    , eseq_sample_lanes.platform
    , eseq_sample_lanes.qc_outcome
    , eseq_sample_lanes.batch_id
    , eseq_sample_lanes.library
    , eseq_sample_lanes.lane_position
    , eseq_sample_lanes.stock_plate_barcode
ORDER BY 
    study_name
    , project_cost_code
    , lane_position
    , platform
    , qc_outcome
    , batch_id
    , library
    , xp
    , sp
    , stock_plate_barcode