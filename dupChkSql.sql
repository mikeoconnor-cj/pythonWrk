USE Warehouse prod_adaugeopi;                                       
USE DATABASE prod_adaugeopi;                                       
                                       
WITH dupCTE
    AS
    (
    SELECT 'patient_x_chronic_condition_month' AS tableName
        , sum(rowsDuped) AS rowsEffected
        , count(*) AS pkRowCount
    FROM
        (
        SELECT chronic_condition_source_cd,
               fk_patient_id,
               measure_id,
               month_cd
            , count(*) AS rowsDuped 
        FROM INSIGHTS.patient_x_chronic_condition_month
        --WHERE CHRONIC_CONDITION_SOURCE_CD <> 'assgn_hcc' --all related to these
        GROUP BY chronic_condition_source_cd,
               fk_patient_id,
               measure_id,
               month_cd
        HAVING count(*) > 1
        ) a 
    ) 
    SELECT tableName 
        , 'prod_adaugeopi' AS orgDBName
        , rowsEffected
        , pkRowCount
    FROM dupCTE           
    

--error log
--A2575_m-2022-01_2022-02-25T20:52:03.220458_40980  load_stage_table -> cclf_7_v26        | failed 
--A2575_m-2022-01_2022-02-25T20:52:05.533755_40981  load_stage_table -> cclf_7_v26        | failed

SELECT date(load_ts) AS load_date
  , SSF_FILE_ID
  , count(*) AS rwCnt
FROM prod_a2575.STG.SSF_CCLF_7_V26 
WHERE date(load_ts) > '2022-02-01'
GROUP BY date(load_ts), SSF_FILE_ID
ORDER BY date(load_ts) DESC

LOAD_DATE SSF_FILE_ID RWCNT
2022-02-24  40,980    120,082
2022-02-24  40,981    816,649



--error log
--A2841_m-2022-01_2022-02-28T18:17:37.473197_41089_0 load_ods_table -> cclf_7_v26          | failed
--A2841_m-2022-01_2022-02-28T18:42:43.825841_41090  load_stage_table -> cclf_7_v26        | success 
--A2841_m-2022-01_2022-02-28T18:42:43.825841_41090_0 load_ods_table -> cclf_7_v26          | failed 

SELECT date(load_ts) AS load_date
  , SSF_FILE_ID
  , count(*) AS rwCnt
FROM prod_a2841.STG.SSF_CCLF_7_V26 
WHERE date(load_ts) > '2022-02-01'
GROUP BY date(load_ts), SSF_FILE_ID
ORDER BY date(load_ts) DESC

LOAD_DATE SSF_FILE_ID RWCNT
2022-02-28  41,089    725,830
2022-02-28  41,090    34,837
2022-02-24  40,917    34,837
2022-02-24  40,918    725,830



--error log
--A3327_m-2022-01_2022-02-25T18:12:00.621451_40859  load_stage_table -> cclf_5_v27 

SELECT date(load_ts) AS load_date
  , SSF_FILE_ID
  , count(*) AS rwCnt
FROM prod_a3327.STG.SSF_CCLF_5_V27 
WHERE date(load_ts) > '2022-02-01'
GROUP BY date(load_ts), SSF_FILE_ID
ORDER BY date(load_ts) DESC

LOAD_DATE SSF_FILE_ID RWCNT
2022-02-25  40,860    5,135,179
2022-02-17  40,859    330,184

--the usual member month report... only shows 2022 data for Adaugeo and ilumed
USE DATABASE prod_adaugeopi;    
USE DATABASE prod_ilumedpi;

Select org_id   
    , period_id 
    , measure_value_decimal AS totlForGroup 
from insights.metric_value_operational_dashboard  
where measure_cd = 'total_member_years_current_month' 
    and patient_medicare_group_cd = '#NA' 
    and org_level_category_cd = 'aco'
    and attribution_type = 'as_was'
    and substr(period_id,3,7) >= '2019-01' 
order by ORG_GROUP_ID 
    ,  period_id     
    
    
USE DATABASE prod_cityblockdce;    --no data either attrib type
USE DATABASE prod_canodce;

Select org_id   
    , period_id 
    , measure_value_decimal AS totlForGroup 
from insights.metric_value_operational_dashboard  
where measure_cd = 'total_member_years_current_month' 
    and patient_medicare_group_cd = '#NA' 
    and org_level_category_cd = 'aco'
    --and attribution_type = 'as_was'
    and substr(period_id,3,7) >= '2019-01' 
order by ORG_GROUP_ID 
    ,  period_id       
 

SELECT * 
FROM PROD_ADAUGEOPI.ods.DCE_ALIGN ----not there
WHERE load_set_id IN (45597,45598,40390,40417)

SELECT * 
FROM PROD_ILUMEDPI.ods.DCE_ALIGN --not there
WHERE load_set_id IN (40410, 45615)


USE DATABASE prod_adaugeopi; --multiple measues
USE DATABASE prod_ilumedpi; --multiple measues
USE DATABASE prod_cityblockdce;    --only *some* annual wellness visit measures
USE DATABASE prod_canodce;  --only *some* annual wellness visit measures
USE DATABASE prod_latitudedc; --only *some* annual wellness visit measures
USE DATABASE prod_bluerockdc; --only *some* annual wellness visit measures
USE DATABASE prod_intermntutdc; --only *some* annual wellness visit measures
  
SELECT org_id
  , measure_cd
from insights.metric_value_operational_dashboard  
GROUP BY org_id
  , measure_cd
ORDER BY org_id
  , measure_cd



  --13 members impacted      
SELECT DISTINCT FK_PATIENT_ID 
FROM
(
    --duplicates:
        SELECT ATTRIBUTION_TYPE
        ,FK_PATIENT_ID
        ,MONTH_CD
        , count(*) AS rowsDuped 
        FROM INSIGHTS.patient_x_month 
        --WHERE ATTRIBUTION_TYPE <> 'as_was' 
            --just 'as_was' has dups
            --with 'as_was', some members have 2 diff: at_time_primary_prov_nh
            --the duplicates quadruplicate because there's a self-join: at_time_attr_pr 
        GROUP BY ATTRIBUTION_TYPE
        ,FK_PATIENT_ID
        ,MONTH_CD
        HAVING count(*) > 1    
) a  


USE WAREHOUSE local_michaeloconnor;
USE DATABASE int_CANODCE_fe; --up to Feb '21.. then Jan '22
--int_INTERMNTUTDC_fe; --up to Feb '21.. then Jan '22
--int_BLUEROCKDC_fe; --up to Feb '21.. then Jan '22
--int_LATITUDEDC_fe; --up to Feb '21.. then Jan '22
--int_cityblockdce_fe; --up to Feb '21.. then Jan '22
--int_adaugeopi_fe; --ok 
--int_ilumedpi_fe --ok

Select org_id   
    , period_id 
    , measure_value_decimal AS totlForGroup 
from insights.metric_value_operational_dashboard  
where measure_cd = 'total_avg_hcc_risk_current_month'
    and patient_medicare_group_cd = '#NA' 
    and org_level_category_cd = 'aco'
    and attribution_type = 'as_was'
    and substr(period_id,3,7) >= '2019-01' 
order by ORG_GROUP_ID 
    ,  period_id  



SELECT *
FROM prod_ADAUGEO.ODS.NH_NETWORK_MODEL_5_PRVDR --looks ok
ORDER BY LOAD_PERIOD DESC
SELECT *
FROM PROD_ADAUGEO.STG.SSF_NH_NETWORK_5_PRVDR_V01 
ORDER BY DAG_RUN_ID desc
SELECT *
FROM prod_ADAUGEO.ODS.NH_NETWORK_MODEL_4_TIN --no data
ORDER BY LOAD_PERIOD DESC
SELECT *
FROM PROD_ADAUGEO.stg.SSF_NH_NETWORK_4_TIN_V01 --no data
ORDER BY DAG_RUN_ID desc
SELECT *
FROM prod_ADAUGEO.ODS.NH_NETWORK_MODEL_3_FAC --no data
ORDER BY LOAD_PERIOD DESC
SELECT *
FROM PROD_ADAUGEO.stg.SSF_NH_NETWORK_3_FAC_V01 --no data
ORDER BY DAG_RUN_ID desc
SELECT *
FROM prod_ADAUGEO.ODS.NH_NETWORK_MODEL_2_GRP --no data
ORDER BY LOAD_PERIOD DESC
SELECT *
FROM PROD_ADAUGEO.stg.SSF_NH_NETWORK_2_GRP_V01 --no data
ORDER BY DAG_RUN_ID desc
SELECT *
FROM prod_ADAUGEO.ODS.NH_NETWORK_MODEL_1_NET --looks ok
ORDER BY LOAD_PERIOD DESC
SELECT *
FROM PROD_ADAUGEO.stg.SSF_NH_NETWORK_1_NET_V01 --looks ok
ORDER BY DAG_RUN_ID desc
SELECT *
FROM prod_ADAUGEO.ODS.NH_NETWORK_MODEL_0_HDR --looks ok
ORDER BY LOAD_PERIOD DESC
SELECT *
FROM PROD_ADAUGEO.stg.SSF_NH_NETWORK_0_HDR_V01 --looks ok
ORDER BY DAG_RUN_ID desc
SELECT *
from PROD_ADAUGEOPI.insights.profile_list_physician_layup --load_ts: 3/17/22
--example primary_provider_network_id: cj_net|ADAUGEOPI|ADAUGEODC


--network advantage -collaborative health systems

select count(1) over() total_rows, * from (
SELECT 
  alignment_period
  ,year
  ,provider_type
  ,provider_npi
  ,provider_name
  ,primary_specialty_flag
  ,organization_npi
  ,organization_name
  ,org_fqhc_flag
  ,org_rhc_flag
  ,SUBSTRING(org_county_cd,1,5) AS org_county_cd
  ,org_county_name
  ,bene_state
  ,SUBSTRING(bene_county_cd,1,5) AS bene_county_cd
  ,bene_county_name
  ,SUBSTRING(bene_cbsa_cd,1,5) as bene_cbsa_cd
  ,bene_cbsa_name
  ,bene_cohort
  ,bene_cnt_aligned
  ,bene_avg_adj_hcc
  ,bene_cnt_strong_alignment
  ,bene_cnt_loose_alignment
  ,bene_cnt_align_alive
  ,bene_cnt_high_needs_eligible
  ,bene_cnt_hlthy_simplecc_null
  ,bene_cnt_frail_elderly
  ,bene_cnt_maj_min_compl_cc
  ,bene_cnt_under65_dis_esrd
  ,bene_cnt_aligned_eligible
  ,bene_total_member_months
  ,pqem_allowed
  ,pqem_spend
  ,pqem_spend_by_align_prov
  ,pqem_partb_spend
  ,pqem_op_fqhc_spend
  ,pqem_op_rhc_spend
  ,pqem_op_cah_spend
  ,partb_spend
  ,inpatient_spend
  ,outpatient_spend
  ,hha_spend
  ,snf_spend
  ,hospice_spend
  ,dme_spend
  ,total_spend
  ,group_level_1_id
  ,group_level_1_name
  ,group_level_2_id
  ,group_level_2_name
  ,group_level_3_id
  ,group_level_3_name
  ,CASE WHEN
    network_1_id = '#NA' THEN 'Out of Network'
      ELSE 'In Network'
      END AS network_flag
  ,network_1_id
  ,network_1_name
FROM vrdc.dc_provider_list_network
WHERE try_cast(year as int) >= 2017 AND network_1_id NOT LIKE '%All'
   
ORDER BY
  alignment_period
  ,provider_type
  ,year 
  ,provider_npi) a   where 1=1  AND "PROVIDER_NPI" IN ('1982750881')   


--problem org

SELECT load_period
  , src_measure_category_label
  , src_measure_cd
  , SRC_THREE_YR_MEAN 
FROM prod_a1052.ods.CCLF_BENCHMARK_1_DETAIL 
WHERE record_status_cd = 'a'
  AND load_period IN ('y-2020', 'y-2021')
  AND src_measure_category_label LIKE '%Expenditures%'  
ORDER BY load_period, src_measure_category_label, src_measure_cd

SELECT *
FROM PROD_A1052.insights.metric_value_bnmrk_x_qexpu
WHERE QUARTER_CD in ('q-2020-4','q-2021-1','q-2021-2','q-2021-3','q-2021-4') 
ORDER BY QUARTER_CD, MEDICARE_COHORT 


--staging table
SELECT *  --in 2020, rec_num is populatd in staging table
FROM prod_a1052.STG.ssf_CCLF_BENCHMARK_1_DETAIL_v05 
WHERE THREE_YR_MEAN IS NOT null
ORDER BY dag_run_id, MEASURE_LABEL 


--correct org... ? by chance in 2021 ?

SELECT load_period
  , src_measure_category_label
  , src_measure_cd
  , SRC_THREE_YR_MEAN 
FROM prod_a3632.ods.CCLF_BENCHMARK_1_DETAIL 
WHERE record_status_cd = 'a'
  AND load_period IN ('y-2020', 'y-2021')
  AND src_measure_category_label LIKE '%Expenditures%'
ORDER BY load_period, src_measure_category_label, src_measure_cd


SELECT *
FROM PROD_A3632.insights.metric_value_bnmrk_x_qexpu
WHERE QUARTER_CD in ('q-2020-4','q-2021-1','q-2021-2','q-2021-3','q-2021-4') 
ORDER BY QUARTER_CD, MEDICARE_COHORT 

--staging table

SELECT *  --in 2020, rec_num is populated in staging table
FROM prod_a3632.STG.ssf_CCLF_BENCHMARK_1_DETAIL_v05 
WHERE THREE_YR_MEAN IS NOT null
ORDER BY dag_run_id, MEASURE_LABEL 



--downstream impact: aco_x_benchmark


----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
--
-- Title:       insights.patient_x_risk_score_month ETL for DCE
-- Author:      Rachel Plummer
-- Date:        July, 11 2021
-- Description: Program outputs insights.pxrsm, a table that contains a risk score on a  
--              per beneficiary per month basis, following the quarterly risk score report uploads.
--              Logic back propogates the earliest risk score available to the historical/alignment years
--              and forward propogates the latest risk score into the next applicable quarter of the PY
--              (to account for months we don't get a an applicable quarterly risk score file, but we DO 
--              get claims data for that month)
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------

INSERT OVERWRITE INTO insights.patient_x_risk_score_month
(
      org_id
    , fk_patient_id
    , risk_score_cd
    , risk_score_source_cd
    , month_cd
    , risk_score
    , load_period
    , load_run_id
    , load_ts
)
with b as (
    with a as (
        with risk_score_cte as (
            SELECT
                org_id
                , SPLIT_PART(PK_DCE_RISK_SCORE_ID, '|', 1)||'|'||SRC_BENE_MBI_ID as fk_patient_id
                , SRC_CLNDR_MNTH as month
                , SRC_CLNDR_YR as year
                , 'hcc' as risk_score_cd
                , 'dce_risk_score' as risk_score_source_cd
                , COALESCE(SRC_RISK_SCORE, SRC_RAW_RISK_SCORE) as risk_score
                , ROW_NUMBER() OVER (PARTITION BY SRC_BENE_MBI_ID ORDER BY SRC_CLNDR_YR, SRC_CLNDR_MNTH DESC) AS month_rank_desc
                , ROW_NUMBER() OVER (PARTITION BY SRC_BENE_MBI_ID ORDER BY SRC_CLNDR_YR, SRC_CLNDR_MNTH) AS month_rank_asc
                , CASE WHEN SRC_CLNDR_MNTH in ('1', '2', '3') 
                            THEN 1
                    WHEN SRC_CLNDR_MNTH in ('4', '5', '6') 
                            THEN 2
                    WHEN SRC_CLNDR_MNTH in ('7', '8', '9') 
                            THEN 3
                    ELSE 4 
                END AS qtr_ind
            FROM ods.dce_risk_score
            WHERE RECORD_STATUS_CD = 'a'
                AND effective_flag
        )
        -- back propogate risk scores from jan2018 to dec2021
        SELECT 
            rs.org_id
            , rs.fk_patient_id
            , rs.risk_score_cd
            , rs.risk_score_source_cd
            , m1.value as month_cd
            , rs.risk_score
        FROM risk_score_cte rs
        LEFT JOIN (
                SELECT 
                    FK_PATIENT_ID
                    , MIN(month_cd) as min_month 
                FROM insights.patient_roster
                group by FK_PATIENT_ID
            ) mm
            ON 1 = 1
            AND mm.FK_PATIENT_ID = rs.fk_patient_id
        LEFT JOIN {{env}}_common.ref.code_month m1
            -- historical alignment year data begins at CY2018+
            ON 'm-2018-01' <= m1.value
            AND min_month > m1.value
        where month_rank_asc = 1

        UNION ALL

        -- propogate risk scores in the current quarter of the PY
        SELECT 
            rs.org_id
            , rs.fk_patient_id
            , rs.risk_score_cd
            , rs.risk_score_source_cd
            , coalesce(m1.value,'m-'||rs.year||'-'||lpad(month, 2, '0')) as month_cd
            , rs.risk_score
        FROM risk_score_cte rs
        LEFT JOIN {{env}}_common.ref.code_month m1
            ON  m1.YEAR    = 'y-'||rs.year
            AND m1.QUARTER = 'q-'||rs.year||'-'||(rs.qtr_ind)

        UNION ALL

        -- forward propogate risk scores into the next quarter of the PY (because the risk score file lags) 
        SELECT 
            rs.org_id
            , rs.fk_patient_id
            , rs.risk_score_cd
            , rs.risk_score_source_cd
            , m1.value as month_cd
            , rs.risk_score
        FROM risk_score_cte rs
        LEFT JOIN {{env}}_common.ref.code_month m1
            ON  m1.YEAR    = 'y-'||rs.year
            AND m1.QUARTER = 'q-'||rs.year||'-'||(rs.qtr_ind+1)
        WHERE month_rank_desc = 1
    )

    -- add rank logic for benes with more than 1 risk score in a given month
    select  
          org_id
        , fk_patient_id
        , risk_score_cd
        , risk_score_source_cd
        , month_cd
        , risk_score
        , ROW_NUMBER() OVER (PARTITION BY fk_patient_id, month_cd ORDER BY risk_score desc) AS row_n 
    from a
)

select 
      org_id
    , fk_patient_id
    , risk_score_cd
    , risk_score_source_cd
    , month_cd
    , risk_score
    , '{{dag_run.conf.load_period}}' AS load_period
    , {{ti.job_id}} AS load_run_id
    , CURRENT_TIMESTAMP AS load_ts
from b 
-- force pick 1 risk score in a given month to resolve dupes (hot fix)
where row_n =1
;