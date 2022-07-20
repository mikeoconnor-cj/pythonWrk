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



--Patient List

select count(1) over() total_rows, * from (WITH denorm AS (
select distinct org_group_id, tin_name
from insights.metric_value_denormalized_total
where org_level_category_cd = 'at_time_tin'
),
chronics as (
SELECT DISTINCT
month_cd,
fk_patient_id,
listagg(distinct measure_id, ',') conditions
FROM insights.patient_x_chronic_condition_month
WHERE measure_id IN ('emr_chronic_condition_stroke_transient_ischemic_attack',
'emr_chronic_condition_obesity',
'emr_chronic_condition_diabetes',
'emr_chronic_condition_ischemic_heart_disease',
'emr_chronic_condition_heart_failure',
'emr_chronic_condition_chronic_obstructive_pulmonary_disease_and_bronchiectasis',
'emr_chronic_condition_asthma',
'emr_chronic_condition_ischemic_heart_disease',
'emr_chronic_condition_prostate_cancer',
'emr_chronic_condition_lung_cancer',
'emr_chronic_condition_female_male_breast_cancer',
'emr_chronic_condition_endometrial_cancer',
'emr_chronic_condition_colorectal_cancer')
AND active_flag = TRUE
AND chronic_condition_source_cd = 'ccw'
group by
month_cd,
fk_patient_id
),
er_visits as (
select
fk_patient_id,
count(distinct pk_claim_id) last_3_mo_er_visits
from insights.metric_value_er
where from_dt <= to_date(right( 'm-2021-12',7),'YYYY-MM') 
  and from_dt >= to_date(right( 'm-2021-12',7),'YYYY-MM') - INTERVAL '3 months'
group by
fk_patient_id
)
select
org_id
,last_name
,first_name
,pt.bene_mbi_id
,case when pat.gender_cd = '1' then 'Male' else 'Female' end as gender_cd
,case when split_part(at_time_tin_fac_nh,'|',3) = '' then split_part(at_time_tin_fac_nh,'|',2) else  split_part(at_time_tin_fac_nh,'|',3) end as at_time_tin_fac_nh
,denorm.tin_name as tin_name
,split_part(at_time_primary_prov_nh,'|',2) as at_time_primary_prov_nh
,prvdr_name
,current_attributed_status
,churned_prev_quarter
,reason_for_churn
,split_part(frailty_group,'|',2) as frailty_group
,split_part(medicare_cohort,'|',2) as medicare_cohort
,last_awv
,split_part(last_awv_provider,'|',2) as last_awv_provider
,cast(date_of_birth as date) as date_of_birth
,cast(date_of_death as date) as date_of_death
,alzheimers_dementia_flag
,anxiety_flag
,cancer_flag
,chronic_heart_failure_flag
,chronic_kidney_disease_flag
,copd_asthma_flag
,depression_flag
,diabetes_flag
,obesity_flag
,stroke_flag
,hosp_ip_paid_amt
,op_paid_amt
,snf_paid_amt
,hh_paid_amt
,hospice_paid_amt
,percent_ccm_compliance
,ccm_eligible_instances
,percent_tcm_compliance
,tcm_eligible_instances
,partbdme_paid_amt
,partb_paid_amt
,total_paid_amt
,ip_admits
,er_admits
,risk_score
,awv_eligible_instances
,attribution_type
,pt.month_cd
,patient_id
,addr_zip as patient_zip
,coalesce(chronics.conditions,'None') as conditions
,coalesce(regexp_count(conditions,',')+1,0) as count_conditions
, coalesce(er_visits.last_3_mo_er_visits,0) as last_3_mo_er_visits
,case when last_3_mo_er_visits is not null then 1 else 0 end as er_flag
,case when count_conditions + er_flag > 4 then 'Level 1'
when count_conditions + er_flag > 2 then 'Level 2'
when count_conditions + er_flag > 0 then 'Level 3'
when count_conditions + er_flag = 0 then 'Level 4'
END as patient_segment
from insights.profile_list_patient_layup pt
left join (select bene_mbi_id, gender_cd from insights.patient) pat
on pat.bene_mbi_id = pt.bene_mbi_id
left join denorm
on pt.at_time_tin_fac_nh = denorm.org_group_id
LEFT JOIN chronics
ON pt.month_cd = chronics.month_cd
AND pt.patient_id = chronics.fk_patient_id
LEFT JOIN er_visits
ON pt.patient_id = er_visits.fk_patient_id
where attribution_type = 'as_was'
and pt.month_cd >  'm-2021-12'

) a   


USE WAREHOUSE local_michaeloconnor;
USE DATABASE local_michaeloconnor;

create or replace TABLE PUBLIC.PARTICIPANT_LIST_NA (
  ALIGNMENT_PERIOD VARCHAR(12),
  YEAR VARCHAR(4),
  PROVIDER_TYPE VARCHAR(25),
  PROVIDER_NPI VARCHAR(10),
  PROVIDER_NAME VARCHAR(150),
  PRIMARY_SPECIALTY_FLAG BOOLEAN,
  ORGANIZATION_NPI VARCHAR(12),   --source outputs a decimal 1902095078.0  VARCHAR(10)
  ORGANIZATION_NAME VARCHAR(150),
  ORG_FQHC_FLAG BOOLEAN,
  ORG_RHC_FLAG BOOLEAN,
  ORG_COUNTY_CD VARCHAR(10),
  ORG_COUNTY_NAME VARCHAR(50),
  BENE_STATE VARCHAR(50),
  BENE_COUNTY_CD VARCHAR(10),
  BENE_COUNTY_NAME VARCHAR(50),
  BENE_CBSA_CD VARCHAR(10),
  BENE_CBSA_NAME VARCHAR(50),
  BENE_COHORT VARCHAR(25),
  BENE_CNT_ALIGNED VARCHAR(38),
  BENE_AVG_ADJ_HCC VARCHAR(16),  --source: numeric value '' is not recognized ... row 2119  NUMBER(14,2)
  BENE_CNT_STRONG_ALIGNMENT VARCHAR(38),
  BENE_CNT_LOOSE_ALIGNMENT VARCHAR(38),
  BENE_CNT_ALIGN_ALIVE VARCHAR(38),
  BENE_CNT_HIGH_NEEDS_ELIGIBLE VARCHAR(38),
  BENE_CNT_HLTHY_SIMPLECC_NULL VARCHAR(38),
  BENE_CNT_FRAIL_ELDERLY VARCHAR(38),
  BENE_CNT_MAJ_MIN_COMPL_CC VARCHAR(38),
  BENE_CNT_UNDER65_DIS_ESRD VARCHAR(38),
  BENE_CNT_ALIGNED_ELIGIBLE VARCHAR(38),
  BENE_TOTAL_MEMBER_MONTHS VARCHAR(38),
  PQEM_ALLOWED VARCHAR(16),
  PQEM_SPEND VARCHAR(16),
  PQEM_SPEND_BY_ALIGN_PROV VARCHAR(16),
  PQEM_PARTB_SPEND VARCHAR(16),
  PQEM_OP_FQHC_SPEND VARCHAR(16),
  PQEM_OP_RHC_SPEND VARCHAR(16),
  PQEM_OP_CAH_SPEND VARCHAR(16),
  PARTB_SPEND VARCHAR(16),
  INPATIENT_SPEND VARCHAR(16),
  OUTPATIENT_SPEND VARCHAR(16),
  HHA_SPEND VARCHAR(16),
  SNF_SPEND VARCHAR(16),
  HOSPICE_SPEND VARCHAR(16),
  DME_SPEND VARCHAR(16),
  TOTAL_SPEND VARCHAR(16),
  GROUP_LEVEL_1_ID VARCHAR(255),
  GROUP_LEVEL_1_NAME VARCHAR(255),
  GROUP_LEVEL_2_ID VARCHAR(255),
  GROUP_LEVEL_2_NAME VARCHAR(255),
  GROUP_LEVEL_3_ID VARCHAR(255),
  GROUP_LEVEL_3_NAME VARCHAR(255),
  NETWORK_FLAG VARCHAR(25),
  NETWORK_1_ID VARCHAR(255),
  NETWORK_1_NAME VARCHAR(255)
)



STAGE_FILE_FORMAT = ( TYPE = 'csv'
            RECORD_DELIMITER = '\\n'
            FIELD_DELIMITER = ','
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            skip_header = 1
            );
           
put file://D:\Users\michael.oconnor\Downloads\pythonHmWrk\Participants_0704202206.csv @public.%PARTICIPANT_LIST_NA;
--LIST @%PARTICIPANT_LIST_NA;
--SELECT * FROM @%PARTICIPANT_LIST_NA

COPY INTO local_michaeloconnor.PUBLIC.PARTICIPANT_LIST_NA;

SELECT * FROM local_michaeloconnor.public.PARTICIPANT_LIST_NA;  




--QA-----

SELECT count(*) --1,641,440
FROM local_michaeloconnor.public.PARTICIPANT_LIST_NA;

SELECT * --12 rows
FROM local_michaeloconnor.public.PARTICIPANT_LIST_NA
WHERE PROVIDER_NPI = '1982750881' 


SELECT *
FROM prod_a1052.ods.CCLF_BENCHMARK_1_DETAIL 
WHERE 
record_status_cd = 'a'
  AND SRC_MEASURE_CATEGORY_LABEL in (
  '[G] 3-Year Weighted Average Annual Per Capita Expenditures ($)'
  , '[R] Regionally-Adjusted Historical Benchmark Expenditures ($)'
  )
-- ORDER BY load_period, src_measure_category_label, src_measure_cd


USE database prod_canodce;

SELECT cclf9.SRC_CRNT_NUM 
  , cclf9.FK_CRNT_BENE_ID 
  , cclf9.SRC_PRVS_NUM 
  , cclf9.FK_PRVS_BENE_ID 
  , cclf9.SRC_PRVS_EFCTV_DT 
  , cclf9.SRC_PRVS_OBSLT_DT 
  , cclf8.*
FROM ODS.CCLF_8_BENE_DEMO cclf8
JOIN  ods.CCLF_9_BENE_XREF cclf9
  ON cclf8.PK_BENE_ID = cclf9.FK_PRVS_BENE_ID 
WHERE cclf8.RECORD_STATUS_CD = 'a'  
  AND cclf9.RECORD_STATUS_CD = 'a'
  AND cclf9.SRC_CRNT_NUM <> cclf9.SRC_PRVS_NUM 

SELECT FK_CRNT_BENE_ID
  , count(*)
FROM ods.CCLF_9_BENE_XREF
WHERE RECORD_STATUS_CD = 'a'
GROUP BY FK_CRNT_BENE_ID
HAVING count(*) > 1
ORDER BY count(*)


WITH curPrv
AS 
(
SELECT cclf9.SRC_CRNT_NUM 
  , cclf9.FK_CRNT_BENE_ID 
  , cclf9.SRC_PRVS_NUM 
  , cclf9.FK_PRVS_BENE_ID 
  , cclf9.SRC_PRVS_EFCTV_DT 
  , cclf9.SRC_PRVS_OBSLT_DT 
  , cclf8.*
FROM ODS.CCLF_8_BENE_DEMO cclf8
JOIN  ods.CCLF_9_BENE_XREF cclf9
  ON cclf8.PK_BENE_ID = cclf9.FK_PRVS_BENE_ID 
WHERE cclf8.RECORD_STATUS_CD = 'a'  
  AND cclf9.RECORD_STATUS_CD = 'a'
  AND cclf9.SRC_CRNT_NUM <> cclf9.SRC_PRVS_NUM  
)

SELECT 'currentID' AS SOURCE
  , * 
FROM insights.PATIENT 
WHERE PK_PATIENT_ID IN  (SELECT FK_CRNT_BENE_ID FROM curPrv)  

UNION ALL 

SELECT 'previousID' AS SOURCE
  , * 
FROM insights.PATIENT 
WHERE PK_PATIENT_ID IN ( SELECT FK_PRVS_BENE_ID FROM curPrv)

ORDER BY full_name, pk_patient_id


SELECT '{orgDB}' as orgDB
    , load_period 
    , src_assgn_period 
    , src_assgn_period_type_cd 
    , src_bene_assgn_window_start_dt 
    , src_bene_assgn_window_end_dt 
    , src_mssp_aco_report_period_start_dt 
    , src_hcc_start_dt 
    , src_risk_score_start_dt 
    , src_aco_track 
    , src_performance_year 
    , src_claims_processed_as_of_dt 
FROM {orgDB}.ODS.CCLF_ASSGN_0_HEADER 
WHERE record_status_cd = 'a'
ORDER BY src_assgn_period

based on row counts, this file got loaded into load_period q-2021-2 into PROD_A1052.ods.CCLF_ASSGN_2_TIN
P.A1052.ACO.QALR.2021Q1.D219999.T0100000_1-1.csv
the row count in raw is only 2,636  so that carries through in Chris F''s query of ODS.. so might be a CMS data issue
based on row counts, this file got loaded into load_period q-2021-2 into PROD_A1052.ods.CCLF_ASSGN_2_TIN_NPI
P.A1052.ACO.QALR.2021Q1.D219999.T0100000_1-4.csv
the row count in raw is only 2,636  so that carries through in Chris F''s query of ODS.. so might be a CMS data issue

we tried to load this file with 3,781 records into staging P.A1052.ACO.QALR.2021Q2.D219999.T0200000_1-1.csv under 
a couple dag runs:  A1052_q-2021-2_2021-09-08T13:50:47.656353 and A1052_q-2021-2_2021-09-03T20:31:45.898322. 
These records didn''t seem to make it into ODS .. in any load period..I think we''ll need to set the DAG RUN 
so it winds up with a Q3 2021 load period in ODS.
This file P.A1052.ACO.QALR.2021Q2.D219999.T0200000_1-4.csv with 4,128 records got loaded into into 
load_period q-2021-3 into PROD_A1052.ods.CCLF_ASSGN_2_TIN_NPI


SELECT FK_BENE_ID 
  , src_year --varchar(4)
  , src_COPD --number(38,0) aka integer
  , src_COPDM --number(38,0) aka integer.  M for mid year?
  , src_COPDE --date E for 'event?' or 'ever?'  
  --possible new field will be src_COPD_NEW --number(38,0) aka integer
  --possible new field will be src_COPDE_NEW --date E for 'event?' or 'ever?' 
  , SRC_CHF --number(38,0) aka integer
  , SRC_CHFM --number(38,0) aka integer. M for mid year?
  , SRC_CHFE --date E for 'event?' or 'ever?'
  --possible new field will be src_CHF_NEW --number(38,0) aka integer
  --possible new field will be src_CHFE_NEW --date  E for 'event?' or 'ever?' 
  , LOAD_PERIOD 
  , RECORD_STATUS_CD 
FROM ods.MDPCP_BENED_YEAR 
WHERE FK_BENE_ID IN (SELECT top 5 fk_bene_id FROM ods.MDPCP_BENED WHERE RECORD_STATUS_CD = 'a')
AND RECORD_STATUS_CD = 'a'
ORDER BY FK_BENE_ID, load_period DESC



SELECT 
  --FK_GROUP_ID
  --,
  FK_NETWORK_ID
  ,FK_PATIENT_ID
  ,FK_PROVIDER_ID
  ,MONTH_CD
  ,ORG_ID
  , count(*) AS rwCnt
FROM prod_ADAUGEOPI.INSIGHTS.PATIENT_ROSTER
--PROD_CITYBLOCKDCE.
GROUP BY 
  --FK_GROUP_ID
  --,
  FK_NETWORK_ID
  ,FK_PATIENT_ID
  ,FK_PROVIDER_ID
  ,MONTH_CD
  ,ORG_ID
HAVING count(*) > 1
ORDER BY FK_NETWORK_ID
,FK_PATIENT_ID
,FK_PROVIDER_ID
,MONTH_CD
,ORG_ID


SELECT 
  FK_GROUP_ID
  ,
  FK_NETWORK_ID
  ,FK_PATIENT_ID
  --,FK_PROVIDER_ID
  ,MONTH_CD
  ,ORG_ID
  , count(*) AS rwCnt
FROM PROD_ILUMEDPI.INSIGHTS.PATIENT_ROSTER
--PROD_CANODCE.INSIGHTS.PATIENT_ROSTER
GROUP BY 
  FK_GROUP_ID
  ,
  FK_NETWORK_ID
  ,FK_PATIENT_ID
  --,FK_PROVIDER_ID
  ,MONTH_CD
  ,ORG_ID
HAVING count(*) > 1
ORDER BY FK_NETWORK_ID
,FK_PATIENT_ID



USE DATABASE int_a2024_fe;

SELECT * from (select
    distinct pxm.fk_patient_id as MBI,
    p.last_name as "Last Name",       
    p.first_name as "First Name", 
    p.date_of_birth as "Date of Birth",
--  CAST(pxm.risk_score AS varchar(16777216)) as "Risk Score", removed because my uploaded table loads blank instead of NULL
    pxm.month_cd,
    month_text.label as "Rolling 12 End Month",
    pxm.attribution_type as "Attribution Type",
    ap.npi_num as "Attributed Provider NPI",
    ap.name as "Attributed Provider Name",
    case when act.Visit_Type is NULL then 'No AWV or IPPE' else act.Visit_Type end as "Visit Type",
    act.Primary_Payer as "Primary Payer",
    act.npi_num as "AWV Provider NPI",
    act.name as "AWV Provider Name",
    act.activity_from_dt as "Activity Date",
    act.activity_from_month_cd as "Activity Month"
    --,NULL as "Notes"   
from insights.patient_x_month pxm
inner join (
    select      
        act.fk_patient_id,      
        act.activity_from_dt,
        act.activity_from_month_cd,     
        awvp.npi_num,
        awvp.name,
        case        
            when act.procedure_hcpcs_cd in ('G0438', 'G0439', 'G0468') then'AWV'        
            when act.procedure_hcpcs_cd in ('G0402', 'G0403', 'G0404', 'G0405') then 'IPPE'     
            end as Visit_Type,       
        case when act.claim_primary_payer_cd  = 'A' then 'Employer group health plan (EGHP) insurance for an aged beneficiary'
            when act.claim_primary_payer_cd  = 'B' then 'EGHP insurance for an end-stage renal disease (ESRD) beneficiary'
            when act.claim_primary_payer_cd  = 'C' then 'Conditional payment by Medicare; future reimbursement from the Public Health Service (PHS) expected'
            when act.claim_primary_payer_cd  = 'D' then 'No fault automobile insurance'
            when act.claim_primary_payer_cd  = 'E' then 'Workers compensation (WC)'
            when act.claim_primary_payer_cd  = 'F' then 'Public Health Service (PHS) or other Federal agency (other than VA)'
            when act.claim_primary_payer_cd  = 'G' then 'Working disabled beneficiary under age 65 with a local government health plan (LGHP)'
            when act.claim_primary_payer_cd  = 'H' then 'Black Lung (BL) program'
            when act.claim_primary_payer_cd  = 'I' then 'Department of Veterans Affairs'
            when act.claim_primary_payer_cd  = 'L' then 'Any Liability Insurance'
            when act.claim_primary_payer_cd  = 'M' then 'Override EGHP - Medicare is primary payer'
            when act.claim_primary_payer_cd  = 'M' then 'Override non-EGHP - Medicare is primary payer'
            else 'Medicare' end as Primary_Payer,
        roll12.month_cd  
    from insights.activity act 
    left join insights.provider awvp
    on act.fk_provider_rendering_id = awvp.pk_provider_id
    left join int_COMMON_FE.REF.code_month_rollup roll12
    on act.activity_from_month_cd = roll12.incl_month_cd
    where act.procedure_hcpcs_cd in ('G0438', 'G0439', 'G0468', 'G0402', 'G0403', 'G0404', 'G0405')
    group by act.fk_patient_id,
    act.activity_from_dt,
    act.activity_from_month_cd,
    awvp.npi_num,
    awvp.name,
    Visit_Type,
    Primary_Payer,
    roll12.month_cd 
    ) act
on pxm.fk_patient_id = act.fk_patient_id
and pxm.month_cd = act.month_cd
inner join insights.patient p
on pxm.fk_patient_id = p.pk_patient_id
left join insights.provider ap
    on pxm.at_time_primary_prov_nh = ap.pk_provider_id
inner join int_COMMON_FE.REF.code_month_rollup month_text
    on pxm.month_cd = month_text.month_cd
    and month_text.rollup_type_cd = 'month'
where NOT p.deceased_flag 
and pxm.at_time_network_nh in ('cj_net|A2024|MLH1','cj_net|A2024|JH1','cj_net|A2024|DH1','cj_net|A2024|HRH1','aco_nh|A2024|A2024')
  and  at_time_tin_fac_nh in ('tin|232359401','tin|231352152','tin|232691968','tin|231355135','tin|030503934','tin|232809585','tin|462624868','tin|251902419','tin|233092765','tin|464855345','tin|232826618','tin|233044300','tin|231714249','tin|455341589','tin|202862477','tin|232696460','tin|232883170','tin|460562524','tin|232067171','tin|232076126','tin|232939030','tin|232858320','tin|231427765','tin|232789777','tin|232368197','tin|232678055','tin|232521457','tin|232408570','tin|232721151','tin|320090935','tin|232013058','tin|203471772','tin|232578098','tin|232176338','tin|232723827','tin|201822899','ccn_num|391901','tin|231476328','tin|233092579','tin|232604356','tin|232086521','tin|232822647','tin|231582931','tin|231856679','tin|460779942','tin|232178136','tin|231739482','ccn_num|391825','tin|231907685','tin|463272165','tin|233074367','tin|232077750','tin|232557833','tin|233094765','tin|231745594','tin|233066504','tin|232109521','tin|232706792','tin|232472290','tin|461634755','tin|232727350','tin|232436929','tin|232630682','tin|232308668','tin|222164755','tin|208533952','tin|200856641','tin|711040272','tin|273976544','ccn_num|391081','tin|233011254','tin|233084552','tin|233060775','tin|232934360','ccn_num|391975','tin|232432388','tin|455433724','ccn_num|391052','tin|232982857','tin|451295794','tin|232774938','tin|232555950','tin|830502251','tin|232666989','tin|232603174','tin|232668767','ccn_num|391832','tin|222999919','tin|233031148','tin|233065923','tin|196384293','tin|231934688','tin|461845738','tin|263083667','tin|232992246','tin|223537847','tin|233088322','ccn_num|391012','tin|232841275','tin|232151807','ccn_num|391113','tin|232830745','tin|204371535','tin|753243288','tin|232843537','tin|232992171','tin|300472026','tin|470888939','tin|043692308','tin|233090795','tin|611410199','tin|233006990','tin|232264644','tin|421586601','tin|232767717','tin|262328128','ccn_num|391015','tin|232625125','tin|233020694','tin|233024823','tin|260027547','tin|210507371','tin|232981147','tin|181403198','tin|320070694','tin|233093818','tin|232728349','tin|510423175','tin|232798510','tin|232735767','tin|232762837','tin|233091795','tin|233067683','tin|320006195','tin|461604417','tin|232221681','tin|770632993','tin|261445277','tin|233076346','tin|232818030','tin|165461772','tin|233064103','ccn_num|391055','tin|233048347','ccn_num|391084','tin|223775463','tin|205039668','tin|270296624','tin|232249231','tin|232608824','tin|461837331','tin|232840780','tin|271838257','tin|264069998','tin|233043160','tin|232418909') 
  and at_time_primary_prov_nh in ('npi_num|1548297443','npi_num|1962408286','npi_num|1366445322','npi_num|1124449434','npi_num|1972641926','npi_num|1386888121','npi_num|1740215714','npi_num|1962582460','npi_num|1033232434','npi_num|1982670527','npi_num|1326274259','npi_num|1851727150','npi_num|1750365557','npi_num|1649317058','npi_num|1881615490','npi_num|1932193182','npi_num|1568488468','npi_num|1578572962','npi_num|1306847835','npi_num|1831477710','npi_num|1609921519','npi_num|1205806551','npi_num|1518394667','npi_num|1225087877','npi_num|1396972642','npi_num|1568476729','npi_num|1073502308','npi_num|1396842126','npi_num|1083779540','npi_num|1154306033','npi_num|1225303449','npi_num|1386618163','npi_num|1396748257','npi_num|1063724490','npi_num|1780687004','npi_num|1376703538','npi_num|1184842601','npi_num|1386688737','npi_num|1962662361','npi_num|1023545662','npi_num|1992850242','npi_num|1467737601','npi_num|1497750905','npi_num|1699935957','npi_num|1942672464','npi_num|1336229459','npi_num|1629049804','npi_num|1033499371','npi_num|1134277767','npi_num|1013901909','npi_num|1467747436','npi_num|1003008574','npi_num|1164445482','npi_num|1164443412','npi_num|1588605232','npi_num|1184620627','npi_num|1275705485','npi_num|1275564619','npi_num|1730159203','npi_num|1326480104','npi_num|1285809434','npi_num|1710987367','npi_num|1750347597','npi_num|1922009174','npi_num|1326314337','npi_num|1255575130','npi_num|1457765158','npi_num|1679599948','npi_num|1437472941','npi_num|1346260130','npi_num|1720215890','npi_num|1508126806','npi_num|1558582197','npi_num|1477228773','npi_num|1831259340','npi_num|1033161682','npi_num|1114048659','npi_num|1407921281','npi_num|1215228531','npi_num|1780915090','npi_num|1770593899','npi_num|1740229954','npi_num|1568597839','npi_num|1588891063','npi_num|1477580702','npi_num|1508997180','npi_num|1811993363','npi_num|1235298662','npi_num|1730510298','npi_num|1851540660','npi_num|1518141860','npi_num|1457513038','npi_num|1295730729','npi_num|1780810986','npi_num|1295076685','npi_num|1891056891','npi_num|1326067018','npi_num|1528472651','npi_num|1285749838','npi_num|1851687305','npi_num|1215970207','npi_num|1255332367','npi_num|1265859029','npi_num|1730486366','npi_num|1619231305','npi_num|1568499184','npi_num|1306099882','npi_num|1588838262','npi_num|1801157250','npi_num|1972862423','npi_num|1265417463','npi_num|1548280803','npi_num|1750332300','npi_num|1326080185','npi_num|1497269377','npi_num|1184687402','npi_num|1518971761','npi_num|1043428063','npi_num|1215986864','npi_num|1497807879','npi_num|1518222348','npi_num|1932454733','npi_num|1710955463','npi_num|1962723148','npi_num|1518027192','npi_num|1366554552','npi_num|1639179005','npi_num|1881790483','npi_num|1770504821','npi_num|1659854867','npi_num|1487891289','npi_num|1659384014','npi_num|1174546881','npi_num|1700826351','npi_num|1235136078','npi_num|1528038098','npi_num|1013231372','npi_num|1538496989','npi_num|1164952669','npi_num|1801819529','npi_num|1689678658','npi_num|1215979331','npi_num|1871678961','npi_num|1114427465','npi_num|1952539538','npi_num|1770785172','npi_num|1699170787','npi_num|1053822437','npi_num|1295831238','npi_num|1245240449','npi_num|1144257452','npi_num|1932112026','npi_num|1114397387','npi_num|1619961455','npi_num|1043234438','npi_num|1740423771','npi_num|1659471217','npi_num|1285780239','npi_num|1750608790','npi_num|1497727044','npi_num|1659626216','npi_num|1700817566','npi_num|1770566655','npi_num|1831112887','npi_num|1689686313','npi_num|1013905512','npi_num|1376566984','npi_num|1487628962','npi_num|1548637788','npi_num|1750568879','npi_num|1689614042','npi_num|1093706996','npi_num|1083085252','npi_num|1669434460','npi_num|1437266137','npi_num|1497874440','npi_num|1457374530','npi_num|1730615550','npi_num|1396187456','npi_num|1710912217','npi_num|1043248271','npi_num|1265474217','npi_num|1548420888','npi_num|1952697674','npi_num|1295737146','npi_num|1134140841','npi_num|1053652511','npi_num|1770803330','npi_num|1710110028','npi_num|1912243692','npi_num|1023038718','npi_num|1043779630','npi_num|1245452945','npi_num|1760590749','npi_num|1669536959','npi_num|1578983987','npi_num|1912369620','npi_num|1457389736','npi_num|1902032352','npi_num|1851734255','npi_num|1740472745','npi_num|1134179278','npi_num|1992149769','npi_num|1437665502','npi_num|1841426509','npi_num|1528142452','npi_num|1093226557','npi_num|1902984255','npi_num|1972095255','npi_num|1285639724','npi_num|1962588335','npi_num|1568623700','npi_num|1083855514','npi_num|1093050759','npi_num|1801821491','npi_num|1023202157','npi_num|1760693097','npi_num|1699771709','npi_num|1316993058','npi_num|1194746719','npi_num|1356756555','npi_num|1467634824','npi_num|1255651717','npi_num|1659736247','npi_num|1073802252','npi_num|1396378584','npi_num|1477579910','npi_num|1942581707','npi_num|1962653998','npi_num|1508801085','npi_num|1205869104','npi_num|1083017750','npi_num|1871685396','npi_num|1790973147','npi_num|1174595532','npi_num|1245207273','npi_num|1558763433','npi_num|1407899586','npi_num|1659310159','npi_num|1184644874','npi_num|1205851474','npi_num|1154697316','npi_num|1326093352','npi_num|1356654289','npi_num|1164689840','npi_num|1679640320','npi_num|1588661706','npi_num|1851738702','npi_num|1912260290','npi_num|1447511043','npi_num|1285925925','npi_num|1205824232','npi_num|1174848071','npi_num|1023093432','npi_num|1184730384','npi_num|1124016886','npi_num|1295719474','npi_num|1124044789','npi_num|1982786786','npi_num|1649432956','npi_num|1720036239','npi_num|1801265103','npi_num|1568621126','npi_num|1275525768','npi_num|1265403349','npi_num|1225061617','npi_num|1720174683','npi_num|1265761241','npi_num|1023082369','npi_num|1770672040','npi_num|1962477349','npi_num|1952428070','npi_num|1912948142','npi_num|1962425678','npi_num|1235182692','npi_num|1265539050','npi_num|1659437549','npi_num|1871698662','npi_num|1558538991','npi_num|1235405515','npi_num|1982676078','npi_num|1467696716','npi_num|1689732216','npi_num|1164745683','npi_num|1154834596','npi_num|1356383442','npi_num|1083084040','npi_num|1902394968','npi_num|1679897540','npi_num|1568873875','npi_num|1366607541','npi_num|1073909925','npi_num|1942403332','npi_num|1568098358','npi_num|1003966920','npi_num|1962433649','npi_num|1578584835','npi_num|1760506059','npi_num|1366475287','npi_num|1962408351','npi_num|1962723205','npi_num|1639122492','npi_num|1306862602','npi_num|1831364348','npi_num|1932599578','npi_num|1669453213','npi_num|1134440027','npi_num|1528234432','npi_num|1326063082','npi_num|1164668927','npi_num|1083090096','npi_num|1043685720','npi_num|1164488417','npi_num|1245466598','npi_num|1750301453','npi_num|1891201968','npi_num|1164873147','npi_num|1033115134','npi_num|1639210578','npi_num|1154396802','npi_num|1538160825','npi_num|1659763878','npi_num|1306834155','npi_num|1568486181','npi_num|1770577850','npi_num|1619973039','npi_num|1982684882','npi_num|1942256854','npi_num|1063405710','npi_num|1851556534','npi_num|1538188487','npi_num|1114980869','npi_num|1629019211','npi_num|1962793059','npi_num|1386663797','npi_num|1144252057','npi_num|1801184684','npi_num|1851365308','npi_num|1205179611','npi_num|1194791574','npi_num|1194898965','npi_num|1821099383','npi_num|1316266729','npi_num|1548464068','npi_num|1356752141','npi_num|1023075090','npi_num|1891089082','npi_num|1487099461','npi_num|1568420529','npi_num|1093257008','npi_num|1215145040','npi_num|1104826825','npi_num|1285802421','npi_num|1649717893','npi_num|1114039682','npi_num|1407156029','npi_num|1285894063','npi_num|1588635700','npi_num|1417316878','npi_num|1285848853','npi_num|1669902946','npi_num|1164476016','npi_num|1265409676','npi_num|1912313966','npi_num|1558383984','npi_num|1487674701','npi_num|1528064094','npi_num|1689829095','npi_num|1750350500','npi_num|1003042714','npi_num|1720314826','npi_num|1740272079','npi_num|1396952206','npi_num|1750387791','npi_num|1568576494','npi_num|1699797860','npi_num|1609076876','npi_num|1033550991','npi_num|1609848456','npi_num|default_npi_391901','npi_num|1962875401','npi_num|1982728283','npi_num|1699749036','npi_num|1760738512','npi_num|1952532327','npi_num|1154434702','npi_num|1003207598','npi_num|1407959000','npi_num|1366499758','npi_num|1790786192','npi_num|1487033957','npi_num|1255524336','npi_num|1952483182','npi_num|1760874283','npi_num|1558791574','npi_num|1669667366','npi_num|1073920930','npi_num|1609816396','npi_num|1386613149','npi_num|1689805616','npi_num|1215096565','npi_num|1558365072','npi_num|1891049235','npi_num|1003863937','npi_num|1619144060','npi_num|1679678825','npi_num|1326132457','npi_num|1225463920','npi_num|1457588501','npi_num|1073599908','npi_num|1891167607','npi_num|1881028884','npi_num|1750644803','npi_num|1023008596','npi_num|1306144514','npi_num|1689697740','npi_num|1114049566','npi_num|1255349759','npi_num|1104880970','npi_num|1588666580','npi_num|1942482112','npi_num|1083630685','npi_num|1013110030','npi_num|1780093757','npi_num|1164450946','npi_num|1225098064','npi_num|1427251594','npi_num|1518149970','npi_num|1902126493','npi_num|1174903256','npi_num|1336167089','npi_num|1730137944','npi_num|1841634466','npi_num|1871515528','npi_num|1760452478','npi_num|1265400931','npi_num|1831104496','npi_num|1467454116','npi_num|1346342821','npi_num|1568442432','npi_num|1871550566','npi_num|1295121556','npi_num|1487692505','npi_num|1801897251','npi_num|1689677650','npi_num|1255628897','npi_num|1205002698','npi_num|1659350130','npi_num|1518963925','npi_num|1548573140','npi_num|1821237520','npi_num|1033132279','npi_num|1215957337','npi_num|1215955448','npi_num|1154491785','npi_num|1750477436','npi_num|1558310029','npi_num|1063889913','npi_num|1598802910','npi_num|1891051256','npi_num|1467421321','npi_num|1538174065','npi_num|1750337648','npi_num|1952561961','npi_num|1720419864','npi_num|1073913596','npi_num|1689659922','npi_num|1912092230','npi_num|1972592459','npi_num|1063680700','npi_num|1346782141','npi_num|1790886471','npi_num|1982823985','npi_num|1366487340','npi_num|1174540843','npi_num|1366885592','npi_num|1124139621','npi_num|1730135393','npi_num|1215149661','npi_num|1952929077','npi_num|1497205157','npi_num|1043284524','npi_num|1679849269','npi_num|1265491419','npi_num|1265799126','npi_num|1790934321','npi_num|1750322418','npi_num|1922016930','npi_num|1619383460','npi_num|1104037274','npi_num|1467648519','npi_num|1356638480','npi_num|1275713042','npi_num|1225127517','npi_num|1619486966','npi_num|1043626559','npi_num|1902827397','npi_num|1104085745','npi_num|1457326191','npi_num|1417019985','npi_num|1174542823','npi_num|1962794743','npi_num|1093711574','npi_num|1013021476','npi_num|1215163787','npi_num|1639191984','npi_num|1447411699','npi_num|1922395714','npi_num|1255387924','npi_num|1013114461','npi_num|1831416064','npi_num|1578548939','npi_num|1679522957','npi_num|1841213477','npi_num|1780840553','npi_num|1285886275','npi_num|1114210952','npi_num|1407070501','npi_num|1730472408','npi_num|1124416185','npi_num|1093197949','npi_num|1568759884','npi_num|1275521254','npi_num|1578724639','npi_num|1487646014','npi_num|1588828230','npi_num|1760400451','npi_num|1679838510','npi_num|1912498767','npi_num|1487896627','npi_num|1073824132','npi_num|1043427172','npi_num|1265552970','npi_num|1154696326','npi_num|1841345600','npi_num|1639234693','npi_num|1497864375','npi_num|1306001599','npi_num|1659323350','npi_num|1144462177','npi_num|1215956974','npi_num|1891725891','npi_num|1083694442','npi_num|1669539581','npi_num|1205805439','npi_num|1720281017','npi_num|1477023307','npi_num|1467466607','npi_num|1356796767','npi_num|1972639334','npi_num|1558337279','npi_num|1487933719','npi_num|1053620815','npi_num|1124245162','npi_num|1043284763','npi_num|1396938205','npi_num|1477576122','npi_num|1013995588','npi_num|1275539710','npi_num|1568442929','npi_num|1598766982','npi_num|1609873991','npi_num|1386667996','npi_num|1972984458','npi_num|1285676171','npi_num|1760461602','npi_num|1497103790','npi_num|1245522481','npi_num|1538406947','npi_num|1629090352','npi_num|1811121890','npi_num|1164626289','npi_num|1992742977','npi_num|1467480319','npi_num|1033120746','npi_num|1962881458','npi_num|1275649212','npi_num|1205035730','npi_num|1013912708','npi_num|1003862665','npi_num|1285187260','npi_num|1750494902','npi_num|1770660698','npi_num|1275767915','npi_num|1346561230','npi_num|1952374571','npi_num|1215976139','npi_num|1447559927','npi_num|1972867521','npi_num|1457526170','npi_num|1184762056','npi_num|1285706515','npi_num|1104955376','npi_num|1902928047','npi_num|1124384573','npi_num|1578075768','npi_num|1912206301','npi_num|1184913865','npi_num|1609092576','npi_num|1396764197','npi_num|1104892660','npi_num|1992774996','npi_num|1255784195','npi_num|1225131824','npi_num|1255351003','npi_num|1073513867','npi_num|1376587840','npi_num|1295048015','npi_num|1992744783','npi_num|1295090074','npi_num|1265403240','npi_num|1528142791','npi_num|1730372632','npi_num|1144249947','npi_num|1407865793','npi_num|1033283205','npi_num|1639434764','npi_num|1609262161','npi_num|1700223013','npi_num|1508377250','npi_num|1942265053','npi_num|1396180592','npi_num|1154514131','npi_num|1558784124','npi_num|1184837874','npi_num|1083856918','npi_num|1639188659','npi_num|1699064865','npi_num|1053371930','npi_num|1447249347','npi_num|1699708099','npi_num|1669498903','npi_num|1447223474','npi_num|1609892215','npi_num|1164826525','npi_num|1801892161','npi_num|1245480607','npi_num|1326300187','npi_num|1568867679','npi_num|1518002567','npi_num|1629211644','npi_num|1871131391','npi_num|1659572337','npi_num|1508825316','npi_num|1871040295','npi_num|1093759995','npi_num|1073808531','npi_num|1669638573','npi_num|1659322303','npi_num|1326216342','npi_num|1679150080','npi_num|1952627317','npi_num|1235123886','npi_num|1376663815','npi_num|1043232598','npi_num|1396874855','npi_num|1033122494','npi_num|1326122714','npi_num|1003834177','npi_num|1598108144','npi_num|1881007987','npi_num|1427169382','npi_num|1700013968','npi_num|1669414827','npi_num|1740518752','npi_num|1699126557','npi_num|1568992394','npi_num|1144385469','npi_num|1649236423','npi_num|1871552711','npi_num|1053686931','npi_num|1992096135','npi_num|1659366987','npi_num|1912930702','npi_num|1184145948','npi_num|1053463885','npi_num|1568751386','npi_num|1093291775','npi_num|1902874654','npi_num|1114337508','npi_num|1740217652','npi_num|1285794545','npi_num|1770698185','npi_num|1457300733','npi_num|1922208180','npi_num|1750345880','npi_num|1598760043','npi_num|1194720615','npi_num|1497719249','npi_num|1447403431','npi_num|1952301707','npi_num|1538184791','npi_num|1982043584','npi_num|1427012939','npi_num|1326498361','npi_num|1629335708','npi_num|1912315839','npi_num|1992749535','npi_num|1992143887','npi_num|1053391854','npi_num|1568482230','npi_num|1285126730','npi_num|1356343297','npi_num|1427302892','npi_num|1720149503','npi_num|1821178690','npi_num|1679782528','npi_num|1558558304','npi_num|1952505810','npi_num|1205020807','npi_num|1144268913','npi_num|1295877983','npi_num|1912024662','npi_num|1477839207','npi_num|1194729566','npi_num|1831415231','npi_num|1629070644','npi_num|1609037654','npi_num|1295724763','npi_num|1518127364','npi_num|1265525240','npi_num|1801887856','npi_num|1932342185','npi_num|1902122567','npi_num|1912113267','npi_num|1316327885','npi_num|1639100043','npi_num|1962409227','npi_num|1831161561','npi_num|1417927179','npi_num|1700825957','npi_num|1457797706','npi_num|1457316804','npi_num|1568440451','npi_num|1356725022','npi_num|1033243480','npi_num|1720125701','npi_num|1255449476','npi_num|1770529935','npi_num|1689012874','npi_num|1578792461','npi_num|1508360538','npi_num|1528125903','npi_num|1770711350','npi_num|1295745396','npi_num|1922362235','npi_num|1770557050','npi_num|1871791673','npi_num|1497761225','npi_num|1215932231','npi_num|1659684942','npi_num|1275929093','npi_num|1306038955','npi_num|1669498861','npi_num|1750343950','npi_num|1669817045','npi_num|1982624235','npi_num|1821016486','npi_num|1568467587','npi_num|1003861733','npi_num|1407806037','npi_num|1134410384','npi_num|1144631730','npi_num|1528273760','npi_num|1447315650','npi_num|1841257953','npi_num|1871741629','npi_num|1538556451','npi_num|1528292091','npi_num|1992163422','npi_num|1497818900','npi_num|1720084296','npi_num|1548255912','npi_num|1194777466','npi_num|1558368811','npi_num|1437129574','npi_num|1154438059','npi_num|1568489334','npi_num|1235231291','npi_num|1457484974','npi_num|1154378891','npi_num|1447414453','npi_num|1427037365','npi_num|1174683114','npi_num|1871536706','npi_num|1861463507','npi_num|1336277060','npi_num|1700280245','npi_num|1750338810','npi_num|1396139705','npi_num|1619060902','npi_num|1891733721','npi_num|1871516096','npi_num|1467486381','npi_num|1972556389','npi_num|1609871615','npi_num|1356734735','npi_num|1235152315','npi_num|1659691566','npi_num|1023010006','npi_num|1467982066','npi_num|1649215815','npi_num|1477577310','npi_num|1407216773','npi_num|1538136833','npi_num|1700295912','npi_num|1679839252','npi_num|1548375348','npi_num|1194713495','npi_num|1679578629','npi_num|1194275354','npi_num|1437329141','npi_num|1407882467','npi_num|1558463232','npi_num|1861480584','npi_num|1407064652','npi_num|1710085147','npi_num|1861742124','npi_num|1669852547','npi_num|1720175474','npi_num|1255370722','npi_num|1942295415','npi_num|1295892693','npi_num|1013987783','npi_num|1679579916','npi_num|1942438528','npi_num|1548233083','npi_num|1548248677','npi_num|1538565221','npi_num|1427021872','npi_num|1841559416','npi_num|1790785012','npi_num|1457369191','npi_num|1619932688','npi_num|1427054725','npi_num|1841480282','npi_num|1891880563','npi_num|1528279932','npi_num|1285787267','npi_num|1013295856','npi_num|1881605897','npi_num|1851379325','npi_num|1750896866','npi_num|1063483527','npi_num|1689767915','npi_num|1578784260','npi_num|1750385696','npi_num|1609272970','npi_num|1457356917','npi_num|1285897173','npi_num|1053522045','npi_num|1659799682','npi_num|1124670484','npi_num|1942206495','npi_num|1508807983','npi_num|1750398350','npi_num|1982869582','npi_num|1306848304','npi_num|1033152319','npi_num|1235182171','npi_num|1992036867','npi_num|1609811066','npi_num|1477524007','npi_num|1043592454','npi_num|1194931907','npi_num|1053416008','npi_num|1154647212','npi_num|1902988397','npi_num|1205397411','npi_num|1295817013','npi_num|1053571000','npi_num|1629747571','npi_num|1518009984','npi_num|1083872048','npi_num|1154392124','npi_num|1336686567','npi_num|1548221591','npi_num|1982091344','npi_num|1649292426','npi_num|1043408206','npi_num|1073898516','npi_num|1568513653','npi_num|1750597654','npi_num|1568919488','npi_num|1992709968','npi_num|1427054741','npi_num|1578621587','npi_num|1295840692','npi_num|1932256237','npi_num|1124177431','npi_num|1669542106','npi_num|1083611339','npi_num|1992176143','npi_num|1487940193','npi_num|1801892823','npi_num|1992204556','npi_num|1740630292','npi_num|1093732448','npi_num|1790739878','npi_num|1063673317','npi_num|1922073923','npi_num|1144203480','npi_num|1578610267','npi_num|1154546380','npi_num|1174184147','npi_num|1508208133','npi_num|1265418073','npi_num|1295832723','npi_num|1790800092','npi_num|1902241425','npi_num|1477083996','npi_num|1407115348','npi_num|1346245073','npi_num|1194228445','npi_num|1346212594','npi_num|1134394695','npi_num|1760698864','npi_num|1578579108','npi_num|1740257955','npi_num|1770725822','npi_num|1386638963','npi_num|1447793344','npi_num|1144694118','npi_num|1033166541','npi_num|1710402730','npi_num|1104378454','npi_num|1003298241','npi_num|1104051473','npi_num|1679776926','npi_num|1598153777','npi_num|1346271244','npi_num|1518967884','npi_num|1154339893','npi_num|1083664965','npi_num|1609050525','npi_num|1932358769','npi_num|1801867361','npi_num|1497798078','npi_num|1710908926','npi_num|1295738292','npi_num|1558388348','npi_num|1518986215','npi_num|1538557582','npi_num|1467486704','npi_num|1316234263','npi_num|1033187646','npi_num|1104823475','npi_num|1356777353','npi_num|1124055058','npi_num|1144218835','npi_num|1033385489','npi_num|1679990899','npi_num|1154879179','npi_num|1548503907','npi_num|1164442984','npi_num|1609118249','npi_num|1215955364','npi_num|1700172376','npi_num|1033450689','npi_num|1376996819','npi_num|1578779245','npi_num|1851856165','npi_num|1184625865','npi_num|1235186453','npi_num|1932692043','npi_num|1316308042','npi_num|1942406111','npi_num|1477802528','npi_num|1659430965','npi_num|1407081748','npi_num|1225195241','npi_num|1023302593','npi_num|1538207600','npi_num|1821435942','npi_num|1720323496','npi_num|1568474617','npi_num|1710271895','npi_num|1811330855','npi_num|1962613125','npi_num|1578046603','npi_num|1376582510','npi_num|1528152618','npi_num|1619947553','npi_num|1457463648','npi_num|1750515078','npi_num|1083063895','npi_num|1124141148','npi_num|1720068422','npi_num|1629049606','npi_num|1952714040','npi_num|1174819130','npi_num|1568437929','npi_num|1609849256','npi_num|1467490516','npi_num|1356729503','npi_num|1649289315','npi_num|1043504871','npi_num|1942292313','npi_num|1932204617','npi_num|1821160276','npi_num|1275500852','npi_num|1174575849','npi_num|1457658015','npi_num|1861430985','npi_num|1144238676','npi_num|1609895838','npi_num|1821416124','npi_num|1952761017','npi_num|1861588006','npi_num|1306106513','npi_num|1215988621','npi_num|1336234103','npi_num|1538102645','npi_num|1053413682','npi_num|1861448714','npi_num|1285913996','npi_num|1508283565','npi_num|1063859429','npi_num|1275860389','npi_num|1245373067','npi_num|1598780728','npi_num|1720221062','npi_num|1598931206','npi_num|1104863067','npi_num|1053535344','npi_num|1750630752','npi_num|1235347105','npi_num|1760412100','npi_num|1134216179','npi_num|1427564830','npi_num|1407113988','npi_num|1760483945','npi_num|1104257872','npi_num|1578529210','npi_num|1760675714','npi_num|1477911295','npi_num|1386985216','npi_num|1023030368','npi_num|1922082536','npi_num|1982672093','npi_num|1063423267','npi_num|1154582237','npi_num|1427098847','npi_num|1841598240','npi_num|1124088513','npi_num|1740257708','npi_num|1578019014','npi_num|1851849152','npi_num|1699902643','npi_num|1740462043','npi_num|1912431529','npi_num|1780662494','npi_num|1023140605','npi_num|1275041477','npi_num|1689669194','npi_num|1528450400','npi_num|1306945613','npi_num|1285659425','npi_num|1508236084','npi_num|1033474861','npi_num|1356617732','npi_num|1073932463','npi_num|1225291123','npi_num|1528389178','npi_num|1467688671','npi_num|1922236017','npi_num|1851396600','npi_num|1780842690','npi_num|1225086218','npi_num|1740443290','npi_num|1982992095','npi_num|1629417985','npi_num|1245262567','npi_num|1558381186','npi_num|1235429119','npi_num|1558362970','npi_num|1396940581','npi_num|1639141435','npi_num|1164562310','npi_num|1659638294','npi_num|1790720050','npi_num|1619936515','npi_num|1457594319','npi_num|default_npi_391113','npi_num|1154554236','npi_num|1477559920','npi_num|1639599293','npi_num|1629387857','npi_num|1346322161','npi_num|1306856448','npi_num|1326444910','npi_num|1629074968','npi_num|1619297710','npi_num|1013990332','npi_num|1720243009','npi_num|1811949191','npi_num|1578542445','npi_num|1235112566','npi_num|1093785370','npi_num|1679581870','npi_num|1639387418','npi_num|1316985906','npi_num|1053607606','npi_num|1598761694','npi_num|1295898815','npi_num|1679684476','npi_num|1376648311','npi_num|1558302604','npi_num|1629074109','npi_num|1558351668','npi_num|1164451605','npi_num|1316233372','npi_num|1871595041','npi_num|1295757649','npi_num|1225031172','npi_num|1225054810','npi_num|1578721965','npi_num|1952636664','npi_num|1407836083','npi_num|1164455093','npi_num|1891860136','npi_num|1689909889','npi_num|1104197029','npi_num|1740528322','npi_num|1891082996','npi_num|1003835786','npi_num|1093374480','npi_num|1710233101','npi_num|1649241001','npi_num|1639193162','npi_num|1902824014','npi_num|1114959772','npi_num|1043750318','npi_num|1821510694','npi_num|1255354577','npi_num|1801053293','npi_num|1598761835','npi_num|1407885924','npi_num|1114995867','npi_num|1518104975','npi_num|1174158372','npi_num|1952360513','npi_num|1023154192','npi_num|1922134394','npi_num|1114967957','npi_num|1477541373','npi_num|1548368327','npi_num|1225279714','npi_num|1134109051','npi_num|1457372963','npi_num|1700149895','npi_num|1568578003','npi_num|1649259862','npi_num|1083059406','npi_num|1437179207','npi_num|1962507020','npi_num|1710957972','npi_num|1902068778','npi_num|1285630665','npi_num|1376944561','npi_num|1881640738','npi_num|1881994754','npi_num|1922014034','npi_num|1871689422','npi_num|1649228552','npi_num|1356682496','npi_num|1689793648','npi_num|1487625323','npi_num|1669665303','npi_num|1699252767','npi_num|1306000757','npi_num|1427304252','npi_num|1588604755','npi_num|1649561838','npi_num|1588027908','npi_num|1336661008','npi_num|1124021399','npi_num|1407008857','npi_num|1659370104','npi_num|1598193989','npi_num|1497729602','npi_num|1972632198','npi_num|1912968223','npi_num|1720165640','npi_num|1811102312','npi_num|1649323882','npi_num|1780645952','npi_num|1265460539','npi_num|1376593491','npi_num|1477551075','npi_num|1407110810','npi_num|1093030462','npi_num|1831132398','npi_num|1215132469','npi_num|1881861110','npi_num|1023490430','npi_num|1457475584','npi_num|1700205424','npi_num|1972806222','npi_num|1033442496','npi_num|1366526543','npi_num|1790764074','npi_num|1164912226','npi_num|1295934172','npi_num|1659492791','npi_num|1902829385','npi_num|1952743866','npi_num|1538136387','npi_num|1538149844','npi_num|1437156098','npi_num|1730103029','npi_num|1750355681','npi_num|1902996309','npi_num|1528031325','npi_num|1811948409','npi_num|1679555320','npi_num|1245247584','npi_num|1063476661','npi_num|1528491958','npi_num|1992967905','npi_num|1487618617','npi_num|1396776985','npi_num|1205805413','npi_num|1447256763','npi_num|1831160787','npi_num|1376864694','npi_num|1306811450','npi_num|1497834170','npi_num|1710106422','npi_num|1699186114','npi_num|1356343354','npi_num|1346553591','npi_num|1336162932','npi_num|1992294995','npi_num|1518930429','npi_num|1831181056','npi_num|1225545577','npi_num|1952372641','npi_num|1386670156','npi_num|1295769628','npi_num|1659769016','npi_num|1629335047','npi_num|1093234817','npi_num|1538161054','npi_num|1467413757','npi_num|1891081774','npi_num|1205282563','npi_num|1972741569','npi_num|1811332703','npi_num|1366503401','npi_num|1043721616','npi_num|1306852728','npi_num|1811130081','npi_num|1205304565','npi_num|1528301751','npi_num|1982670436','npi_num|1720076706','npi_num|1427068683','npi_num|1972150399','npi_num|1568464568','npi_num|1477719789','npi_num|1588679161','npi_num|1164785374','npi_num|1770711640','npi_num|1801864004','npi_num|1508860990','npi_num|1689607038','npi_num|1952384646','npi_num|1558467746','npi_num|1417003641','npi_num|1194003582','npi_num|1811980881','npi_num|1891764155','npi_num|1720511561','npi_num|1326383233','npi_num|1851383368','npi_num|1952360596','npi_num|1477807691','npi_num|1225169808','npi_num|1962472696','npi_num|1902865603','npi_num|1922024090','npi_num|1073569158','npi_num|1528292711','npi_num|1467459727','npi_num|1093716201','npi_num|1346426830','npi_num|1033591383','npi_num|1982679239','npi_num|1902281769','npi_num|1992318141','npi_num|1073892626','npi_num|1265701437','npi_num|1528223567','npi_num|1467919951','npi_num|1336192004','npi_num|1902860489','npi_num|1366401135','npi_num|1295388874','npi_num|1861463861','npi_num|1295705911','npi_num|1881773067','npi_num|1629009881','npi_num|1629237250','npi_num|1386699957','npi_num|1588644108','npi_num|1124061221','npi_num|1942234158','npi_num|1255354999','npi_num|1720001811','npi_num|1750338349','npi_num|1740294974','npi_num|1699709071','npi_num|1316108269','npi_num|1922008861','npi_num|1629099346','npi_num|1275695629','npi_num|1679505424','npi_num|1568494029','npi_num|1316074461','npi_num|1750434791','npi_num|1811954951','npi_num|1831429752','npi_num|1659699437','npi_num|1265455497','npi_num|1942202197','npi_num|1679572713','npi_num|1699066043','npi_num|1043205370','npi_num|1184739500','npi_num|1043384035','npi_num|1912127945','npi_num|1497014070','npi_num|1972568251','npi_num|1710934740','npi_num|1821485632','npi_num|1730185950','npi_num|1851331052','npi_num|1104971498','npi_num|1821288721','npi_num|1770584450','npi_num|1851657183','npi_num|1316975451','npi_num|1871648170','npi_num|1336411172','npi_num|1386704377','npi_num|1316918360','npi_num|1538260294','npi_num|1538672308','npi_num|1124035431','npi_num|1265482616','npi_num|1972579423','npi_num|1972711299','npi_num|1710196100','npi_num|1609847839','npi_num|1497076673','npi_num|1154301927','npi_num|1346247905','npi_num|1609800929','npi_num|1518077353','npi_num|1427090935','npi_num|1831537018','npi_num|1194180596','npi_num|1700969151','npi_num|1417930322','npi_num|1841703907','npi_num|1699245498','npi_num|1730354127','npi_num|1104897172','npi_num|1184638918','npi_num|1285695445','npi_num|1093926560','npi_num|1407923113','npi_num|1053484717','npi_num|1720177173','npi_num|1518277714','npi_num|1942515549','npi_num|1891728994','npi_num|1992737860','npi_num|1679836183','npi_num|1689170458','npi_num|1740302942','npi_num|1609286293','npi_num|1760607535','npi_num|1942206982','npi_num|1760485197','npi_num|1194145698','npi_num|1225007305','npi_num|1730617259','npi_num|1265745251','npi_num|1912987587','npi_num|1639154347','npi_num|1306236807','npi_num|1063701316','npi_num|1871538611','npi_num|1174582076','npi_num|1033258645','npi_num|1851653927','npi_num|1679846794','npi_num|1154361277','npi_num|1841495348','npi_num|1336699255','npi_num|1578741310','npi_num|1225053465','npi_num|1023014123','npi_num|1467457150','npi_num|1366458549','npi_num|1659312502','npi_num|1063723351','npi_num|1073721999','npi_num|1366768996','npi_num|1104009273','npi_num|1083053201','npi_num|1093072944','npi_num|default_npi_391015','npi_num|1619413879','npi_num|1881891083','npi_num|1356371058','npi_num|1215160288','npi_num|1821315060','npi_num|1902844129','npi_num|1407816721','npi_num|1942520481','npi_num|1851680045','npi_num|1033370986','npi_num|1568841021','npi_num|1447487269','npi_num|1760425755','npi_num|1962823997','npi_num|1811991292','npi_num|1073126157','npi_num|1659594018','npi_num|1508890104','npi_num|1336171206','npi_num|1013112051','npi_num|1306869854','npi_num|1609242940','npi_num|1245766591','npi_num|1952396376','npi_num|1669804316','npi_num|1750383683','npi_num|1326333881','npi_num|1235367541','npi_num|1174865802','npi_num|1598743064','npi_num|1356440770','npi_num|1184150518','npi_num|1700820040','npi_num|1932544467','npi_num|1497723944','npi_num|1811125933','npi_num|1861989279','npi_num|1194885251','npi_num|1518012897','npi_num|1710905294','npi_num|1639640949','npi_num|1548578438','npi_num|1174593958','npi_num|1538177316','npi_num|1992728869','npi_num|1689994105','npi_num|1679956122','npi_num|1346269081','npi_num|1093781148','npi_num|1497166177','npi_num|1174594923','npi_num|1023146693','npi_num|1376505743','npi_num|1700817251','npi_num|1740252402','npi_num|1760423792','npi_num|1518123710','npi_num|1366460578','npi_num|1013080670','npi_num|1679670129','npi_num|1407830649','npi_num|1699094607','npi_num|1720089170','npi_num|1356363782','npi_num|1881605103','npi_num|1770025033','npi_num|1770571937','npi_num|1831480292','npi_num|1427497023','npi_num|1265738801','npi_num|1356365225','npi_num|1679516850','npi_num|1023037546','npi_num|1285888743','npi_num|1790286102','npi_num|1326201732','npi_num|1063594877','npi_num|1134105208','npi_num|1629496294','npi_num|1245345651','npi_num|1962449322','npi_num|1316972359','npi_num|1225067630','npi_num|1437312592','npi_num|1669849261','npi_num|1174765622','npi_num|1821404088','npi_num|1083739015','npi_num|1619967635','npi_num|1720345655','npi_num|1427025626','npi_num|1184985905','npi_num|1467863324','npi_num|1407815905','npi_num|1073707501','npi_num|1609108380','npi_num|1659575900','npi_num|1851326466','npi_num|1386875938','npi_num|1346271038','npi_num|1548499510','npi_num|1770758369','npi_num|1932265709','npi_num|1851358360','npi_num|1932171154','npi_num|1417362328','npi_num|1184887937','npi_num|#NA','npi_num|1316225857','npi_num|1336165497','npi_num|1124354840','npi_num|1356847271','npi_num|1073657409','npi_num|1245206929','npi_num|1568725505','npi_num|1720213820','npi_num|1699758516','npi_num|1356395883','npi_num|1265795959','npi_num|1942771001','npi_num|1780950071','npi_num|1265438642','npi_num|1891780631','npi_num|1760460596','npi_num|1518932672','npi_num|1407292949','npi_num|1326010323','npi_num|1346689940','npi_num|1366880031','npi_num|1689694440','npi_num|1467487595','npi_num|1265430870','npi_num|1366490096','npi_num|1316970403','npi_num|1841493285','npi_num|1396755625','npi_num|1649245689','npi_num|1023098894','npi_num|1265667604','npi_num|1841266657','npi_num|1982607842','npi_num|1023036134','npi_num|1649662925','npi_num|1225000698','npi_num|1619911609','npi_num|1831224096','npi_num|1982600292','npi_num|1558554345','npi_num|1073580601','npi_num|1386667392','npi_num|1134568314','npi_num|1598737223','npi_num|1992125413','npi_num|1750674909','npi_num|1619347655','npi_num|1962667188','npi_num|1992399984','npi_num|1992944912','npi_num|1033159231','npi_num|1821013079','npi_num|1548550650','npi_num|1023218815','npi_num|1093764540','npi_num|1750563615','npi_num|1104046994','npi_num|1487946505','npi_num|1215246780','npi_num|1376625418','npi_num|1609883735','npi_num|1548559925','npi_num|1205925682','npi_num|1760672828','npi_num|1952345779','npi_num|1548643299','npi_num|1174594253','npi_num|1295124642','npi_num|1871865204','npi_num|1093909533','npi_num|1821529264','npi_num|1992932917','npi_num|1053501387','npi_num|1154483501','npi_num|1598760282','npi_num|1760488100','npi_num|1972033884','npi_num|1952352999','npi_num|1659374627','npi_num|1093738239','npi_num|1558509430','npi_num|1174052997','npi_num|1609347855','npi_num|1366414765','npi_num|1659373454','npi_num|1164434874','npi_num|1003817339','npi_num|1881697845','npi_num|1225055221','npi_num|1831193226','npi_num|1437215324','npi_num|1023036894','npi_num|1619918067','npi_num|1669492336','npi_num|1083741433','npi_num|1184658189','npi_num|1386646875','npi_num|1346585882','npi_num|1720392012','npi_num|1114968898','npi_num|1235367525','npi_num|1841239662','npi_num|1669449880','npi_num|1841290962','npi_num|1043581432','npi_num|1104806769','npi_num|1922259431','npi_num|1295073401','npi_num|1588810170','npi_num|1225033582','npi_num|1275662280','npi_num|1184886814','npi_num|1427374941','npi_num|1003292293','npi_num|1932127230','npi_num|1598870909','npi_num|1861592347','npi_num|1023014545','npi_num|1467870238','npi_num|1538165915','npi_num|1093226797','npi_num|1225079486','npi_num|1699743377','npi_num|1205156676','npi_num|1023498854','npi_num|1053428938','npi_num|1720309297','npi_num|1639265903','npi_num|1083680136','npi_num|1659486033','npi_num|1083612659','npi_num|1407836174','npi_num|1932105038','npi_num|1508071952','npi_num|default_npi_391832','npi_num|1932122637','npi_num|1831158930','npi_num|1205869989','npi_num|1093920217','npi_num|1316943830','npi_num|1992760219','npi_num|1104882646','npi_num|1215321500','npi_num|1942232244','npi_num|1982073250','npi_num|1740384056','npi_num|1831484138','npi_num|1528159555','npi_num|1679556427','npi_num|1245265354','npi_num|1497980346','npi_num|1922200922','npi_num|1710470562','npi_num|1992107056','npi_num|1699873570','npi_num|1497734602','npi_num|1285186809','npi_num|1285630566','npi_num|1033106828','npi_num|1386846806','npi_num|1619125127','npi_num|1679513873','npi_num|1366499345','npi_num|1902209612','npi_num|default_npi_391825','npi_num|1568481703','npi_num|1346238284','npi_num|1346746948','npi_num|1033132352','npi_num|1215950357','npi_num|1528095783','npi_num|1659371847','npi_num|1417218348','npi_num|1043706880','npi_num|1447360284','npi_num|1801818141','npi_num|1003834235','npi_num|1174647879','npi_num|1184937450','npi_num|1811155922','npi_num|1821018532','npi_num|1134542558','npi_num|1942576640','npi_num|1992741128','npi_num|1992013338','npi_num|1962404897','npi_num|1356414098','npi_num|1164749511','npi_num|1710387428','npi_num|1679624605','npi_num|1861521197','npi_num|1225691629','npi_num|1689697484','npi_num|1710311378','npi_num|1144588260','npi_num|1417983271','npi_num|1902297724','npi_num|1033102041','npi_num|1265669907','npi_num|1780883371','npi_num|1053633560','npi_num|1619473287','npi_num|1740878545','npi_num|1619126570','npi_num|1225397847','npi_num|1457386617','npi_num|1447318126','npi_num|1851874465','npi_num|1386688323','npi_num|1609875947','npi_num|1154546398','npi_num|1629076625','npi_num|1265792816','npi_num|1043367329','npi_num|1770560419','npi_num|1871934950','npi_num|1164780979','npi_num|1043672264','npi_num|1972595577','npi_num|1154395952','npi_num|1558472100','npi_num|1053570374','npi_num|1548209760','npi_num|1487635488','npi_num|1043285703','npi_num|1518093046','npi_num|1477562064','npi_num|1942261607','npi_num|1689623233','npi_num|1912958224','npi_num|1073503066','npi_num|1770505109','npi_num|1912903220','npi_num|1043236672','npi_num|1841298023','npi_num|1851395818','npi_num|1629237276','npi_num|1306826672','npi_num|1962458653','npi_num|1508403569','npi_num|1508223702','npi_num|1093732752','npi_num|1710951090','npi_num|1376078444','npi_num|1689853921','npi_num|1801874656','npi_num|1124085725','npi_num|1336119114','npi_num|1043877988','npi_num|1699145821','npi_num|1013469238','npi_num|1871732818','npi_num|1811252679','npi_num|1629515457','npi_num|1821174483','npi_num|1992715643','npi_num|1538176003','npi_num|1437523784','npi_num|1750380770','npi_num|1033177357','npi_num|1194766386','npi_num|1740787423','npi_num|1487685079','npi_num|1073519179','npi_num|1124020680','npi_num|1013231398','npi_num|1215358064','npi_num|1639558901','npi_num|1659397610','npi_num|1053827402','npi_num|1902809791','npi_num|1679765804','npi_num|1518279066','npi_num|1972508737','npi_num|1033299573','npi_num|1902907678','npi_num|1336124221','npi_num|1801148259','npi_num|1164860565','npi_num|1023049400','npi_num|1457399313','npi_num|1285650341','npi_num|1649403502','npi_num|1639469653','npi_num|1841759693','npi_num|1063957264','npi_num|1366456725','npi_num|1912200635','npi_num|1477555324','npi_num|1184029720','npi_num|1679677074','npi_num|1831409226','npi_num|1215346812','npi_num|1962648139','npi_num|1659345205','npi_num|1619992302','npi_num|1699862748','npi_num|1689968570','npi_num|1063451748','npi_num|1427027614','npi_num|1164634598','npi_num|1407179435','npi_num|1922367820','npi_num|1417954256','npi_num|1114112828','npi_num|1336209980','npi_num|1508867854','npi_num|1700971058','npi_num|1003897240','npi_num|1437219086','npi_num|1821035148','npi_num|1427082734','npi_num|1669425591','npi_num|1700271293','npi_num|1669726907','npi_num|1932101235','npi_num|1598767816','npi_num|1801270459','npi_num|1629195250','npi_num|1942671490','npi_num|1598735284','npi_num|1649745951','npi_num|1174603609','npi_num|1083603104','npi_num|1508850892','npi_num|1023204534','npi_num|1346301801','npi_num|1205934890','npi_num|1124352315','npi_num|1720378367','npi_num|1144853425','npi_num|1407869191','npi_num|1871658989','npi_num|1063487783','npi_num|1881617413','npi_num|1649432196','npi_num|1245346873','npi_num|1487073763','npi_num|1730197484','npi_num|1467453142','npi_num|default_npi_391975','npi_num|1295769511','npi_num|1598084451','npi_num|1366768822','npi_num|1134168339','npi_num|1346203205','npi_num|1356432462','npi_num|1083641419','npi_num|1346336930','npi_num|1760484521','npi_num|1750558474','npi_num|1306242300','npi_num|1609262559','npi_num|1447458351','npi_num|1124048194','npi_num|1538694419','npi_num|1952307340','npi_num|1043489271','npi_num|1073515375','npi_num|1396768420','npi_num|1912210766','npi_num|1487655734','npi_num|1255714358','npi_num|1629183033','npi_num|1225079213','npi_num|1346322823','npi_num|1659371383','npi_num|1720359896','npi_num|1154534766','npi_num|1306913033','npi_num|1013237981','npi_num|1003839606','npi_num|1942643705','npi_num|1093876765','npi_num|1447546338','npi_num|1891946356','npi_num|1457311581','npi_num|1083975015','npi_num|1336673227','npi_num|1871787739','npi_num|1538587308','npi_num|1972973634','npi_num|1861831018','npi_num|1477554541','npi_num|1952669038','npi_num|1245215136','npi_num|1508895749','npi_num|1538103924','npi_num|1881941870','npi_num|1982816260','npi_num|1083730352','npi_num|1124056395','npi_num|1750597381','npi_num|1801819131','npi_num|1497758452','npi_num|1720099815','npi_num|1730510918','npi_num|1902886575','npi_num|1154611325','npi_num|1821077934','npi_num|1174920086','npi_num|1679765234','npi_num|1912118951','npi_num|1225099559','npi_num|1245250943','npi_num|1356397061','npi_num|1821452590','npi_num|1326335761','npi_num|1356575096','npi_num|1942223078','npi_num|1932633658','npi_num|1205028149','npi_num|1376777169','npi_num|1851453252','npi_num|1285663187','npi_num|1265476170','npi_num|1740269018','npi_num|1801820188','npi_num|1447284674','npi_num|1215933692','npi_num|1780933358','npi_num|1346610359','npi_num|1497087902','npi_num|1629187836','npi_num|1649446006','npi_num|1720192677','npi_num|1629096383','npi_num|1447434469','npi_num|1336510163','npi_num|1144386582','npi_num|1427323195','npi_num|1740421346','npi_num|1306004510','npi_num|1841294220','npi_num|1790707198','npi_num|1982683207','npi_num|1811993140','npi_num|1679545743','npi_num|1306865191','npi_num|1508893926','npi_num|1467717009','npi_num|1386285203','npi_num|1902027311','npi_num|1245258276','npi_num|1578854824','npi_num|1639171598','npi_num|1669614327','npi_num|1568695682','npi_num|1528091410','npi_num|1083807002','npi_num|1336140979','npi_num|1073571626','npi_num|1487751475','npi_num|1003834581','npi_num|1427219344','npi_num|1427317114','npi_num|1154621449','npi_num|1841389863','npi_num|1588680003','npi_num|1558652198','npi_num|1619312147','npi_num|1790158566','npi_num|1205876406','npi_num|1053594408','npi_num|1336282490','npi_num|1538589288','npi_num|1942269931','npi_num|1639172562','npi_num|1386845006','npi_num|1295097376','npi_num|1003880352','npi_num|1467489740','npi_num|1669451605','npi_num|1689925067','npi_num|1578820338','npi_num|1548337041','npi_num|1790298487','npi_num|1366426934','npi_num|1326366246','npi_num|1841705795','npi_num|1346403334','npi_num|1700329117','npi_num|1518923796','npi_num|1548204985','npi_num|1528089745','npi_num|1760794366','npi_num|1104819150','npi_num|1891084000','npi_num|1588854863','npi_num|1215005582','npi_num|1467831941','npi_num|1225038771','npi_num|1942316658','npi_num|1295246189','npi_num|1376869776','npi_num|1497069629','npi_num|1598740714','npi_num|1043743966','npi_num|1942465950','npi_num|1851384291','npi_num|1386770519','npi_num|1043513153','npi_num|1083734305','npi_num|1457644502','npi_num|1275553455','npi_num|1265966295','npi_num|1184125684','npi_num|1699211029','npi_num|1013251347','npi_num|1871814046','npi_num|1225297070','npi_num|1588906044','npi_num|1548280217','npi_num|1275582025','npi_num|1982864328','npi_num|1114951191','npi_num|1437199734','npi_num|1760687552','npi_num|1912998782','npi_num|1063433696','npi_num|1790030724','npi_num|1932614260','npi_num|1912292970','npi_num|1881616126','npi_num|1053380923','npi_num|1407099930','npi_num|1619973278','npi_num|1841603644','npi_num|1588696884','npi_num|1659392926','npi_num|1801876032','npi_num|1255305017','npi_num|1821352295','npi_num|1346263480','npi_num|1548292873','npi_num|1417937715','npi_num|1215925979','npi_num|1528069200','npi_num|1326307943','npi_num|1013222413','npi_num|1508058470','npi_num|1891284790','npi_num|1770802795','npi_num|1669404869','npi_num|1174586358','npi_num|1629286018','npi_num|1043434533','npi_num|1871594978','npi_num|1922058916','npi_num|1386662864','npi_num|1467502245','npi_num|1053556167','npi_num|1770765919','npi_num|1669771176','npi_num|1639140122','npi_num|1528497054','npi_num|1891722195','npi_num|1023069598','npi_num|1962824292','npi_num|1356780274','npi_num|1952728396','npi_num|1184695363','npi_num|1275670655','npi_num|1205865094','npi_num|1063469344','npi_num|1013933548','npi_num|1053345934','npi_num|1750795811','npi_num|1033265004','npi_num|1710406947','npi_num|1669686978','npi_num|1831382555','npi_num|1639417850','npi_num|1295941425','npi_num|1689609596','npi_num|1043626476','npi_num|1568496818','npi_num|1104016674','npi_num|1437318763','npi_num|1821520263','npi_num|1902018468','npi_num|1730652850','npi_num|1154666857','npi_num|1538213822','npi_num|1528059763','npi_num|1144494170','npi_num|1316486012','npi_num|1164553426','npi_num|1043290372','npi_num|1649664806','npi_num|1235143223','npi_num|1366607780','npi_num|1649211525','npi_num|1396059069','npi_num|1770532293','npi_num|1821392978','npi_num|1437349321','npi_num|1255403747','npi_num|1669783098','npi_num|1174541734','npi_num|1700845633','npi_num|1972782878','npi_num|1083652051','npi_num|1710003652','npi_num|1518389410','npi_num|1780606202','npi_num|1184698367','npi_num|1922446699','npi_num|1154895241','npi_num|1285830356','npi_num|1710171327','npi_num|1518937895','npi_num|1942534433','npi_num|1487709093','npi_num|1528222866','npi_num|1366413593','npi_num|1710950019','npi_num|1376515122','npi_num|1053315523','npi_num|1750309803','npi_num|1306106836','npi_num|1598179657','npi_num|1902132871','npi_num|1518919281','npi_num|1871532507','npi_num|1659534238','npi_num|1073925764','npi_num|1205824109','npi_num|1902883846','npi_num|1669900304','npi_num|1548291594','npi_num|1942536354','npi_num|1306009618','npi_num|1295005049','npi_num|1922068162','npi_num|1245233204','npi_num|1437444973','npi_num|1023068855','npi_num|1184733065','npi_num|1568499473','npi_num|1952370520','npi_num|1881633576','npi_num|1316960925','npi_num|1144243791','npi_num|1053310821','npi_num|1033346648','npi_num|1659710283','npi_num|1265410690','npi_num|1184934960','npi_num|1093029738','npi_num|1134100167','npi_num|1497733281','npi_num|1528452893','npi_num|1235119637','npi_num|1023042462','npi_num|1578694162','npi_num|1730103177','npi_num|1720406705','npi_num|1093751711','npi_num|1275795221','npi_num|1013117332','npi_num|1609140383','npi_num|1386682888','npi_num|1487888103','npi_num|1255305546','npi_num|1790778561','npi_num|1003139593','npi_num|1326350679','npi_num|1265451678','npi_num|1730362906','npi_num|1669653812','npi_num|1922395300','npi_num|1427190883','npi_num|1275007825','npi_num|1063415610','npi_num|1134232580','npi_num|1013978741','npi_num|1245499144','npi_num|1588656219','npi_num|1679103634','npi_num|1184718751','npi_num|1114929585','npi_num|1063799351','npi_num|1053817080','npi_num|1922086768','npi_num|1477866101','npi_num|1316998545','npi_num|1821451121','npi_num|1508818709','npi_num|1811989817','npi_num|1184014318','npi_num|1649638297','npi_num|1154434108','npi_num|1568862944','npi_num|1982660643','npi_num|1184151755','npi_num|1477586501','npi_num|1760795496','npi_num|1518355023','npi_num|1306803770','npi_num|1932191558','npi_num|1699783985','npi_num|1841295318','npi_num|1366832990','npi_num|1124067350','npi_num|1962574483','npi_num|1891769162','npi_num|1114212552','npi_num|1275533390','npi_num|1346267911','npi_num|1568640597','npi_num|1407366164','npi_num|1043216377','npi_num|1982647400','npi_num|1790948891','npi_num|1811000300','npi_num|1518963917','npi_num|1891892352','npi_num|1245499052','npi_num|1285675256','npi_num|1306896493','npi_num|1740317429','npi_num|1073969499','npi_num|1861506446','npi_num|1093734410','npi_num|1518930007','npi_num|1801004106','npi_num|1811142508','npi_num|1477615946','npi_num|1215242995','npi_num|1902817323','npi_num|1447222765','npi_num|1598760340','npi_num|1255693370','npi_num|1336235183','npi_num|1609828342','npi_num|1851318307','npi_num|1295093466','npi_num|1891962999','npi_num|1083823504','npi_num|1427156017','npi_num|1730107897','npi_num|1700890860','npi_num|1801816681','npi_num|1871573410','npi_num|1689677486','npi_num|1184914046','npi_num|1275533689','npi_num|1326007576','npi_num|1720488208','npi_num|1023426954','npi_num|1588767180','npi_num|1609824044','npi_num|1720508492','npi_num|1922353069','npi_num|1144211392','npi_num|1124063532','npi_num|1619183381','npi_num|1356328108','npi_num|1457568354','npi_num|1285068825','npi_num|1255549713','npi_num|1548589948','npi_num|1437114873','npi_num|1437179116','npi_num|1386655512','npi_num|1194963868','npi_num|1083035307','npi_num|1730178484','npi_num|1639192941','npi_num|1619109758','npi_num|1831447515','npi_num|1528370251','npi_num|1225450950','npi_num|1679108559','npi_num|1770554966','npi_num|1619934502','npi_num|1295024057','npi_num|1487150959','npi_num|1467846691','npi_num|1184642357','npi_num|1790001485','npi_num|1679032809','npi_num|1235101999','npi_num|1235208810','npi_num|1225069818','npi_num|1164742029','npi_num|1083961445','npi_num|1346680519','npi_num|1710249263','npi_num|1750719837','npi_num|1437546074','npi_num|1265725352','npi_num|1245749720','npi_num|1588942643','npi_num|1982605192','npi_num|1639533235','npi_num|1306942933','npi_num|1780611145','npi_num|1619972163','npi_num|1841553716','npi_num|1992270748','npi_num|1750545661','npi_num|1700940350','npi_num|1477666378','npi_num|1376594630','npi_num|1811106669','npi_num|1134484942','npi_num|1770555864','npi_num|1134183247','npi_num|1255366282','npi_num|1427138940','npi_num|1386690337','npi_num|1447458153','npi_num|1124021431','npi_num|1871684944','npi_num|1255302246','npi_num|1962486357','npi_num|1215355821','npi_num|1699754895','npi_num|1588717177','npi_num|1558659250','npi_num|1215970595','npi_num|1821380551','npi_num|1265405591','npi_num|1659300721','npi_num|1376843086','npi_num|1174845390','npi_num|1386616449','npi_num|1891030961','npi_num|1447255260','npi_num|1134212129','npi_num|1508869579','npi_num|1023233145','npi_num|1972508539','npi_num|1275599193','npi_num|1144660309','npi_num|1750591939','npi_num|1285800854','npi_num|1639144835','npi_num|1306373832','npi_num|1003952326','npi_num|1144487745','npi_num|1457444283','npi_num|1073638433','npi_num|1053317438','npi_num|1336445642','npi_num|1013935220','npi_num|1730295171','npi_num|1629034905','npi_num|1134136567','npi_num|1962430314','npi_num|1558333716','npi_num|1437447059','npi_num|1740626803','npi_num|1275517849','npi_num|1306997861','npi_num|1851781728','npi_num|1578575940','npi_num|1568500361','npi_num|1912252032','npi_num|1023286929','npi_num|1457396475','npi_num|1265694475','npi_num|1689664088','npi_num|1992700959','npi_num|1003836768','npi_num|1821087222','npi_num|1720526148','npi_num|1154651917','npi_num|1285861823','npi_num|1043230733','npi_num|1003253691','npi_num|1629416235','npi_num|1033204805','npi_num|1609814813','npi_num|1154351294','npi_num|1811192156','npi_num|1710943063','npi_num|1962831628','npi_num|1912268343','npi_num|1023368552','npi_num|1326441452','npi_num|1316942832','npi_num|default_npi_391055','npi_num|1548230733','npi_num|1770514507','npi_num|1063755270','npi_num|1225043961','npi_num|1518315688','npi_num|1871955120','npi_num|1699935585','npi_num|1679704084','npi_num|1093922528','npi_num|1821387622','npi_num|1144743097','npi_num|1972525400','npi_num|1730136219','npi_num|1851687966','npi_num|1932146982','npi_num|1467641423','npi_num|1730672718','npi_num|1124125638','npi_num|1083684252','npi_num|1538591383','npi_num|1174934046','npi_num|1841296944','npi_num|1114919966','npi_num|1982798914','npi_num|1770743957','npi_num|1467491050','npi_num|1386737310','npi_num|1003818204','npi_num|1558399717','npi_num|1861547150','npi_num|1891791828','npi_num|1568756286','npi_num|1811969538','npi_num|1427118074','npi_num|1184647794','npi_num|1629583497','npi_num|1033342621','npi_num|1598980724','npi_num|1982606919','npi_num|1417998956','npi_num|1750375978','npi_num|1518923481','npi_num|1518493956','npi_num|1811960305','npi_num|1629382643','npi_num|1558324343','npi_num|1568576999','npi_num|1356323471','npi_num|1215911987','npi_num|1174654479','npi_num|1164449153','npi_num|1710178173','npi_num|1609899418','npi_num|1528084050','npi_num|1730491770','npi_num|1326257734','npi_num|1871955575','npi_num|1881809069','npi_num|1669828729','npi_num|1538320783','npi_num|1679029300','npi_num|1104867159','npi_num|1891790317','npi_num|1124049812','npi_num|1467476713','npi_num|1619328572','npi_num|1881613206','npi_num|1376922849','npi_num|1245420330','npi_num|1174885362','npi_num|1265641120','npi_num|1487657458','npi_num|1750644704','npi_num|1326343732','npi_num|1013974609','npi_num|1578803649','npi_num|1205306545','npi_num|1184670408','npi_num|1700104957','npi_num|1922007079','npi_num|1275830762','npi_num|1356696348','npi_num|1427334598','npi_num|1801138334','npi_num|1316943244','npi_num|1932398153','npi_num|1114003027','npi_num|1285653048','npi_num|1912297904','npi_num|1487910741','npi_num|1508214305','npi_num|1487130233','npi_num|1770878456','npi_num|1245373075','npi_num|1710919014','npi_num|1932140621','npi_num|1619961893','npi_num|1073510186','npi_num|1609894807','npi_num|1518964543','npi_num|1376698142','npi_num|1760877955','npi_num|1902806672','npi_num|1134149677','npi_num|1750541801','npi_num|1922067917','npi_num|1871536912','npi_num|1518337021','npi_num|1417040528','npi_num|1710132469','npi_num|1689862799','npi_num|1740422401','npi_num|1588822977','npi_num|default_npi_391084','npi_num|1649678012','npi_num|1922274794','npi_num|1215993571','npi_num|1063977973','npi_num|1770949901','npi_num|1417900655','npi_num|1568922557','npi_num|1013295831','npi_num|1629058631','npi_num|1245586940','npi_num|1245277128','npi_num|1518962075','npi_num|1407827702','npi_num|1194802629','npi_num|1063457596','npi_num|1699822536','npi_num|1659303741','npi_num|1013202803','npi_num|1063406239','npi_num|1659532935','npi_num|1467458901','npi_num|1285632950','npi_num|1710349584','npi_num|1871559153','npi_num|1407213036','npi_num|1497014849','npi_num|1205888377','npi_num|1427427673','npi_num|1932105368','npi_num|1073544433','npi_num|1659565224','npi_num|1366745200','npi_num|1225079965','npi_num|1932305075','npi_num|1922542927','npi_num|1801110721','npi_num|1871001669','npi_num|1831390681','npi_num|1275518433','npi_num|1821173170','npi_num|1982864997','npi_num|1619405776','npi_num|1710956974','npi_num|1447275698','npi_num|1215079330','npi_num|1679983951','npi_num|1306864988','npi_num|1578635041','npi_num|1952306904','npi_num|1962479576','npi_num|1376794057','npi_num|1902809866','npi_num|1538755673','npi_num|1003966482','npi_num|1760724819','npi_num|1194772137','npi_num|1942224209','npi_num|1265681605','npi_num|1669495289','npi_num|1700058195','npi_num|1437325768','npi_num|1639148091','npi_num|1982930996','npi_num|1770572422','npi_num|1740667641','npi_num|1932149929','npi_num|1972564763','npi_num|1992716419','npi_num|1205856143','npi_num|1245215508','npi_num|1336382027','npi_num|1053750034','npi_num|1992204226','npi_num|1053321059','npi_num|1710497094','npi_num|1083159248','npi_num|1760424220','npi_num|1730404591','npi_num|1790755015','npi_num|1174694442','npi_num|1811283153','npi_num|1275513814','npi_num|1770033714','npi_num|1346409513','npi_num|1760732606','npi_num|1144635079','npi_num|1194838060','npi_num|1396725966','npi_num|1760406961','npi_num|1053434399','npi_num|1134196116','npi_num|1366531998','npi_num|1629330162','npi_num|1205187580','npi_num|1609842160','npi_num|1033553839','npi_num|1821039090','npi_num|1770725392','npi_num|1538552989','npi_num|1962626671','npi_num|1285609404','npi_num|1861411274','npi_num|1790030427','npi_num|1073545380','npi_num|1952322356','npi_num|1932350287','npi_num|1144490004','npi_num|1124473921','npi_num|1447273370','npi_num|1912119710','npi_num|1215908645','npi_num|1285601039','npi_num|1962468074','npi_num|1548261936','npi_num|1417967191','npi_num|1023114139','npi_num|1477761682','npi_num|1316454051','npi_num|1366462434','npi_num|1861458150','npi_num|1497991079','npi_num|1770733008','npi_num|1912979923','npi_num|1861463036','npi_num|1588955025','npi_num|1023451259','npi_num|1588895767','npi_num|1194016154','npi_num|1821069519','npi_num|1073675609','npi_num|1578537171','npi_num|1518961408','npi_num|1316929078','npi_num|1922010545','npi_num|1073543195','npi_num|1508111303','npi_num|1891712618','npi_num|1861529331','npi_num|1497893523','npi_num|1013355411','npi_num|1316079874','npi_num|1508838863','npi_num|1851925119','npi_num|1164410718','npi_num|1013919570','npi_num|1255505962','npi_num|1003811670','npi_num|1871595181','npi_num|1962713404','npi_num|1336528892','npi_num|default_npi_391052','npi_num|1790761401','npi_num|1205800018','npi_num|1073517918','npi_num|1750369864','npi_num|1043212574','npi_num|1720080864','npi_num|1629509971','npi_num|1164633566','npi_num|1326147455','npi_num|1679579767','npi_num|1245207224','npi_num|1336226166','npi_num|1477546083','npi_num|1164682761','npi_num|1629096821','npi_num|1457378028','npi_num|1346861978','npi_num|1043423221','npi_num|1114313004','npi_num|1053363234','npi_num|1346401478','npi_num|1225217037','npi_num|1831367531','npi_num|1518438001','npi_num|1053640052','npi_num|1174545701','npi_num|1457353336','npi_num|1750724498','npi_num|1780611202','npi_num|1235349655','npi_num|1073774857','npi_num|1104893759','npi_num|1558537662','npi_num|1932339488','npi_num|1316154255','npi_num|1891842266','npi_num|1013381292','npi_num|1710361787','npi_num|1902238736','npi_num|1699860981','npi_num|1982623658','npi_num|1043250020','npi_num|1578799086','npi_num|1982724274','npi_num|1770581696','npi_num|1801203252','npi_num|1083142541','npi_num|1891754891','npi_num|1780990226','npi_num|1225084361','npi_num|1174962773','npi_num|1023085792','npi_num|1982856498','npi_num|1386684280','npi_num|1568632800','npi_num|1609255025','npi_num|1396103321','npi_num|1922202621','npi_num|1306882279','npi_num|1174500680','npi_num|1639518327','npi_num|1982757837','npi_num|1366796799','npi_num|1699735589','npi_num|1750600789','npi_num|1598786857','npi_num|1912393539','npi_num|1124253992','npi_num|1922157346','npi_num|1699989798','npi_num|1083632848','npi_num|1942361902','npi_num|1417284811','npi_num|1922254598','npi_num|1295758845','npi_num|1215368667','npi_num|1790914372','npi_num|1821432766','npi_num|1336394550','npi_num|1780617746','npi_num|1366807372','npi_num|1861856155','npi_num|1437362548','npi_num|1528222452','npi_num|1801840459','npi_num|1578857488','npi_num|1295702058','npi_num|1780680199','npi_num|1699770131','npi_num|1336190032','npi_num|1295963445','npi_num|1609163484','npi_num|1245248442','npi_num|1164493300','npi_num|1780798892','npi_num|1992814685','npi_num|1346246725','npi_num|1104827286','npi_num|1578568325','npi_num|1629040084','npi_num|1730124777','npi_num|1356382618','npi_num|1942537725','npi_num|1720053465','npi_num|1174572622','npi_num|1679868376','npi_num|1013981166','npi_num|1629093844','npi_num|1659792570','npi_num|1093089567','npi_num|1235161662','npi_num|1396937835','npi_num|1376825281','npi_num|1841755014','npi_num|1770621336','npi_num|1033552112','npi_num|1619917267','npi_num|1831313352','npi_num|1164481032','npi_num|1346778099','npi_num|1528078698','npi_num|1770648750','npi_num|1396164190','npi_num|1083155204','npi_num|1710920319','npi_num|1770603193','npi_num|1528482908','npi_num|1609188879','npi_num|1063753598','npi_num|1285630681','npi_num|1245214089','npi_num|1356399075','npi_num|1003203100','npi_num|1407851835','npi_num|1104828672','npi_num|1255336277','npi_num|1760524870','npi_num|1891879375','npi_num|1265852206','npi_num|1255333225','npi_num|1306008818','npi_num|1306007505','npi_num|1275739906','npi_num|1093735789','npi_num|1598781601','npi_num|1134142763','npi_num|1881670891','npi_num|1730146481','npi_num|1326298159','npi_num|1356502900','npi_num|1487854824','npi_num|1659303923','npi_num|1699066860','npi_num|1669443875','npi_num|1023273158','npi_num|1851397954','npi_num|default_npi_391081','npi_num|1821389248','npi_num|1275961294','npi_num|1801014055','npi_num|1558833541','npi_num|1306930243','npi_num|1720084247','npi_num|1144225905','npi_num|1962477174','npi_num|1346697398','npi_num|1316207293','npi_num|1891751046','npi_num|1174528129','npi_num|1083616882','npi_num|1033661368','npi_num|1720227002','npi_num|1841281995','npi_num|1023216850','npi_num|1972982494','npi_num|1346661329','npi_num|1063483469','npi_num|1023262128','npi_num|1225329063','npi_num|1073969929','npi_num|1144296252','npi_num|1649565367','npi_num|1447515317','npi_num|1194019372','npi_num|1104850031','npi_num|1457827024','npi_num|1093176505','npi_num|1346280633','npi_num|1942652094','npi_num|1215970694','npi_num|1558314732','npi_num|1013178961','npi_num|1023490448','npi_num|1417957838','npi_num|1316213903','npi_num|1598788689','npi_num|1730273772','npi_num|1720056336','npi_num|1831440767','npi_num|1619108834','npi_num|1376587543','npi_num|1215357843','npi_num|1891797759','npi_num|1235180472','npi_num|1073728663','npi_num|1114922945','npi_num|1750386272','npi_num|1932339017','npi_num|1447202395','npi_num|1417305921','npi_num|1821037599','npi_num|1467687061','npi_num|1275869752','npi_num|1265478275','npi_num|1275952764','npi_num|1770588030','npi_num|1275811440','npi_num|1720349160','npi_num|1477559698','npi_num|1740726017','npi_num|1619919313','npi_num|1508132291','npi_num|1871792606','npi_num|1982626321','npi_num|1427500339','npi_num|1861712127','npi_num|1467743344','npi_num|1861414351','npi_num|1659347896','npi_num|1427217694','npi_num|1689629438','npi_num|1154751964','npi_num|1811908841','npi_num|1730418609','npi_num|1720273915','npi_num|1467987743','npi_num|1124655220','npi_num|1275500712','npi_num|1356343388','npi_num|1689675050','npi_num|1942206347','npi_num|1194754770','npi_num|default_npi_391012','npi_num|1386935559','npi_num|1275538969','npi_num|1518987890','npi_num|1467472175','npi_num|1780682815','npi_num|1881742484','npi_num|1730394099','npi_num|1063797330','npi_num|1700832425','npi_num|1215907084','npi_num|1487627055','npi_num|1992874283','npi_num|1194836387','npi_num|1396280590','npi_num|1518226760','npi_num|1801932751','npi_num|1164794269','npi_num|1285209338','npi_num|1013118876','npi_num|1164720546','npi_num|1235183906','npi_num|1831199405','npi_num|1619976768','npi_num|1184036378','npi_num|1952315855','npi_num|1730169798','npi_num|1467498931','npi_num|1194712430','npi_num|1417319914','npi_num|1740209659','npi_num|1144242660','npi_num|1740632843','npi_num|1912340415','npi_num|1114487469','npi_num|1508128398','npi_num|1427074681','npi_num|1427020874','npi_num|1548315526','npi_num|1932361938','npi_num|1720127947','npi_num|1609886902','npi_num|1467551374','npi_num|1891967006','npi_num|1659346344','npi_num|1962405936','npi_num|1750518650','npi_num|1962798249','npi_num|1811315096','npi_num|1235149766','npi_num|1780619759','npi_num|1366742918','npi_num|1831626175','npi_num|1275828253','npi_num|1841203726','npi_num|1962726893','npi_num|1043258791','npi_num|1649630815','npi_num|1679536650','npi_num|1023057007','npi_num|1326056151','npi_num|1841267077','npi_num|1083791222','npi_num|1861802811','npi_num|1851319347','npi_num|1861472474','npi_num|1063481398','npi_num|1487999595','npi_num|1811003452','npi_num|1205861499','npi_num|1205149853','npi_num|1255655205','npi_num|1811917933','npi_num|1881918340','npi_num|1245729854','npi_num|1003077074','npi_num|1891797742','npi_num|1295871598','npi_num|1851683023','npi_num|1699271676','npi_num|1093731184','npi_num|1710971023','npi_num|1750304465','npi_num|1700049905','npi_num|1528088234','npi_num|1073533394','npi_num|1174550461','npi_num|1588067748','npi_num|1740279843','npi_num|1629038583','npi_num|1114915147','npi_num|1386907434','npi_num|1851806442','npi_num|1487677084','npi_num|1811316854','npi_num|1285961417','npi_num|1083906473','npi_num|1346262607','npi_num|1336331313','npi_num|1023138237','npi_num|1750487674','npi_num|1427116748','npi_num|1134477748','npi_num|1114311347','npi_num|1740297464','npi_num|1205201068','npi_num|1992241319','npi_num|1730320417','npi_num|1366400988','npi_num|1922322197','npi_num|1821553470','npi_num|1376551564','npi_num|1669809828','npi_num|1467452581','npi_num|1003191883','npi_num|1164956173','npi_num|1477764678','npi_num|1083078539','npi_num|1033572318','npi_num|1346912870','npi_num|1548350879','npi_num|1780643775','npi_num|1174758155','npi_num|1548253362','npi_num|1669413209','npi_num|1316000805','npi_num|1508944570','npi_num|1689667487','npi_num|1063791416','npi_num|1700051554','npi_num|1689602229','npi_num|1154826337','npi_num|1518983030','npi_num|1518316470','npi_num|1255357877','npi_num|1700876133','npi_num|1255502902','npi_num|1336399963','npi_num|1245314343','npi_num|1134281140','npi_num|1811259443','npi_num|1306829783','npi_num|1669449195','npi_num|1659667244','npi_num|1528296514','npi_num|1174843643','npi_num|1407825854','npi_num|1720068588','npi_num|1821215567','npi_num|1629071048','npi_num|1740286228','npi_num|1073032603','npi_num|1245258482','npi_num|1790739621','npi_num|1225007099','npi_num|1477816528','npi_num|1851358741','npi_num|1649367681','npi_num|1083875637','npi_num|1528436748','npi_num|1689804031','npi_num|1821078643','npi_num|1962431114','npi_num|1215978549','npi_num|1104821768','npi_num|1538125992','npi_num|1770567901','npi_num|1821217837','npi_num|1801867726','npi_num|1346426749','npi_num|1013028729','npi_num|1740540251','npi_num|1437578655','npi_num|1598146268','npi_num|1477008720','npi_num|1154648350','npi_num|1669819132','npi_num|1568433936','npi_num|1548283344','npi_num|1699949040','npi_num|1295845030','npi_num|1154863389','npi_num|1376569418','npi_num|1154358117','npi_num|1629132006','npi_num|1821057720','npi_num|1811361561','npi_num|1114476207','npi_num|1336301571','npi_num|1306818497','npi_num|1902904592','npi_num|1649666223','npi_num|1083619811','npi_num|1780842666','npi_num|1942280300','npi_num|1235102542','npi_num|1972593812','npi_num|1679970909','npi_num|1851577571','npi_num|1679554109','npi_num|1104843770','npi_num|1083858054','npi_num|1588642292','npi_num|1528030327','npi_num|1518174994','npi_num|1528435534','npi_num|1760684633','npi_num|1659502441','npi_num|1528229770','npi_num|1417908898','npi_num|1316448269','npi_num|1467999243','npi_num|1205907383','npi_num|1437394210','npi_num|1194956797','npi_num|1285618421','npi_num|1699857508','#NA','npi_num|1568493195','npi_num|1992130207','npi_num|1720242456','npi_num|1093943896','npi_num|1942211198','npi_num|1891741625','npi_num|1841538436','npi_num|1407215569','npi_num|1225054398','npi_num|1033436662','npi_num|1760829618','npi_num|1942220629','npi_num|1649226184','npi_num|1821223017','npi_num|1659777472','npi_num|1558565093','npi_num|1881944007','npi_num|1871737759','npi_num|1194987073','npi_num|1710193982','npi_num|1447220769','npi_num|1487870259','npi_num|1235544776','npi_num|1518050525','npi_num|1275738924','npi_num|1033131099','npi_num|1093710519','npi_num|1255518411','npi_num|1033132261','npi_num|1619973674','npi_num|1194764696','npi_num|1619256492','npi_num|1891745287','npi_num|1437219987','npi_num|1073507224','npi_num|1760456297','npi_num|1881851004','npi_num|1124439062','npi_num|1811968290','npi_num|1356856421','npi_num|1962416651','npi_num|1639163751','npi_num|1760680318','npi_num|1528220829','npi_num|1992729370','npi_num|1821409764','npi_num|1811106008','npi_num|1942526405','npi_num|1992093587','npi_num|1295821692','npi_num|1487819769','npi_num|1497856561','npi_num|1770774598','npi_num|1487802492','npi_num|1801817051','npi_num|1053612291','npi_num|1205885332','npi_num|1821288408','npi_num|1841213048','npi_num|1568721223','npi_num|1134117336','npi_num|1528564820','npi_num|1649676370','npi_num|1376533596','npi_num|1104858091','npi_num|1528179553','npi_num|1376545236','npi_num|1922036235','npi_num|1548471071','npi_num|1902194186','npi_num|1770746604','npi_num|1194301614','npi_num|1730181595','npi_num|1275824260','npi_num|1396971370','npi_num|1003137332','npi_num|1497103881','npi_num|1275762353','npi_num|1205887494','npi_num|1093735847','npi_num|1760408371','npi_num|1881867018','npi_num|1144663717','npi_num|1184966574','npi_num|1649250416','npi_num|1235588989','npi_num|1972869402','npi_num|1730531336','npi_num|1003075946','npi_num|1356983860','npi_num|1376523977','npi_num|1225049307','npi_num|1245499540','npi_num|1013987544','npi_num|1013171420','npi_num|1073712238','npi_num|1265022024','npi_num|1467508101','npi_num|1346234838','npi_num|1033575287','npi_num|1386613693','npi_num|1285942524','npi_num|1225190341','npi_num|1881763613','npi_num|1790984003','npi_num|1124093877','npi_num|1760462063','npi_num|1710967005','npi_num|1477832392','npi_num|1326676008','npi_num|1619972924','npi_num|1700175049','npi_num|1023486644','npi_num|1356397574','npi_num|1811160823','npi_num|1194136077','npi_num|1790768745','npi_num|1710167929','npi_num|1700310794','npi_num|1780056796','npi_num|1134329022','npi_num|1912240581','npi_num|1437163714','npi_num|1326159443','npi_num|1598722472','npi_num|1447639075','npi_num|1427094697','npi_num|1972504066','npi_num|1396748224','npi_num|1992749469','npi_num|1417900945','npi_num|1538187398','npi_num|1518938646','npi_num|1497860043','npi_num|1831231299','npi_num|1114123981','npi_num|1205912417','npi_num|1679894372','npi_num|1396139697','npi_num|1013086917','npi_num|1881611176','npi_num|1083610471','npi_num|1073519187','npi_num|1669676797','npi_num|1184822504','npi_num|1932216538','npi_num|1033291844','npi_num|1467434571','npi_num|1750429080','npi_num|1215224183','npi_num|1013060466','npi_num|1497980601','npi_num|1952407108','npi_num|1932524147','npi_num|1043753973','npi_num|1760631477','npi_num|1528111069','npi_num|1134370018','npi_num|1740209493','npi_num|1871618215','npi_num|1114343654','npi_num|1376788299','npi_num|1942316989','npi_num|1922334093','npi_num|1538591912','npi_num|1932394095','npi_num|1104258276','npi_num|1730154774','npi_num|1861937781','npi_num|1194941195','npi_num|1710956156','npi_num|1720235450','npi_num|1841292398','npi_num|1790710788','npi_num|1336669134','npi_num|1275766453','npi_num|1689060477','npi_num|1073028254','npi_num|1760443022','npi_num|1588696959','npi_num|1154397974','npi_num|1083666754','npi_num|1629091228','npi_num|1043448624','npi_num|1417047275','npi_num|1639141815','npi_num|1184646523','npi_num|1972742906','npi_num|1134122286','npi_num|1437386141','npi_num|1376575878','npi_num|1104189174','npi_num|1205893773','npi_num|1225295645','npi_num|1376531517','npi_num|1558781567','npi_num|1306200969','npi_num|1780677500','npi_num|1811031222','npi_num|1154362614','npi_num|1124092739','npi_num|1811219520','npi_num|1992071930','npi_num|1013933217','npi_num|1568495760','npi_num|1760980007','npi_num|1043590367','npi_num|1326066523','npi_num|1104865294','npi_num|1720026537','npi_num|1295927945','npi_num|1598076358','npi_num|1013376466','npi_num|1407051741','npi_num|1376982223','npi_num|1215369640','npi_num|1447224076','npi_num|1629099981','npi_num|1942460670')  
 and pxm.attribution_type = 'as_was' 
 ) a 
--WHERE mbi = 'cms_mssp|5NA8WC5TY10'

--'cms_mssp|5AD2FP5AR24'
 --order by 1,2,3,4,5 

minus
 
SELECT MBI 
  , "Last Name"
  ,"First Name"
  ,"Date of Birth"
  , month_cd
  , "Rolling 12 End Month"
  , "Attribution Type"
    , NULLIF("Attributed Provider NPI",'') "Attributed Provider NPI"
    , NULLIF("Attributed Provider Name",'') "Attributed Provider Name"
    , "Visit Type"
    , "Primary Payer"
    , NULLIF("AWV Provider NPI",'') "AWV Provider NPI"
    , NULLIF("AWV Provider Name",'') "AWV Provider Name"
    , "Activity Date"
    , "Activity Month"
    --, "Notes"  because of upload these were blank not NULL
FROM local_michaeloconnor.public.ANNUAL_WELLNESS_VISITS_PI 
--WHERE mbi = 'cms_mssp|5NA8WC5TY10'
--'cms_mssp|5AD2FP5AR24'



SELECT provider_npi
  , YEAR 
  , alignment_period
  ,PROVIDER_TYPE
  ,PROVIDER_NAME
  ,PRIMARY_SPECIALTY_FLAG
  ,ORGANIZATION_NPI
  ,ORGANIZATION_NAME
  ,ORG_FQHC_FLAG
  ,ORG_RHC_FLAG
  ,ORG_COUNTY_CD
  ,ORG_COUNTY_NAME
  ,BENE_STATE
  ,BENE_COUNTY_CD
  ,BENE_COUNTY_NAME
  ,BENE_CBSA_CD
  ,BENE_CBSA_NAME 
  ,BENE_COHORT
  ,BENE_CNT_ALIGNED
  ,BENE_AVG_ADJ_HCC
  ,BENE_CNT_STRONG_ALIGNMENT
  ,BENE_CNT_LOOSE_ALIGNMENT
  ,BENE_CNT_ALIGN_ALIVE
  ,BENE_CNT_HIGH_NEEDS_ELIGIBLE 
  ,BENE_CNT_HLTHY_SIMPLECC_NULL
  ,BENE_CNT_FRAIL_ELDERLY
  ,BENE_CNT_MAJ_MIN_COMPL_CC
  ,BENE_CNT_UNDER65_DIS_ESRD
  ,BENE_CNT_ALIGNED_ELIGIBLE
  ,BENE_TOTAL_MEMBER_MONTHS
  ,PQEM_ALLOWED
  ,PQEM_SPEND
  ,PQEM_SPEND_BY_ALIGN_PROV
  ,PQEM_PARTB_SPEND
  ,PQEM_OP_FQHC_SPEND
  ,PQEM_OP_RHC_SPEND  
  ,PQEM_OP_CAH_SPEND
  ,PARTB_SPEND
  ,INPATIENT_SPEND  
  ,OUTPATIENT_SPEND
  ,HHA_SPEND
  ,SNF_SPEND
  ,HOSPICE_SPEND
  ,DME_SPEND
  ,TOTAL_SPEND
  ,GROUP_LEVEL_1_ID
  ,GROUP_LEVEL_1_NAME
  ,GROUP_LEVEL_2_ID
  ,GROUP_LEVEL_2_NAME
  ,GROUP_LEVEL_3_ID 
  ,GROUP_LEVEL_3_NAME
  ,NETWORK_FLAG
  ,NETWORK_1_ID
  ,NETWORK_1_NAME 
  , count(*) AS rwCnt
FROM local_michaeloconnor.public.PARTICIPANT_LIST_NA_2

GROUP BY provider_npi
  , YEAR 
  , alignment_period
  ,PROVIDER_TYPE
  ,PROVIDER_NAME
  ,PRIMARY_SPECIALTY_FLAG
  ,ORGANIZATION_NPI
  ,ORGANIZATION_NAME
  ,ORG_FQHC_FLAG
  ,ORG_RHC_FLAG
  ,ORG_COUNTY_CD
  ,ORG_COUNTY_NAME
  ,BENE_STATE
  ,BENE_COUNTY_CD
  ,BENE_COUNTY_NAME
  ,BENE_CBSA_CD
  ,BENE_CBSA_NAME 
  ,BENE_COHORT
  ,BENE_CNT_ALIGNED
  ,BENE_AVG_ADJ_HCC
  ,BENE_CNT_STRONG_ALIGNMENT
  ,BENE_CNT_LOOSE_ALIGNMENT
  ,BENE_CNT_ALIGN_ALIVE
  ,BENE_CNT_HIGH_NEEDS_ELIGIBLE 
  ,BENE_CNT_HLTHY_SIMPLECC_NULL
  ,BENE_CNT_FRAIL_ELDERLY
  ,BENE_CNT_MAJ_MIN_COMPL_CC
  ,BENE_CNT_UNDER65_DIS_ESRD
  ,BENE_CNT_ALIGNED_ELIGIBLE
  ,BENE_TOTAL_MEMBER_MONTHS
  ,PQEM_ALLOWED
  ,PQEM_SPEND
  ,PQEM_SPEND_BY_ALIGN_PROV
  ,PQEM_PARTB_SPEND
  ,PQEM_OP_FQHC_SPEND
  ,PQEM_OP_RHC_SPEND  
  ,PQEM_OP_CAH_SPEND
  ,PARTB_SPEND
  ,INPATIENT_SPEND  
  ,OUTPATIENT_SPEND
  ,HHA_SPEND
  ,SNF_SPEND
  ,HOSPICE_SPEND
  ,DME_SPEND
  ,TOTAL_SPEND
  ,GROUP_LEVEL_1_ID
  ,GROUP_LEVEL_1_NAME
  ,GROUP_LEVEL_2_ID
  ,GROUP_LEVEL_2_NAME
  ,GROUP_LEVEL_3_ID 
  ,GROUP_LEVEL_3_NAME
  ,NETWORK_FLAG
  ,NETWORK_1_ID
  ,NETWORK_1_NAME
HAVING count(*) > 1 
ORDER BY provider_npi
  , YEAR 
  , alignment_period



  WITH dupCTE
    AS
    (
    SELECT 'PATIENT_X_PCP_TIN_PROVIDER_MONTH' AS tableName
        , sum(rowsDuped) AS rowsEffected
        , count(*) AS pkRowCount
    FROM
        (
        SELECT ATTR_STEP
      , FK_PATIENT_ID
      , FK_PCP_FACILITY_ID
      , FK_PCP_PROVIDER_ID
      , FK_PCP_TIN_ID
      , MONTH_CD
      , SOURCE_CD
            , count(*) AS rowsDuped 
        FROM PROD_A3632.INSIGHTS.PATIENT_X_PCP_TIN_PROVIDER_MONTH
        --WHERE MONTH_CD <> 'm-2022-06' --no dups with this uncommented
        GROUP BY ATTR_STEP
      , FK_PATIENT_ID
      , FK_PCP_FACILITY_ID
      , FK_PCP_PROVIDER_ID
      , FK_PCP_TIN_ID
      , MONTH_CD
      , SOURCE_CD
        HAVING count(*) > 1
        ) a 
    ) 
    SELECT tableName 
        , 'PROD_A3632' AS orgDBName
        , rowsEffected
        , pkRowCount
    FROM dupCTE



SELECT EFF_START_DT
      ,LOAD_RUN_ID
      ,PK_BENE_ID
      ,SRC_BENE_RNG_BGN_DT
      , count(*) AS rowsDuped 
FROM ODS.cclf_8_bene_demo 
WHERE record_status_cd = 'a'
GROUP BY EFF_START_DT
      ,LOAD_RUN_ID
      ,PK_BENE_ID
      ,SRC_BENE_RNG_BGN_DT
HAVING count(*) > 1


SELECT PK_PATIENT_ID
  , count(*) AS rowsDuped 
FROM insights.patient
GROUP BY PK_PATIENT_ID
HAVING count(*) > 1


SELECT BENE_MBI_ID 
  , BENE_RNG_BGN_DT
  , count(*) AS rwCt
FROM prod_elationdce.stg.ssf_cclf_8_v26
WHERE DAG_RUN_ID = 'ELATIONDCE_m-2022-06_2022-07-19T17:22:26.178963_47425' ----4,288 cases
          --'ELATIONDCE_m-2022-05_2022-06-15T20:28:24.163253_45509' --no dups
GROUP BY BENE_MBI_ID 
  , BENE_RNG_BGN_DT
HAVING count(*) > 1


USE prod_elationdce --4,288 cases
USE prod_adaugeopi -- no records
USE prod_a1052 --no records
USE prod_a2024 --no records
USE prod_canodce --no records

SELECT BENE_MBI_ID 
  , BENE_RNG_BGN_DT
  , count(*) AS rwCt
FROM stg.ssf_cclf_8_v26
WHERE DAG_RUN_ID = 
(
    SELECT max(DAG_RUN_ID)
    FROM stg.ssf_cclf_8_v26
)
GROUP BY BENE_MBI_ID 
  , BENE_RNG_BGN_DT
HAVING count(*) > 1