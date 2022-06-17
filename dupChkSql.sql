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
