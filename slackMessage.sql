OPS-1779_DCE_PI_duplicative_rows_patient_roster  (dev)


OPS-1779_DCE_PI_duplicative_rows_patient_roster_tst (test)

OPS-1779_DCE_PI_duplicative_rows_patient_roster_mstr (mstr)


----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
-- Author:         Rachel Plummer
-- Date:	       June 24, 2021
-- Description:    Outputs insights.patient_roster, a table that loads in the most recent patient_roster
--                 based on the monthly alignment reports (DCE_ALIGN), that accounts for patient fall-off
--                 month over month.
--
--                 The patient_roster is updated monthly as we receive DC member's most recent
--                 list of assignable beneficiaries. Each new monthly report is appended to historicals,
--                 with the 'load_period' field signifying the month in which the roster applies.
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------

INSERT OVERWRITE INTO INSIGHTS.PATIENT_ROSTER (
      ORG_ID
    , FK_PATIENT_ID
    , MONTH_CD
    , FK_NETWORK_ID
    , FK_GROUP_ID
    , FK_PROVIDER_ID
    , ATTRIBUTION_CURR_PERIOD_FLAG
    , ATTRIBUTION_PREV_PERIOD_FLAG
    , ATTRIBUTION_ANY_PERIOD_FLAG
    , ASSIGNABLE_CURR_PERIOD_FLAG
    , ASSIGNABLE_PREV_PERIOD_FLAG
    , ASSIGNABLE_ANY_PERIOD_FLAG
    , VOLUNTARY_ALIGN_IND
    , LOAD_PERIOD
    , LOAD_RUN_ID
    , LOAD_TS
 )
WITH va AS (
    SELECT 
          org_id 
        , LOAD_PERIOD AS month_cd
        , SRC_BENE_MBI_ID AS fk_patient_id
        , SRC_PRVDR_TIN AS fk_pcp_tin_id
        , SRC_PRVDR_NPI AS fk_provider_id
        , '#NA' AS fk_network_id
        , row_number() over (partition by src_bene_mbi_id order by eff_start_dt desc) as row_n
    FROM ods.dce_palmr
    WHERE SRC_ALGN_TYPE_VA LIKE '%VA%'
        AND record_status_cd = 'a'
        AND effective_flag
)
, cba AS (
  SELECT 
        org_id 
      , LOAD_PERIOD AS month_cd
      , SRC_BENE_MBI_ID AS fk_patient_id
      , SRC_PRVDR_TIN AS fk_pcp_tin_id
      , SRC_PRVDR_NPI AS fk_provider_id
      , '#NA' AS fk_network_id
      , (((1/3) * SRC_QEM_ALLOWED_PRIMARY_AY1) + ((2/3) * SRC_QEM_ALLOWED_PRIMARY_AY2)) AS weighted_allowed
      , row_number() over (partition by SRC_BENE_MBI_ID order by (((1/3) * SRC_QEM_ALLOWED_PRIMARY_AY1) + ((2/3) * SRC_QEM_ALLOWED_PRIMARY_AY2)) desc) AS row_n
  FROM ods.dce_palmr
  WHERE SRC_ALGN_TYPE_CLM = 'Y'
      AND SRC_BENE_MBI_ID not in (SELECT distinct fk_patient_id FROM va)
      AND record_status_cd = 'a'
      AND effective_flag
)
, final_attr as (
  SELECT 
        org_id
      , month_cd
      , fk_patient_id
      , fk_provider_id
      , fk_pcp_tin_id
      , fk_network_id
      , 'Y' as voluntary_align_ind
  FROM va
  WHERE row_n = 1
  UNION ALL
  SELECT 
        org_id
      , month_cd
      , fk_patient_id
      , fk_provider_id
      , fk_pcp_tin_id
      , fk_network_id
      , 'N' as voluntary_align_ind
  FROM cba
  WHERE row_n = 1
)
, bene_x_period as (
  select 
    SPLIT_PART(align.PK_DCE_ALIGN_ID, '|', 1)||'|'||align.SRC_BENE_MBI_ID AS fk_patient_id 
    , TO_CHAR(min(SRC_BENE_EFCTV_ST_DT), 'm-YYYY-MM') as start_dt
    , coalesce(TO_CHAR(max(
      case 
        when TO_CHAR(SRC_BENE_EFCTV_ST_DT, 'm-YYYY-MM') = LOAD_PERIOD 
          then null 
        else SRC_BENE_EFCTV_TERM_DT 
      end), 'm-YYYY-MM'), to_char(SRC_BENE_DEATH_DT,'m-YYYY-MM'), max_lp.max_load_period) as end_dt
  from ODS.DCE_ALIGN align
  join (select max(load_period) as max_load_period
       from ods.dce_align) max_lp
  WHERE align.LOAD_PERIOD = max_lp.max_load_period
  group by 
    SPLIT_PART(align.PK_DCE_ALIGN_ID, '|', 1)||'|'||align.SRC_BENE_MBI_ID, 
    SRC_BENE_DEATH_DT, 
   max_lp.max_load_period
)
SELECT DISTINCT
      '{{ dag_run.conf.org_id }}' AS org_id
    , bxp.fk_patient_id AS fk_patient_id
    , m.value AS month_cd
    , attr.fk_network_id AS fk_network_id
    , 'tin|'||attr.fk_pcp_tin_id as fk_group_id
    , 'npi_num|'||attr.fk_provider_id as fk_provider_id
    , TRUE as attribution_curr_period_flag
    , FALSE as attribution_prev_period_flag 
    , TRUE as attribution_any_period_flag
    , FALSE as assignable_curr_period_flag
    , FALSE as assignable_prev_period_flag
    , FALSE as assignable_any_period_flag
    , attr.voluntary_align_ind
    , '{{dag_run.conf.load_period}}' AS load_period
    , {{ti.job_id}} AS load_run_id
    , CURRENT_TIMESTAMP AS load_ts
FROM bene_x_period bxp
LEFT JOIN {{env}}_common.ref.code_month m
  on bxp.start_dt <= m.value
  and bxp.end_dt >= m.value
LEFT JOIN final_attr attr
  ON SPLIT_PART(bxp.fk_patient_id, '|', 2) = attr.fk_patient_id
  AND substr(m.value, 3, 4) = substr(attr.MONTH_CD, 3, 4) 
;


SELECT PK_BENE_ID        --6 members w/ dup active & effective records for ilumedpi
	, count(*) recCnt
FROM prod_ilumedpi.ods.cclf_8_bene_demo
WHERE RECORD_STATUS_CD = 'a'
AND EFFECTIVE_FLAG 
GROUP BY PK_BENE_ID 
HAVING count(*) > 1    

SELECT PK_BENE_ID        --1 member w/ dup active & effective record for adaugeopi
	, count(*) recCnt
FROM PROD_ADAUGEOPI.ods.cclf_8_bene_demo
WHERE RECORD_STATUS_CD = 'a'
AND EFFECTIVE_FLAG 
GROUP BY PK_BENE_ID 
HAVING count(*) > 1  