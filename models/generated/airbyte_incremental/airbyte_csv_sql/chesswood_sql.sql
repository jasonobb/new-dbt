{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "airbyte_csv_sql",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('chesswood_sql_ab3') }}
select
    lease_num,
    lease_term,
    lease_type,
    lease_yield,
    lease_app_num,
    lease_comp_num,
    lease_pmts_rem,
    lease_loan_rate,
    lease_sign_date,
    lease_term_date,
    lease_book_value,
    lease_booked_idc,
    lease_equip_cost,
    lease_equip_type,
    lease_pmts_final,
    lease_pmts_invcd,
    lease_start_date,
    lease_total_pmts,
    lease_vendor_num,
    lease_billto_city,
    lease_cust_id_num,
    lease_delinq_code,
    lease_implicit_rate,
    lease_maturity_date,
    lease_pay_frequency,
    lease_pmts_received,
    lease_purch_opt_amt,
    lease_acct_dist_code,
    lease_booked_eq_cost,
    lease_pmts_rem_invcd,
    lease_rent_tran_code,
    lease_booked_residual,
    lease_county_tax_code,
    lease_oldest_rent_due,
    lease_booked_post_date,
    lease_interim_rent_amt,
    lease_booked_receivable,
    lease_contract_mgr_code,
    lease_next_inv_due_date,
    lease_total_monthly_pmt,
    contract_finance_type_id,
    lease_earn_to_date_lease,
    lease_extended_matur_date,
    customer_business_indivdual_indicator,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_chesswood_sql_hashid
from {{ ref('chesswood_sql_ab3') }}
-- chesswood_sql from {{ source('airbyte_csv_sql', '_airbyte_raw_chesswood_sql') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

