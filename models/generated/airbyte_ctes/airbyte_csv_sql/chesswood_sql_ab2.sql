{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_airbyte_csv_sql",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('chesswood_sql_ab1') }}
select
    cast(lease_num as {{ dbt_utils.type_string() }}) as lease_num,
    cast(lease_term as {{ dbt_utils.type_string() }}) as lease_term,
    cast(lease_type as {{ dbt_utils.type_string() }}) as lease_type,
    cast(lease_yield as {{ dbt_utils.type_string() }}) as lease_yield,
    cast(lease_app_num as {{ dbt_utils.type_string() }}) as lease_app_num,
    cast(lease_comp_num as {{ dbt_utils.type_string() }}) as lease_comp_num,
    cast(lease_pmts_rem as {{ dbt_utils.type_string() }}) as lease_pmts_rem,
    cast(lease_loan_rate as {{ dbt_utils.type_string() }}) as lease_loan_rate,
    cast(lease_sign_date as {{ dbt_utils.type_string() }}) as lease_sign_date,
    cast(lease_term_date as {{ dbt_utils.type_string() }}) as lease_term_date,
    cast(lease_book_value as {{ dbt_utils.type_string() }}) as lease_book_value,
    cast(lease_booked_idc as {{ dbt_utils.type_string() }}) as lease_booked_idc,
    cast(lease_equip_cost as {{ dbt_utils.type_string() }}) as lease_equip_cost,
    cast(lease_equip_type as {{ dbt_utils.type_string() }}) as lease_equip_type,
    cast(lease_pmts_final as {{ dbt_utils.type_string() }}) as lease_pmts_final,
    cast(lease_pmts_invcd as {{ dbt_utils.type_string() }}) as lease_pmts_invcd,
    cast(lease_start_date as {{ dbt_utils.type_string() }}) as lease_start_date,
    cast(lease_total_pmts as {{ dbt_utils.type_string() }}) as lease_total_pmts,
    cast(lease_vendor_num as {{ dbt_utils.type_string() }}) as lease_vendor_num,
    cast(lease_billto_city as {{ dbt_utils.type_string() }}) as lease_billto_city,
    cast(lease_cust_id_num as {{ dbt_utils.type_string() }}) as lease_cust_id_num,
    cast(lease_delinq_code as {{ dbt_utils.type_string() }}) as lease_delinq_code,
    cast(lease_implicit_rate as {{ dbt_utils.type_string() }}) as lease_implicit_rate,
    cast(lease_maturity_date as {{ dbt_utils.type_string() }}) as lease_maturity_date,
    cast(lease_pay_frequency as {{ dbt_utils.type_string() }}) as lease_pay_frequency,
    cast(lease_pmts_received as {{ dbt_utils.type_string() }}) as lease_pmts_received,
    cast(lease_purch_opt_amt as {{ dbt_utils.type_string() }}) as lease_purch_opt_amt,
    cast(lease_acct_dist_code as {{ dbt_utils.type_string() }}) as lease_acct_dist_code,
    cast(lease_booked_eq_cost as {{ dbt_utils.type_string() }}) as lease_booked_eq_cost,
    cast(lease_pmts_rem_invcd as {{ dbt_utils.type_string() }}) as lease_pmts_rem_invcd,
    cast(lease_rent_tran_code as {{ dbt_utils.type_string() }}) as lease_rent_tran_code,
    cast(lease_booked_residual as {{ dbt_utils.type_string() }}) as lease_booked_residual,
    cast(lease_county_tax_code as {{ dbt_utils.type_string() }}) as lease_county_tax_code,
    cast(lease_oldest_rent_due as {{ dbt_utils.type_string() }}) as lease_oldest_rent_due,
    cast(lease_booked_post_date as {{ dbt_utils.type_string() }}) as lease_booked_post_date,
    cast(lease_interim_rent_amt as {{ dbt_utils.type_string() }}) as lease_interim_rent_amt,
    cast(lease_booked_receivable as {{ dbt_utils.type_string() }}) as lease_booked_receivable,
    cast(lease_contract_mgr_code as {{ dbt_utils.type_string() }}) as lease_contract_mgr_code,
    cast(lease_next_inv_due_date as {{ dbt_utils.type_string() }}) as lease_next_inv_due_date,
    cast(lease_total_monthly_pmt as {{ dbt_utils.type_string() }}) as lease_total_monthly_pmt,
    cast(contract_finance_type_id as {{ dbt_utils.type_string() }}) as contract_finance_type_id,
    cast(lease_earn_to_date_lease as {{ dbt_utils.type_string() }}) as lease_earn_to_date_lease,
    cast(lease_extended_matur_date as {{ dbt_utils.type_string() }}) as lease_extended_matur_date,
    cast(customer_business_indivdual_indicator as {{ dbt_utils.type_string() }}) as customer_business_indivdual_indicator,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('chesswood_sql_ab1') }}
-- chesswood_sql
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

