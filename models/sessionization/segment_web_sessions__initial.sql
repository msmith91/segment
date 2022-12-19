{{ config(
    materialized = 'incremental',
    unique_key = 'session_id',
    sort = 'session_start_tstamp',
    partition_by = {'field': 'session_start_tstamp', 'data_type': 'timestamp', 'granularity': var('segment_bigquery_partition_granularity')},
    dist = 'session_id',
    cluster_by = 'session_id'
    )}}

{% set partition_by = "partition by session_id" %}

{% set window_clause = "
    partition by session_id
    order by page_view_number
    rows between unbounded preceding and unbounded following
    " %}

{% set first_values1 = {
    'utm_source' : 'utm_source',
    'utm_content' : 'utm_content',
    } %}
{% set first_values2 = {
    'utm_medium' : 'utm_medium',
    'utm_campaign' : 'utm_campaign'
    } %}
{% set first_values3 = {
    'utm_term' : 'utm_term',
    'gclid' : 'gclid',
    'page_url' : 'first_page_url'
    } %}
{% set first_values4 = {
    'page_url_host' : 'first_page_url_host',
    'page_url_path' : 'first_page_url_path',
    'page_url_query' : 'first_page_url_query'
    } %}
{% set first_values5 = {
    'referrer' : 'referrer',
    'referrer_host' : 'referrer_host'
    } %}

{% set first_values6 = {
    'device' : 'device',
    'device_category' : 'device_category'
    } %}

{% set last_values1 = {
        'page_url' : 'last_page_url',
        'page_url_host' : 'last_page_url_host' 
    } %}

 {% set last_values2 = {
        'page_url_path' : 'last_page_url_path',
        'page_url_query' : 'last_page_url_query'
    } %}

{% for col in var('segment_pass_through_columns') %}
    {% do first_values1.update({col: 'first_' ~ col}) %}
    {% do first_values2.update({col: 'first_' ~ col}) %}
    {% do first_values3.update({col: 'first_' ~ col}) %}
    {% do first_values4.update({col: 'first_' ~ col}) %}
    {% do first_values5.update({col: 'first_' ~ col}) %}
    {% do first_values6.update({col: 'first_' ~ col}) %}
    {% do last_values1.update({col: 'last_' ~ col}) %}
    {% do last_values2.update({col: 'last_' ~ col}) %}
{% endfor %}


with pageviews_sessionized as (

    select * from {{ref('segment_web_page_views__sessionized')}}

    {% if is_incremental() %}
    {{
        generate_sessionization_incremental_filter( this, 'tstamp', 'session_start_tstamp', '>' )
    }}
    {% endif %}

),

referrer_mapping as (

    select * from {{ ref('referrer_mapping') }}

),

agg_first as (

    select distinct

        session_id,
        anonymous_id,
        min(tstamp) over ( {{partition_by}} ) as session_start_tstamp,
        max(tstamp) over ( {{partition_by}} ) as session_end_tstamp,
        count(*) over ( {{partition_by}} ) as page_views

    from pageviews_sessionized

),
agg_first1 as (

    select distinct

        session_id,
        anonymous_id,

        {% for (key, value) in first_values1.items() %}
        first_value({{key}}) over ({{window_clause}}) as {{value}},
        {% endfor %}

    from pageviews_sessionized

),
agg_first2 as (

    select distinct

        session_id,
        anonymous_id,

        {% for (key, value) in first_values2.items() %}
        first_value({{key}}) over ({{window_clause}}) as {{value}},
        {% endfor %}

    from pageviews_sessionized

),
agg_first3 as (

    select distinct

        session_id,
        anonymous_id,

        {% for (key, value) in first_values3.items() %}
        first_value({{key}}) over ({{window_clause}}) as {{value}},
        {% endfor %}

    from pageviews_sessionized

),
agg_first4 as (

    select distinct

        session_id,
        anonymous_id,

        {% for (key, value) in first_values4.items() %}
        first_value({{key}}) over ({{window_clause}}) as {{value}},
        {% endfor %}

    from pageviews_sessionized

),
agg_first5 as (

    select distinct

        session_id,
        anonymous_id,

        {% for (key, value) in first_values5.items() %}
        first_value({{key}}) over ({{window_clause}}) as {{value}},
        {% endfor %}

    from pageviews_sessionized

),
agg_first6 as (

    select distinct

        session_id,
        anonymous_id,

        {% for (key, value) in first_values6.items() %}
        first_value({{key}}) over ({{window_clause}}) as {{value}},
        {% endfor %}

    from pageviews_sessionized

),
agg_last1 as (

    select distinct

        session_id,
        anonymous_id,

        {% for (key, value) in last_values1.items() %}
        last_value({{key}}) over ({{window_clause}}) as {{value}}{% if not loop.last %},{% endif %}
        {% endfor %}

    from pageviews_sessionized

),
agg_last2 as (

    select distinct

        session_id,
        anonymous_id,

        {% for (key, value) in last_values2.items() %}
        last_value({{key}}) over ({{window_clause}}) as {{value}}{% if not loop.last %},{% endif %}
        {% endfor %}

    from pageviews_sessionized

),

diffs as (

    select

        af.*,
        af1.* except (session_id, anonymous_id),
        af2.* except (session_id, anonymous_id),
        af3.* except (session_id, anonymous_id),
        af4.* except (session_id, anonymous_id),
        af5.* except (session_id, anonymous_id),
        af6.* except (session_id, anonymous_id),
        al1.* except (session_id, anonymous_id),
        al2.* except (session_id, anonymous_id),

        {{ dbt.datediff('session_start_tstamp', 'session_end_tstamp', 'second') }} as duration_in_s

    from agg_first af
    left join agg_first1 af1
        on af.session_id = af1.session_id AND af.anonymous_id = af1.anonymous_id
    left join agg_first2 af2
        on af.session_id = af2.session_id AND af.anonymous_id = af2.anonymous_id
    left join agg_first3 af3
        on af.session_id = af3.session_id AND af.anonymous_id = af3.anonymous_id
    left join agg_first4 af4
        on af.session_id = af4.session_id AND af.anonymous_id = af4.anonymous_id
    left join agg_first5 af5
        on af.session_id = af5.session_id AND af.anonymous_id = af5.anonymous_id
    left join agg_first6 af6
        on af.session_id = af6.session_id AND af.anonymous_id = af6.anonymous_id
    left join agg_last1 al1
        on af.session_id = al1.session_id AND af.anonymous_id = al1.anonymous_id
    left join agg_last2 al2
        on af.session_id = al2.session_id AND af.anonymous_id = al2.anonymous_id

),

tiers as (

    select

        *,

        case
            when duration_in_s between 0 and 9 then '0s to 9s'
            when duration_in_s between 10 and 29 then '10s to 29s'
            when duration_in_s between 30 and 59 then '30s to 59s'
            when duration_in_s > 59 then '60s or more'
            else null
        end as duration_in_s_tier

    from diffs

),

mapped as (

    select
        tiers.*,
        referrer_mapping.medium as referrer_medium,
        referrer_mapping.source as referrer_source

    from tiers

    left join referrer_mapping on tiers.referrer_host = referrer_mapping.host

)

select * from mapped
