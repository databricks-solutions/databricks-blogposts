device_model="""
SELECT
    id,
    name,
    hardware_family_name,
    is_desupported,
    is_discontinued,
    supported_os,
    id_legacy as data_platform_id,
    tenant
FROM databricks_ps_test.metadata_driven_schema.onetp_hardware_model
WHERE id IN (SELECT DISTINCT model_id FROM LIVE.device)
"""

device_product="""
SELECT
    id,
    name,
    hardware_family_name,
    is_desupported,
    is_discontinued,
    vendor_category,
    id_legacy as data_platform_id,
    tenant
FROM databricks_ps_test.metadata_driven_schema.onetp_hardware_product
WHERE id IN (SELECT DISTINCT product_id FROM LIVE.device)
"""

device_manufacturer="""
SELECT
    id,
    acquired_date,
    city,
    country,
    description,
    email,
    employees,
    try_cast(employees_date AS DATE),
    fax,
    try_cast(fiscal_end_date AS DATE),
    industry_tag_title,
    CASE
        WHEN is_publicly_traded = "yes" THEN True
        WHEN is_publicly_traded = "no" THEN False
        ELSE NULL
    END AS is_publicly_traded,
    known_as,
    legal,
    name,
    owner_id,
    phone,
    try_cast(profits_date AS DATE),
    profits_per_year,
    revenue,
    try_cast(revenue_date AS DATE),
    short_name,
    state,
    street,
    symbol,
    tier,
    website,
    zip,
    id_legacy as data_platform_id,
    tenant
FROM databricks_ps_test.metadata_driven_schema.onetp_manufacturer
WHERE id IN (SELECT DISTINCT manufacturer_id FROM LIVE.device)
"""

device_lifecycle="""
SELECT 
     id,
    general_availability,
    try_cast(general_availability_date AS DATE) AS general_availability_date,
    general_availability_exception,
    ga_range_start as general_availability_range_start,
    ga_range_end as general_availability_range_end,
    obsolete,
    try_cast(obsolete_date AS DATE) AS obsolete_date,
    obsolete_exception,
    obsolete_range_start,
    obsolete_range_end,
    try_cast(introduction_date AS DATE) AS introduction_date,
    introduction_exception,
    introduction_range_start,
    introduction_range_end,
    last_availability,
    try_cast(last_availability_date AS DATE) AS last_availability_date,
    last_availability_exception,
    last_availability_range_start,
    last_availability_range_end,
    CASE
        WHEN obsolete_date IS NULL THEN NULL
        WHEN (obsolete_date > '1900-01-01' AND obsolete_date < current_date()) THEN True
        ELSE False
    END AS `is_obsolete_date_expired`,
    CASE
        WHEN introduction_date IS NULL THEN NULL
        WHEN (introduction_date > '1900-01-01' AND introduction_date < current_date()) THEN True
        ELSE False
    END AS `is_introduction_date_expired`,
    CASE
        WHEN last_availability_date IS NULL THEN NULL
        WHEN (last_availability_date > '1900-01-01' AND last_availability_date < current_date()) THEN True
        ELSE False
    END AS `is_last_availability_date_expired`,
    CASE
        WHEN general_availability_date IS NULL THEN NULL
        WHEN (general_availability_date > '1900-01-01' AND general_availability_date < current_date()) THEN True
        ELSE False
    END AS `is_general_availability_date_expired`,
    datediff(general_availability_date, current_date()) as days_until_general_availability_date,
    datediff(obsolete_date, current_date()) as days_until_obsolete_date,
    datediff(introduction_date, current_date()) as days_until_introduction_date,
    datediff(last_availability_date, current_date()) as days_until_last_availability_date,
    id_legacy as data_platform_id,
    tenant
FROM databricks_ps_test.metadata_driven_schema.onetp_hardware_life_cycle WHERE id IN (SELECT DISTINCT lifecycle_id FROM LIVE.device)
"""
