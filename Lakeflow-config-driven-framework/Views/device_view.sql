-- Databricks notebook source
CREATE OR REPLACE VIEW databricks_ps_test.silver.device_view (
    id COMMENT 'Internal identifier for the device.',
    tenant COMMENT 'Internal identifier for the tenant.',
    model_id COMMENT 'Identifier for the device model.',
    product_id COMMENT 'Identifier for the device product.',
    manufacturer_id COMMENT 'Identifier for the device manufacturer.',
    family_id COMMENT 'Identifier for the device family.',
    lifecycle_id COMMENT 'Identifier for the device lifecycle.',
    taxonomy_id COMMENT 'Identifier for the device taxonomy.',
    platform_id COMMENT 'Identifier for the device platform.',
    os_release_id COMMENT 'Identifier for the device operating system release.',
    os_product_id COMMENT 'Identifier for the device operating system product.',
    manufacturer_name COMMENT 'Name of the device manufacturer, providing human-readable identification.',
    product_name COMMENT 'Name of the device product, providing human-readable identification.',
    model_name COMMENT 'Name of the device model, providing human-readable identification.',
    evidence_manufacturer_name COMMENT 'Name of the manufacturer of the evidence associated with the device, providing human-readable identification.',
    evidence_model_name COMMENT 'Name of the model of the evidence associated with the device, providing human-readable identification.',
    --business_unit COMMENT 'Identifier for the business unit responsible for the device.',
    --cost_center COMMENT 'Identifier for the cost center responsible for the device.',
    --location COMMENT 'Identifier for the location of the device.',
    chassis_type COMMENT 'Identifier for the chassis type of the device.',
    domain COMMENT 'Represents the domain or category of the device, providing context for its use and purpose.',
    hdd_total_space COMMENT 'Represents the total storage capacity of the device\'s hard disk drive(s).',
    inventory_agent COMMENT 'Identifies the agent responsible for managing the device\'s inventory.',
    ignored COMMENT 'A boolean flag indicating whether the device should be ignored or not.',
    inventory_date COMMENT 'The date when the device was added to the inventory.',
    last_loggedon_user COMMENT 'Represents the user who last logged into the device.',
    name COMMENT 'The name of the device, providing a human-readable label for identification.',
    serial_number COMMENT 'Unique identifier for the device, allowing tracking and management of individual devices.',
    total_memory COMMENT 'Represents the total memory capacity of the device.',
    calculated_user COMMENT 'Represents the user account calculated by the device, providing information about the device\'s user-facing capabilities.',
    service_pack COMMENT 'Represents the service pack or version of the device\'s software.',
    created_at COMMENT 'The date and time when the device was created or manufactured.',
    updated_at COMMENT 'The date and time when the device was last updated or modified.',
    platform_label COMMENT 'Represents the label or name of the device\'s platform.',
    platform_type COMMENT 'Represents the type of the device\'s platform, such as operating system or firmware.',
    number_of_hdd COMMENT 'Represents the number of hard disk drives (HDDs) in the device.',
    number_of_display_adapters COMMENT 'Represents the number of display adapters in the device.',
    processor_name COMMENT 'Represents the name of the device\'s processor.',
    number_of_processors COMMENT 'Represents the number of processors in the device.',
    max_clock_speed COMMENT 'Represents the maximum clock speed of the device\'s processor.',
    number_of_cores COMMENT 'Represents the total number of physical cores in the device.',
    number_of_logical_cores COMMENT 'Represents the total number of logical cores in the device, which is the number of virtual cores that can be used by the operating system.'--,
    --bios_version COMMENT 'Identifier for the BIOS version.',
    --bios_release_date COMMENT 'Date when the BIOS was released.'
) COMMENT 'The *device* dataset contains information about devices in your inventory, including their unique identifier, model, manufacturer, and lifecycle details.' AS
SELECT
    c.id,
    c.tenant,
    -- dimension foreign keys
    c.technopedia_model_tid AS model_id,
    c.technopedia_product_tid AS product_id,
    c.technopedia_owner_tid AS manufacturer_id,
    coalesce(hp.hardware_family_id, hp_core.hardware_family_id) AS family_id,
    coalesce(hlc.ID, hlc_itsm.ID) AS lifecycle_id,
    coalesce(hm.taxonomy_id, hm_core.taxonomy_id, hp.taxonomy_id, hp_core.taxonomy_id) AS taxonomy_id,
    ios.platform_id AS platform_id,
    ios.release_id AS os_release_id,
    ios.product_id AS os_product_id,

    -- -- descriptive names
    coalesce(ma.name, ma_core.name)  AS manufacturer_name,
    coalesce(hp.name, hp_core.name) AS product_name,
    coalesce(hm.name, hm_core.name) AS model_name,

    -- properties
    c.manufacturer AS evidence_manufacturer_name,
    c.model_no AS evidence_model_name,
    --c.business_unit,
    --c.cost_center,
    --c.location,
    c.chassis_type,
    c.domain,
    c.hdd_total_space,
    c.inventory_agent,
    c.ignored,
    c.inventory_date,
    c.last_loggedon_user,
    c.name,
    c.serial_number,
    c.total_memory,
    c.calculated_user,
    c.service_pack,
    c.created_at,
    c.updated_at,
    ios.platform_label,
    ios.platform_type,

    -- pre-calc agreggation values
    c.hdd_count AS number_of_hdd,
    c.display_adapter_count AS number_of_display_adapters,

    proc.name AS processor_name,
    c.number_of_processors AS number_of_processors,
    proc.max_clock_speed AS max_clock_speed,
    proc.number_of_cores AS number_of_cores,
    proc.number_of_logical_cores AS number_of_logical_cores--,
    --c.bios_version,
    --c.bios_release_date

FROM databricks_ps_test.bronze.computer3 AS c

----old logic----
-- LEFT JOIN LATERAL (
--     SELECT
--         first(proc.name) AS name,
--         max(proc.clock_speed_max) AS max_clock_speed,
--         sum(proc.core_count) AS number_of_cores,
--         sum(proc.logical_count) AS number_of_logical_cores
--     FROM posexplode(c.processors) AS c(computer_idx, proc)
-- ) AS proc
----old logic----
--*************--
----intrim logic----
-- LEFT JOIN LATERAL (
--     SELECT
--         first(proc.name) AS name, --- issue here 
--         max(proc.clock_speed_max) AS max_clock_speed,
--         sum(proc.core_count) AS number_of_cores,
--         sum(proc.logical_count) AS number_of_logical_cores
--     FROM databricks_ps_test.bronze.computer_processor proc
--     WHERE proc.device_id = c.id and c.tenant = proc.tenant
-- ) AS proc ON true
----intrim logic----
--- new logic ---
LEFT JOIN LATERAL (
    SELECT
        FIRST_VALUE(proc.name) OVER (ORDER BY proc.name) AS name,
        MAX(proc.clock_speed_max) OVER () AS max_clock_speed,
        SUM(proc.core_count) OVER () AS number_of_cores,
        SUM(proc.logical_count) OVER () AS number_of_logical_cores
    FROM databricks_ps_test.bronze.computer_processor proc
    WHERE proc.device_id = c.id AND c.tenant = proc.tenant
    LIMIT 1
) AS proc ON true
--- new logic ---
-- operating system
LEFT JOIN (
    SELECT
        ios.computer_id,
        coalesce(srp.platform_id   ,srp_core.platform_id   ) as platform_id,
        coalesce(srp.platform_label,srp_core.platform_label) as platform_label,
        coalesce(srp.platform_type ,srp_core.platform_type ) as platform_type,
        ios.technopedia_release_tid AS release_id,
        ios.technopedia_product_tid AS product_id,
        row_number() OVER (PARTITION BY ios.computer_ID ORDER BY ios.updated_at DESC, ios.id ASC, srp.platform_id) AS _rank
    FROM databricks_ps_test.bronze.installed_operating_system ios
    JOIN (select * from databricks_ps_test.bronze.onetp_software_release_platform where tenant <> 'core') srp ON srp.software_release_id = ios.technopedia_release_tid
    JOIN (select * from databricks_ps_test.bronze.onetp_software_release_platform where tenant  = 'core') srp_core ON srp_core.software_release_id = ios.technopedia_release_tid
) ios ON ios.computer_id = c.id AND ios._rank = 1
-- technopedia
LEFT JOIN (select * from databricks_ps_test.bronze.onetp_hardware_model where tenant <> 'core') hm ON c.technopedia_model_tid = hm.id and c.tenant = hm.tenant
LEFT JOIN (select * from databricks_ps_test.bronze.onetp_hardware_model where tenant  = 'core') hm_core ON c.technopedia_model_tid = hm_core.id --and c.tenant = hm_core.tenant

LEFT JOIN (select * from databricks_ps_test.bronze.onetp_hardware_product where tenant <> 'core') hp ON c.technopedia_product_tid = hp.id and c.tenant = hp.tenant
LEFT JOIN (select * from databricks_ps_test.bronze.onetp_hardware_product where tenant  = 'core') hp_core ON c.technopedia_product_tid = hp_core.id --and c.tenant = hp_core.tenant


LEFT JOIN (select * from databricks_ps_test.bronze.onetp_manufacturer where tenant <> 'core') ma ON hm.manufacturer_id = ma.id and hm.tenant = ma.tenant
LEFT JOIN (select * from databricks_ps_test.bronze.onetp_manufacturer where tenant  = 'core') ma_core ON hm_core.manufacturer_id = ma_core.id --and hm_core.tenant = ma_core.tenant


LEFT JOIN (select * from databricks_ps_test.bronze.onetp_hardware_life_cycle where tenant <> 'itsm') hlc ON hlc.model_id = hm.id and hlc.tenant = hm.tenant
LEFT JOIN (select * from databricks_ps_test.bronze.onetp_hardware_life_cycle where tenant =  'itsm') hlc_itsm ON hlc_itsm.model_id = hm_core.id --and hlc_itsm.tenant = hm_core.tenant
WHERE c.ignored = FALSE;