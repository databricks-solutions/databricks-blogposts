from pyspark import pipelines as dp
import pyspark.sql.functions as F

# I/O variables
table_prefix = spark.conf.get("table_prefix")
input_table_path = f"{table_prefix}_productmanuals_extract"

# Config variables
schema = """
    file_name STRING COMMENT 'Original PDF file name.',
    manufacturer STRING COMMENT 'Brand or manufacturer name.',
    model_number STRING COMMENT 'Product model number or identifier.',
    product_name STRING COMMENT 'Full product name or description.',
    product_type STRING COMMENT 'Type of tool (drill, drill/driver, hammer drill, etc.).',
    rated_voltage_v DOUBLE COMMENT 'Rated voltage in volts.',
    max_torque_nm DOUBLE COMMENT 'Maximum torque in Newton-meters.',
    no_load_speed_low_rpm INT COMMENT 'No-load speed for low gear in RPM.',
    no_load_speed_high_rpm INT COMMENT 'No-load speed for high gear in RPM.',
    chuck_capacity_mm STRING COMMENT 'Chuck capacity range in mm.',
    max_drilling_diameter_wood_mm DOUBLE COMMENT 'Maximum drilling diameter in wood in mm.',
    max_drilling_diameter_steel_mm DOUBLE COMMENT 'Maximum drilling diameter in steel in mm.',
    weight_kg DOUBLE COMMENT 'Weight of the tool in kg (without battery).',
    compatible_batteries STRING COMMENT 'Compatible battery packs.',
    compatible_chargers STRING COMMENT 'Compatible chargers.'
"""


@dp.table(
    name=f"{table_prefix}_productmanuals_processed",
    comment="Processed product catalog from power tool manuals: structured specifications for cross-vendor comparison, procurement intelligence, and product recommendation.",
    schema=schema,
)
def productmanuals_processed():
    return (
        spark.readStream.table(input_table_path)
        .select(
            F.col("file_name"),
            F.expr("ai_result:response.manufacturer::STRING").alias("manufacturer"),
            F.expr("ai_result:response.model_number::STRING").alias("model_number"),
            F.expr("ai_result:response.product_name::STRING").alias("product_name"),
            F.expr("ai_result:response.product_type::STRING").alias("product_type"),
            F.expr("ai_result:response.rated_voltage_v::DOUBLE").alias("rated_voltage_v"),
            F.expr("ai_result:response.max_torque_nm::DOUBLE").alias("max_torque_nm"),
            F.expr("ai_result:response.no_load_speed_low_rpm::INT").alias("no_load_speed_low_rpm"),
            F.expr("ai_result:response.no_load_speed_high_rpm::INT").alias("no_load_speed_high_rpm"),
            F.expr("ai_result:response.chuck_capacity_mm::STRING").alias("chuck_capacity_mm"),
            F.expr("ai_result:response.max_drilling_diameter_wood_mm::DOUBLE").alias("max_drilling_diameter_wood_mm"),
            F.expr("ai_result:response.max_drilling_diameter_steel_mm::DOUBLE").alias("max_drilling_diameter_steel_mm"),
            F.expr("ai_result:response.weight_kg::DOUBLE").alias("weight_kg"),
            F.expr("ai_result:response.compatible_batteries::STRING").alias("compatible_batteries"),
            F.expr("ai_result:response.compatible_chargers::STRING").alias("compatible_chargers"),
        )
    )
