CREATE TABLE "feature_store"."energy_data_features"(
    "id" text,
    "day_date" date,
    "energy_median" double,
    "energy_mean" double,
    "energy_max" double,
    "energy_count" int,
    "energy_std" double,
    "energy_sum" double,
    "energy_min" double,
    "energy_sum_3months" double,
    "energy_sum_6months" double,
    "energy_sum_1yr" double,
    "energy_count_3months" bigint,
    "energy_count_6months" bigint,
    "energy_count_1yr" bigint,
    "energy_max_3months" double,
    "energy_max_6months" double,
    "energy_max_1yr" double,
    "energy_mean_3months" double,
    "energy_mean_6months" double,
    "energy_mean_1yr" double,
    "energy_stddev_3months" double,
    "energy_stddev_6months" double,
    "energy_stddev_1yr" double,
    PRIMARY KEY("id"))
WITH CUSTOM_PROPERTIES = {
    'capacity_mode':{'throughput_mode':'PAY_PER_REQUEST'}
}