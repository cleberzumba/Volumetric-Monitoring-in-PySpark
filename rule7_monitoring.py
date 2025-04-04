def rule7_monitoring(sqlContext, schema, table, current_date, previous_date):
    print(f'\nRunning rule 7 for {table} with current date {current_date} - previous date {previous_date}')

    rule_id = 7
    rule_name = f"rule{rule_id}"

    if not current_date or not previous_date:
        print("Error: Processing dates cannot be None.")
        return

    def load_table(table_name, date1, date2):
        return sqlContext.table(table_name).where(col("processing_date").isin(date1, date2))

    # Load tables
    df_base = load_table("schema.fact_base", current_date, previous_date) \
        .withColumnRenamed("person_type", "base_person_type").alias("base")

    df_dim_1 = load_table("schema.dim_table_1", current_date, previous_date).alias("dim1")
    df_dim_2 = load_table("schema.dim_table_2", current_date, previous_date).alias("dim2")
    df_dim_3 = load_table("schema.dim_table_3", current_date, previous_date).alias("dim3")
    df_dim_4 = load_table("schema.dim_table_4", current_date, previous_date) \
        .withColumn("formatted_processing_date", translate(col("processing_date"), "-", "")).alias("dim4")

    # JOINs
    joined_df = df_base \
        .join(df_dim_1, (col("base.processing_date") == col("dim1.processing_date")) &
                        (col("base.id_1") == col("dim1.id_1")) &
                        (col("base.id_2") == col("dim1.id_2")), "left") \
        .join(df_dim_2, (col("base.processing_date") == col("dim2.processing_date")) &
                        (col("base.id_2") == col("dim2.id_2")), "left") \
        .join(df_dim_3, (col("base.processing_date") == col("dim3.processing_date")) &
                        (col("base.id_3") == col("dim3.id_3")), "left") \
        .join(df_dim_4, (col("base.processing_date") == col("dim4.formatted_processing_date")) &
                        (col("base.id_3") == col("dim4.id_4")), "left")

    # Derived columns
    joined_df = joined_df.withColumn("segment", when(col("category").isin("A", "B"), "TYPE_1")
                                                .when(col("category") == "C", "TYPE_2")
                                                .otherwise("OTHERS"))

    joined_df = joined_df.withColumn("person_type", when(col("base_person_type") == "F", "INDIVIDUAL")
                                                      .when(col("base_person_type") == "J", "CORPORATE")
                                                      .otherwise(None))

    # Aggregation
    volume_df = joined_df.groupBy("base.processing_date", "segment", "person_type").count()

    # Split into current and previous
    current_volume = volume_df.filter(col("processing_date") == current_date) \
                              .withColumnRenamed("count", "current_metric_value")

    previous_volume = volume_df.filter(col("processing_date") == previous_date) \
                               .withColumnRenamed("count", "previous_metric_value") \
                               .withColumnRenamed("processing_date", "previous_date")

    comparison_df = current_volume.join(previous_volume, ["segment", "person_type"], "outer").fillna(0)

    comparison_df = comparison_df.withColumn("variation_percentage",
        when(col("previous_metric_value") == 0, None)
        .otherwise(round(abs((col("current_metric_value") - col("previous_metric_value")) / col("previous_metric_value")) * 100, 2))
    )

    comparison_df = comparison_df.withColumn("metric_status",
        when(col("variation_percentage") < 20, lit("OK"))
        .when((col("variation_percentage") >= 20) & (col("variation_percentage") < 30), lit("WARNING"))
        .when(col("variation_percentage") >= 30, lit("NOT OK"))
        .otherwise("NOT OK")
    )

    comparison_df = comparison_df.withColumn("metric_status_description",
        when(col("variation_percentage") < 20, lit("variation < 20%"))
        .when((col("variation_percentage") >= 20) & (col("variation_percentage") < 30), lit("20% <= variation < 30%"))
        .when(col("variation_percentage") >= 30, lit("variation >= 30%"))
        .otherwise(" - ")
    )

    df_final = comparison_df.select(
        lit("ANALYTICS_PROJECT").alias("monitored_product"),
        lit(rule_id).alias("monitoring_rule_id"),
        col("person_type").alias("document_type"),
        col("segment").alias("metric_segment"),
        col("previous_metric_value").cast("decimal(18,4)").alias("previous_value"),
        col("current_metric_value").cast("decimal(18,4)").alias("current_value"),
        col("variation_percentage").cast("double").alias("monitoring_metric_value"),
        col("metric_status"),
        col("metric_status_description"),
        col("previous_date").alias("previous_partition_date"),
        col("processing_date").alias("current_partition_date"),
        col("processing_date").alias("processing_date")
    )

    df_final.createOrReplaceTempView(f"view_{rule_name}_final")
    df_final.show(truncate=False)
