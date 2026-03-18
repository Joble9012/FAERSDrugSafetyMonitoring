import pyspark.sql.functions as F

def create_silver_tables(Demographics, Drug, Reaction, Outcome):

    # ===============================
    # Demographics
    # ===============================
    silver_demographics = (
        Demographics
        .withColumn(
            "primary_id",
            F.when(F.col("primary_id").isNull(), F.monotonically_increasing_id())
            .otherwise(F.col("primary_id"))
        )
        .withColumn(
            "age_group_readable",
            F.when(F.col("age_group") == "A", "Adult (18-44)")
            .when(F.col("age_group") == "C", "Child (0-11)")
            .when(F.col("age_group") == "E", "Elderly (65+)")
            .when(F.col("age_group") == "I", "Infant (0-1)")
            .when(F.col("age_group") == "N", "Neonate (0-28 days)")
            .when(F.col("age_group") == "T", "Teen (12-17)")
            .otherwise("Unknown")
        )
        .withColumn(
            "gender_of_patient",
            F.when(F.col("gender_of_patient").isNull(), "UNKNOWN")
            .otherwise(F.col("gender_of_patient"))
        )
        .dropDuplicates(["primary_id", "year"])
    )

    # ===============================
    # Drugs
    # ===============================
    silver_drugs = (
        Drug
        .withColumn(
            "primary_id",
            F.when(F.col("primary_id").isNull(), F.monotonically_increasing_id())
            .otherwise(F.col("primary_id"))
        )
        .withColumn(
            "drug_name",
            F.when(F.col("drug_name").isNull(), "UNKNOWN DRUG")
            .otherwise(F.initcap(F.col("drug_name")))
        )
        .withColumn(
            "product_active_ingredient",
            F.initcap(F.col("product_active_ingredient"))
        )
        .dropDuplicates(["primary_id", "drug_sequence_number", "year"])
    )

    # ===============================
    # Reactions
    # ===============================
    silver_reactions = (
        Reaction
        .withColumn(
            "primary_id",
            F.when(F.col("primary_id").isNull(), F.monotonically_increasing_id())
            .otherwise(F.col("primary_id"))
        )
        .withColumn(
            "preferred_term_for_event",
            F.initcap(
                F.coalesce(
                    F.col("preferred_term_for_event"),
                    F.lit("Unknown")
                )
            )
        )
        .dropDuplicates(["primary_id", "preferred_term_for_event", "year"])
    )

    # ===============================
    # Outcomes
    # ===============================
    silver_outcomes = (
        Outcome
        .withColumn(
            "primary_id",
            F.when(F.col("primary_id").isNull(), F.monotonically_increasing_id())
            .otherwise(F.col("primary_id"))
        )
        .withColumn(
            "outcome_cleaned",
            F.when(F.col("patient_outcome") == "DE", "Death")
            .when(F.col("patient_outcome") == "CA", "Congenital Anomaly")
            .when(F.col("patient_outcome") == "DS", "Disability")
            .when(F.col("patient_outcome").isin("HO", "Hospitalization - Initial or Prolonged"), "Hospitalization")
            .when(F.col("patient_outcome") == "LT", "Life-Threatening")
            .when(F.col("patient_outcome").isin("OT", "Other Serious (Important Medical Event)"), "Other Serious Event")
            .when(F.col("patient_outcome").isin("RI", "Required Intervention to Prevent Permanent Impairment/Damage"), "Required Intervention")
            .otherwise("Unknown")  # ✅ handles both nulls and unmapped codes
        )
        .dropDuplicates(["primary_id", "outcome_cleaned", "year"])
    )

    return {
        "demographics": silver_demographics,
        "drugs": silver_drugs,
        "reactions": silver_reactions,
        "outcomes": silver_outcomes
    }