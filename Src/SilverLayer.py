from pyspark.sql.functions import col, when, upper

def create_silver_tables(DemographicsAll, DrugAll, ReactionAll, OutcomeAll):
    # ===============================
    # Demographics
    # ===============================
    silver_demographics = (
        DemographicsAll
        .filter(col("primary_id").isNotNull())
        .dropDuplicates(["primary_id"])
        .withColumn(
            "age_group_readable",
            when(col("age_group") == "A", "Adult (18-44)")
            .when(col("age_group") == "C", "Child (0-11)")
            .when(col("age_group") == "E", "Elderly (65+)")
            .when(col("age_group") == "I", "Infant (0-1)")
            .when(col("age_group") == "N", "Neonate (0-28 days)")
            .when(col("age_group") == "T", "Teen (12-17)")
            .otherwise("Unknown")
        )
        .withColumn(
            "gender_of_patient",
            when(col("gender_of_patient").isNull(), "UNKNOWN")
            .otherwise(col("gender_of_patient"))
        )
    )

    # ===============================
    # Drugs
    # ===============================
    silver_drugs = (
        DrugAll
        .filter(col("primary_id").isNotNull())
        .dropDuplicates(["primary_id", "drug_sequence_number", "drug_name"])
        .withColumn(
            "drug_name",
            when(col("drug_name").isNull(), "UNKNOWN DRUG")
            .otherwise(upper(col("drug_name")))
        )
        .withColumn("product_active_ingredient", upper(col("product_active_ingredient")))
    )

    # ===============================
    # Reactions
    # ===============================
    silver_reactions = (
        ReactionAll
        .filter(col("primary_id").isNotNull())
        .dropDuplicates(["primary_id", "preferred_term_for_event"])
        .withColumn("preferred_term_for_event", upper(col("preferred_term_for_event")))
    )

    # ===============================
    # Outcomes
    # ===============================
    silver_outcomes = (
        OutcomeAll
        .filter(col("primary_id").isNotNull())
        .dropDuplicates(["primary_id", "patient_outcome"])
    )

    # ===============================
    # Return all Silver tables
    # ===============================
    return {
        "demographics": silver_demographics,
        "drugs": silver_drugs,
        "reactions": silver_reactions,
        "outcomes": silver_outcomes
    }