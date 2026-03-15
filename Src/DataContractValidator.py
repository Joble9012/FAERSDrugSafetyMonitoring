import yaml
from pyspark.sql.functions import col

# ===============================
# Load YAML contract
# ===============================
def load_contract(path: str):
    """Load YAML contract file"""
    with open(path, "r") as f:
        contract = yaml.safe_load(f)
    return contract


# ===============================
# Validate Table Against Contract
# ===============================
def validate_table_contract(df, table_name: str, contract: dict):
    """Validate a Spark DataFrame against YAML contract"""
    
    # Check contract exists for table
    if table_name not in contract["tables"]:
        raise Exception(f"Contract not defined for table {table_name}")
    
    table_contract = contract["tables"][table_name]
    
    for col_contract in table_contract["columns"]:
        col_name = col_contract["name"]
        
        # Column exists
        if col_name not in df.columns:
            raise Exception(f"[{table_name}] Missing column {col_name}")
        
        # Null check for non-nullable columns
        if not col_contract.get("nullable", True):
            null_count = df.filter(col(col_name).isNull()).limit(1).count()
            if null_count > 0:
                raise Exception(
                    f"[{table_name}] Null values found in non-nullable column: {col_name}"
                )
        
        # Allowed values check
        allowed_values = col_contract.get("allowed_values")
        if allowed_values:
            invalid_count = (
                df.filter(~col(col_name).isin(allowed_values) & col(col_name).isNotNull())
                  .limit(1)
                  .count()
            )
            if invalid_count > 0:
                raise Exception(
                    f"[{table_name}] Invalid values found in column {col_name}"
                )
    
    # Duplicate checks (example only for demographics)
    if table_name == "demographics":
        duplicate_count = (
            df.groupBy("primary_id")
              .count()
              .filter(col("count") > 1)
              .limit(1)
              .count()
        )
        if duplicate_count > 0:
            raise Exception(f"[{table_name}] Duplicate primary_id values detected")
    
    print(f"[{table_name}] Data contract passed")