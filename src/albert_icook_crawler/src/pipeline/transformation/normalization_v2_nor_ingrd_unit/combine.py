import pandas as pd
import numpy as np

from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[4]
INGREDIENT_FILE_PATH = ROOT_DIR / "data" / "db_ingredients" / f"icook_recipe_{datetime.today().date()}_recipe_ingredients_unitN.csv"
REFERENCE_FILE_PATH = ROOT_DIR / "data" / "db_unit_normalization" / "unit_normalization_db.csv"
RESULT_DIR = ROOT_DIR / "data" / "db_ingredients" / "combine"
RESULT_DIR.mkdir(exist_ok=True, parents=True)
RESULT_FILE_PATH = ROOT_DIR / "data" / "db_ingredients" / "combine" /f"icook_recipe_{datetime.today().date()}_recipe_ingredients_unitN_combine.csv"

def fill_weights_with_composite_key(recipe_path, unit_path, output_path):
    # 1. Read data
    df_recipe = pd.read_csv(recipe_path)
    df_unit = pd.read_csv(unit_path)

    # ================= Configure Column Names (Modify Here) =================
    # A. Column names for icoo_recipe (The target table to be filled)
    RECIPE_ITEM_COL = 'ingredients'          # Ingredient Name column (e.g., Apple)
    RECIPE_UNIT_COL = 'unit_name'                # Unit column (e.g., Piece, Cup)
    TARGET_COL      = 'Normalized_Weight_g' # The weight column to be filled

    # B. Column names for unit_normal (The reference table)
    REF_ITEM_COL    = 'ingredients'                # Ingredient Name in reference table
    REF_UNIT_COL    = 'unit_name'           # Unit Name in reference table
    REF_WEIGHT_COL  = 'grams_per_unit'   # Standard Weight in reference table
    # ========================================================================

    print(f"Total rows in original data: {len(df_recipe)}")

    # 2. Create Composite Key Lookup Map (Mapping Dictionary)
    # Set 'Ingredient' and 'Unit' from the unit table as Index.
    # This creates a (Ingredient, Unit) tuple as the Key.
    # Resulting dict format: { ('Apple', 'Piece'): 150.0, ('Apple', 'Box'): 5000.0 }
    
    lookup_map = df_unit.set_index([REF_ITEM_COL, REF_UNIT_COL])[REF_WEIGHT_COL].to_dict()

    # 3. Define condition for filling
    # Condition: Value is NaN OR Value is 0.0
    condition = (df_recipe[TARGET_COL].isna()) | (df_recipe[TARGET_COL] == 0.0)
    print(f"Found {condition.sum()} rows that need filling...")

    # 4. Execute Mapping (Core Logic)
    if condition.sum() > 0:
        # 4.1 Create a list of (Ingredient, Unit) tuples for the rows that need fixing
        # zip combines the two columns into tuples, e.g., ('Apple', 'Piece')
        keys_to_lookup = list(zip(
            df_recipe.loc[condition, RECIPE_ITEM_COL], 
            df_recipe.loc[condition, RECIPE_UNIT_COL]
        ))
        
        # 5.2 Look up values in the dictionary
        # If key is not found, .get() returns NaN (or specified default)
        mapped_values = [lookup_map.get(key, np.nan) for key in keys_to_lookup]
        
        # 5.3 Fill data back
        # Note: We only fill values that were successfully found (non-NaN).
        # If the lookup results in NaN (key not in unit_normal), the original 0.0 remains.
        
        # Convert mapped_values to a Series to align indices
        fill_series = pd.Series(mapped_values, index=df_recipe[condition].index)
        
        # Get current values for the target rows
        current_values = df_recipe.loc[condition, TARGET_COL]
        
        # Use combine_first to update. 
        # Logic: Update 0.0 with found value. If found value is NaN, keep original.
        df_recipe.loc[condition, TARGET_COL] = fill_series.combine_first(current_values.replace(0.0, np.nan))

    # 6. Check results
    remaining_zeros = (df_recipe[TARGET_COL] == 0.0).sum()
    remaining_nans = df_recipe[TARGET_COL].isna().sum()
    print(f"Processing complete. Remaining 0.0 count: {remaining_zeros}, Remaining NaN count: {remaining_nans}")

    # 7. Output
    df_recipe.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"File saved to: {output_path}")

if __name__ == "__main__":
    fill_weights_with_composite_key(
        recipe_path=INGREDIENT_FILE_PATH,
        unit_path=REFERENCE_FILE_PATH,
        output_path=RESULT_FILE_PATH,
    )