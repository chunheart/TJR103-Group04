import utils_separate_num as sen
import pandas as pd

from twisted.protocols.amp import Decimal

def imputation_with_one(df: pd.DataFrame, col_name: str) -> list | None:
    # convert each number to the decimal type
    filled_series = df[f"{col_name}"].fillna("1人份", inplace=False)
    return filled_series

def get_recipe_ppl_num(text) -> Decimal | None:
    """retrieve the number from the serving-number of recipe"""
    # get num
    num_part = sen.get_num_in_field_quantity(text)
    # make sure this program will run to the end
    try:
        return num_part
    except ValueError as e:
        print(e)
        return None
