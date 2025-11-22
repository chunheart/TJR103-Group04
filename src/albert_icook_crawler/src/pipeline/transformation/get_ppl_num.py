from albert_icook_crawler.src.pipeline.transformation import get_num as sen
import pandas as pd


def imputation_with_one(df: pd.DataFrame, col_name: str) -> list | None:
    # convert each number to the decimal type
    filled_series = df[f"{col_name}"].fillna("1人份", inplace=True)
    return filled_series

def get_recipe_ppl_num(text) -> int | None:
    """retrieve the number from the serving-number of recipe"""
    # get num
    num_part = sen.get_num_field_quantity(text)
    # make sure this program will run to the end
    try:
        return int(num_part)
    except ValueError as e:
        print(e)
        return None
