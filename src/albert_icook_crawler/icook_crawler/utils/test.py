
import utils_separate_num as sen
import utils_regex_pattern as rep
import pandas as pd

csv_file = "/Users/cct/Downloads/2025-10-18.csv"


df = pd.read_csv(csv_file)
df["t_qty_raw"] = df["qty_raw"].apply(sen.get_num_in_field_quantity)

selected_df = df[["ingredient", "qty_raw", "t_qty_raw"]]

selected_df.to_csv("/Users/cct/Downloads/test_2025-10-18_4.csv", index=False, encoding="utf-8")

# ok: "1.5公斤",  "三公斤", "1kg", "1/3公斤", "1.2-2.1公斤", "1-2公斤", "1/3-1/2公斤", "一～三公斤", "三分之二～三分之二公斤", "三分之二~一公斤", "半茶匙"
def test():
    # text = ["1.5公斤",  "三公斤", "1kg", "1/3公斤", "1.2-2.1公斤", "1-2公斤", "1/3-1/2公斤", "一～三公斤", "三分之二～三分之二公斤", "三分之二~一公斤", "半茶匙"]
    text1 = ["1000\n\t人份"]
    for text in text1:
        text = rep.blank_comma_removal(text)
        # matches = COMPILED_PATTERN_WITH_NUMBERS_RANGE.finditer(text)
        # print("=" * 30)
        # for m in matches:
        #     print(m.group(0))
        #     print(m.group(1))
        #     print(m.group(2))
        try:
            print(text)
            num_part = sen.get_num_in_field_quantity(text)
            unit_part = sen.get_unit_in_field_quantity(text)
            print(num_part, type(num_part))
            print(unit_part, type(unit_part))
            print("=" * 30)
        except ValueError as e:
            print(e)

# if __name__ == "__main__":
#     test()
#     # test_text = "2\n\t人份"
#     # test_text = test_text.replace("\n","").replace("\t", "")
#     # print(test_text)
# # s = "https://icook.tw/recipes/479331"
# # s1 = "https://www.ytower.com.tw/recipe/iframe-recipe.asp?seq=B01-2003"
# # print(len(s1))

if __name__ == "__main__":
    # # load data
    # test_df = pd.read_csv("/Users/cct/Downloads/output_csvs/icook_599.csv")
    # # impute the cells whose values are NaN
    # test_df["people"] = imputation_with_one(test_df, "people")
    # # remove blank, newline mark of the left and right sides
    # test_df["people"] = test_df["people"].apply(lambda x: x.strip())
    # # remove blank, newline mark of between words
    # test_df["people"] = test_df["people"].replace("\t", "").replace("\n", "")
    # # retrieve the num part
    # test_df["t_people"] = test_df["people"].apply(get_recipe_ppl_num)
    # print(test_df["t_people"])
    text = "三分之一"
    exist = any(sep in text for sep in "分之")
    if exist:
        print("exist")
    else:
        print("not exist")
