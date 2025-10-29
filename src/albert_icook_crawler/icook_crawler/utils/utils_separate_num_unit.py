import re

from decimal import Decimal, ROUND_HALF_UP

"""
README:
This util is to process the filed of quantity and it mainly separate the values into the number part and the unit part.
Basically, The data type of the number part is decimal; the unit part is string 
"""

"""The below is constant"""
PATTERN_STR_VERBOSE_WITH_NUMBERS = r"""
    ( # Group 1: 捕獲整個「數字」部分
        (?:(?:.*)+分之(?:.*))                            # 中文表達分數 (e.g., 三分之二)
        |
        (?:\d+(?:\.\d+)?)[~-](?:\d+(?:\.\d+)?)          # 範圍 (e.g., 2-3)
        | 
        (?:\d+\/\d+)                                    # 分數 (e.g., 1/2)
        | 
        (?:[半一二三四五六七八九十百千萬]+)                  # 中文數字 (e.g., 一)
        | 
        (?:\d+(?:\.\d+)?)                               # 整數/小數 (e.g., 200, 0.5)
        # |
        
    ) # --- Group 1 結束 ---
    
    (?: # 單位部分 (可選)
        \s* # 0 或多個空格
        ( # Group 2: 捕獲「單位」本身
            [a-zA-Z%°\.一-龥]+   # 英文/符號/中文 
        ) # --- Group 2 結束 ---
    )? # 整個單位部分可選
    """

# PATTERN_STR_VERBOSE_WITHOUT_NUMBERS = r"""
#     # 直接選單位
#     (\b\w+\b)   # 完全沒數字 (e.g., 適量))
#     """

# Compiled pattern with numbers
COMPILED_PATTERN_WITH_NUMBERS = re.compile(
    PATTERN_STR_VERBOSE_WITH_NUMBERS,
    re.VERBOSE
)

# Compiled pattern without numbers
COMPILED_PATTERN_WITHOUT_NUMBERS = re.compile(
    PATTERN_STR_VERBOSE_WITH_NUMBERS,
    re.VERBOSE
)

# MANDARIN_FRACTION = re.compile(r"[一二三四五六七八九十]+分之[一二三四五六七八九十]+")

# convert the characters with the meaning of number to the relative numeric numbers
CHAR_NUM_MAPS ={
    "零": 0,
    "半": 0.5,
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
}


def blank_comma_removal(text: str) -> str:
    """
    Remove the comma symbol.
    Here it will also remove blanks.

    Param text : string
    Return : string
    """
    text = text.strip()
    text = text.replace(" ", "")

    if "," in text:
        text = text.replace(",", "")
        return text
    return text


"""The below is to get the number part"""
def get_num_in_field_quantity(text: str) -> float | str | None:
    """
    Separate the number and thr unit, and mainly fetch the number
    """
    have_num = any(num.isdigit() for num in text)
    if have_num: # check if text has numerics
        matches = COMPILED_PATTERN_WITH_NUMBERS.finditer(text) # bool
        if matches is not None:
            return match_num_with_digit(matches)
    else: # check if text has chinese-character numbers
        have_char_num = have_chinese_char_num(text)
        if have_char_num:
            matches = COMPILED_PATTERN_WITH_NUMBERS.finditer(text) # bool
            if matches is not None:
                return match_num_with_chinese(matches)
    return None


def have_chinese_char_num(text) -> bool:
    """
    # check if text has chinese-character numbers
    param matches: Iterator[Match[str]]
    return: bool
    """
    for char in text:
        if char in CHAR_NUM_MAPS:
            return True
    return False


def match_num_with_digit(matches) -> float | Decimal | str | None:
    """
    extract num part of digits
    param matches: Iterator[Match[str]]
    """

    for m in matches: # activate this iterate generator
        try:
            """when value is presented as an integer or an integer with decimal, such as 1 or 1.5"""
            number_part = Decimal(m.group(1))
            return number_part

        except ValueError:
            """ when value is presented as a fraction, such as 1/3"""
            if "/" in m.group(1):
                fraction = m.group(1).split("/") # list
                """
                fraction[0] = numerator(分子)
                fraction[-1] = denominator(分母)
                """
                try:
                    numerator = Decimal(fraction[0])
                    denominator = Decimal(fraction[-1])
                    number_part = (numerator / denominator).quantize(
                        Decimal("0.01"),
                        rounding=ROUND_HALF_UP,
                    )
                    return number_part
                except ValueError:
                    return m.group(1)

            elif any(sep in m.group(1) for sep in ("-", "~")):
                """when value is presented as a range of numbers, such as 4-5 or 4~5"""
                range_num = re.split(r"[-~]", m.group(1))
                first_num = Decimal(range_num[0])
                last_num = Decimal(range_num[-1])
                average = (first_num + last_num) / 2
                return average
    return None


def match_num_with_chinese(matches) -> float | Decimal | str | None:
    """
    extract num part of digits that are meant by Chinese characters
    param matches: Iterator[Match[str]]
    """
    # 三 | 三分之一
    for m in matches:
        # no 分之
        if "分之" not in m.group(1):
            if m.group(1) in CHAR_NUM_MAPS:
                return Decimal(CHAR_NUM_MAPS[m.group(1)])
            else:
                return m.group(1)

        char_fraction = m.group(1).split("分之")
        # with 分之
        if char_fraction:
            """
            char_fraction[-1] = numerator(分子)
            char_fraction[0] = denominator(分母)
            """
            try:
                numerator = Decimal(char_fraction[-1])
                denominator = Decimal(char_fraction[0])
                number_part = (numerator / denominator).quantize(
                    Decimal("0.01"),
                    rounding=ROUND_HALF_UP,
                )
                return number_part
            except ValueError:
                return m.group(1)
    return None


"""The below is to get the unit part"""
# thoughts:
# check if the value can be analyzed to extract digits and those meant by Chinese characters
# if true, just find m.group(2)
# if false, that means there is no number part, can just return text

def get_unit_in_field_quantity(text: str) ->  str | None:
    have_num = any(num.isdigit() for num in text)
    if have_num:
        matches = COMPILED_PATTERN_WITH_NUMBERS.finditer(text)
        if matches:
            return match_unit_in_field_quantity(matches)
    else:
        have_char_num = have_chinese_char_num(text)
        if have_char_num:
            matches = COMPILED_PATTERN_WITH_NUMBERS.finditer(text)
            return match_unit_in_field_quantity(matches)
    return text


def match_unit_in_field_quantity(matches) -> str | None:
    """
    extract unit part of quantity
    param matches: Iterator[Match[str]]
    """
    for m in matches:
        return m.group(2)
    return None

# "1/3公斤", , "三分之二公斤"
def test():
    text = ["1.5公斤",  "三公斤", "1kg"]
    for text in text:
        matches = COMPILED_PATTERN_WITH_NUMBERS.finditer(text)
        print("=" * 30)
        for m in matches:
            print(m.group(0))
            print(m.group(1))
            print(m.group(2))
        try:
            num_part = get_num_in_field_quantity(text)
            unit_part = get_unit_in_field_quantity(text)
            print("="*30)
            print(num_part, type(num_part))
            print("="*30)
            print(unit_part, type(unit_part))
        except ValueError as e:
            print(e)

if __name__ == "__main__":
    test()