import utils_regex_pattern as rep
from utils_separate_num import have_chinese_char_num


"""The below is to get the number part"""
def get_unit_in_field_quantity(text: str) -> str | None:
    """
    Separate the number and thr unit, and mainly fetch the number
    """

    have_digit_num = any(num.isdigit() for num in text)
    # first filter: check if text has digits
    if have_digit_num: # with digits
        # second filter: check if the value has a range
        if any(sep in text for sep in ("~", "-", "～", "至", "_")):
            # third filter: check if the value is a fraction
            if any(sep in text for sep in "/"):
                matches = rep.CMP_PATTERN_WITH_DIGITAL_FRACTION_RANGE.finditer(text)
                if matches is not None:
                    return match_unit(matches)
            else: # range without fraction
                matches = rep.CMP_PATTERN_WITH_DIGITAL_RANGE.finditer(text)
                if matches is not None:
                    return match_unit(matches)

        else: # not a range
            if any(sep in text for sep in "/"): # a fraction
                matches = rep.CMP_PATTERN_WITH_DIGITAL_FRACTION_WITHOUT_RANGE.finditer(text) # bool
                if matches is not None:
                    return match_unit(matches)
            else: # not a fraction
                matches = rep.CMP_PATTERN_WITH_DIGITAL_WITHOUT_RANGE.finditer(text)
                if matches is not None:
                    return match_unit(matches)

    elif have_chinese_char_num(text): # without digits, but with Chinese characters that stand for number
        # second filter: check if the value has a range
        if any(sep in text for sep in ("~", "-", "～", "至", "_")):
            # third filter: check if the value is a fraction
            if any(sep in text for sep in "分之"): # a fraction
                matches = rep.CMP_PATTERN_WITH_CHINESE_FRACTION_RANGE.finditer(text)
                if matches is not None:
                    return match_unit(matches)
            else: # not a fraction
                matches = rep.CMP_PATTERN_WITH_CHINESE_RANGE.finditer(text)
                if matches is not None:
                    return match_unit(matches)
        else: # not has a range
            if any(sep in text for sep in "分之"): # a fraction
                matches = rep.CMP_PATTERN_WITH_CHINESE_FRACTION_WITHOUT_RANGE.finditer(text) # bool
                if matches is not None:
                    return match_unit(matches)
            else: # not a fraction
                matches = rep.CMP_PATTERN_WITH_CHINESE_WITHOUT_RANGE.finditer(text)  # bool
                if matches is not None:
                    return match_unit(matches)
    return None

def match_unit(matches) ->  str | None:
    """
    extract num part of digits
    param matches: Iterator[Match[str]]
    """
    for m in matches: # activate this iterate generator
        return m.group(2)
    return None

# if __name__ == "__main__":
#     tests = ["1kg", "1.2kg", "1-2 kg", "1/2-1kg","1/3-1/2kg", "1.1-1.2kg", "一kg", "一～二kg", "三分之一~二分之一公斤"]
#     for test in tests:
#         ans = get_unit_in_field_quantity(test)
#         print(ans, type(ans))