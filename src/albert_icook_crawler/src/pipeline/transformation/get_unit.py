import albert_icook_crawler.src.pipeline.utils.num_regex_pattern as rep
from albert_icook_crawler.src.pipeline.transformation.get_num import have_chinese_char_num
from albert_icook_crawler.src.pipeline.utils.num_regex_pattern import FRACTION_SYMBOL_NUM_MAPS


fraction_pattern = '|'.join(rep.re.escape(k) for k in FRACTION_SYMBOL_NUM_MAPS.keys())
FRACTION_PATTERN_SET = r'[\u00BC-\u00BE\u2150-\u215E]'
PATTERN_OPTIMIZED = rf'(\d*\s*{FRACTION_PATTERN_SET}?)\s*(.+)'


def _replace_fraction(match):
    return FRACTION_SYMBOL_NUM_MAPS.get(match.group(0), '')

def extract_and_convert(text: str) -> str | None:
    match = rep.re.search(PATTERN_OPTIMIZED, text)
    unit = match.group(2).strip()
    unit_without_space = unit.replace(' ', '')
    try:
        return unit_without_space

    except ValueError:
        return None


"""The below is to get the number part"""
def get_unit_field_quantity(text: str) -> str | None:
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

            elif any(sep in text for sep in ("½", "⅓", "⅔", "¼", "¾", "⅕")):
                return extract_and_convert(text)

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

    return text

def match_unit(matches) ->  str | None:
    """
    extract num part of digits
    param matches: Iterator[Match[str]]
    """
    for m in matches: # activate this iterate generator
        return m.group(2)

    return None

if __name__ == "__main__":
    tests = ["1½匙"]
    for test in tests:
        ans = get_unit_field_quantity(test)
        print(ans, type(ans))