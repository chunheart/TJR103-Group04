

def only_one_value(text: str) -> str | bool:
    """
    FOR FIELD INGREDIENTS USAGE
    make sure every cell has only one ingredient.
    """

    test_comma = text.split(",")
    if len(test_comma) == 1:
         pass
    else:
        return f"{text} has multiple values separated by comma, \",\""

    test_big_comma = text.split("，")
    if len(test_big_comma) == 1:
        pass
    else:
        return f"{text} has multiple values separated by Chinese comma, \"，\""

    test_dun_comma = text.split("、")
    if len(test_dun_comma) == 1:
        pass
    else:
        return f"{text} has multiple values separated by dun-comma, \"、\""

    test_blank = text.split(" ")
    if len(test_blank) == 1:
        pass
    else:
        return f"{text} has multiple values separated by blank"

    return True


def test():
    text1 = "紅蘿蔔"
    text2 = "紅蘿蔔 香蕉"
    text3 = "紅蘿蔔,香蕉"
    text4 = "紅蘿蔔, 香蕉"
    text5 = "紅蘿蔔、香蕉"

    print("-"*10, "result", "-"*10)
    print(only_one_value(text1))
    print(only_one_value(text2))
    print(only_one_value(text3))
    print(only_one_value(text4))
    print(only_one_value(text5))


if __name__ == "__main__":
    test()