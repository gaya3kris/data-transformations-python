from data_transformations.wordcount import word_count_transformer
from tests.unit import SPARK

def test_remove_nothing() -> None:
    no_special_characters = SPARK.createDataFrame(
        [
            ["Hello World"]
        ],
        ["value"]
    )

    actual = word_count_transformer.remove_special_characters("value",no_special_characters)
    expected = no_special_characters

    assert expected.collect() == actual.collect()

def test_replace_special_characters_space() -> None:
    no_special_characters = SPARK.createDataFrame(
        [
            ["Hello World's!"]
        ],
        ["value"]
    )

    actual = word_count_transformer.remove_special_characters("value",no_special_characters)
    expected = SPARK.createDataFrame(
        [
            ["Hello World's "]
        ],
        ["value"]
    )

    assert expected.collect() == actual.collect()

def test_word_count() -> None:
    input_df = SPARK.createDataFrame(
        [
            ["Hello World's!"]
        ],
        ["value"]
    )

    expected = SPARK.createDataFrame(
        [
            ["Hello",1],
            ["World's",1]
        ],
        ["word","count"]
    )

    actual = word_count_transformer.word_count(input_df)

    assert expected.collect() == actual.collect()