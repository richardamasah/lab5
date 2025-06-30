from etl_scripts.helpers import clean_product_name

def test_clean_product_name_normal():
    assert clean_product_name("  fresh apple  ") == "Fresh Apple"

def test_clean_product_name_empty():
    assert clean_product_name(" ") == ""

def test_clean_product_name_none():
    assert clean_product_name(None) is None
