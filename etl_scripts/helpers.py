def clean_product_name(name: str) -> str:
    if name is None:
        return None
    return name.strip().title()
