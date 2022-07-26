def page_offset(offset: dict, _: list[dict]):
    page = offset["page"]
    return {"page": page + 1}
