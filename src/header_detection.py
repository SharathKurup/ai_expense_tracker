import re

def normalize_headers(header: str) -> str:
    if not header:
        return ""
    header = header.lower().strip()
    header = re.sub(r"\(cid:\d+\)", " ",header)
    header = re.sub(r"\s+", " ", header)
    header = re.sub(r"[^a-z0-9 ]", "", header)
    # header = re.sub(r'[^A-Za-z0-9 ]', " ", header)
    return header.strip()

def detect_column_map(header_row, header_config):
    col_map = {}
    for idx, cell in enumerate(header_row):
        if not cell:
            continue
        cell_lower = normalize_headers(cell)
        for key, aliases in header_config.items():
            if cell_lower in [a.lower() for a in aliases]:
                col_map[key] = idx
    return col_map