import pikepdf
from pathlib import Path
import re
import os
from dotenv import load_dotenv

from ORG.pdfDataOrchestrator import process_single_statement

load_dotenv()

INPUT_DIR = Path(os.getenv("INPUT_DIR", "attachments/locked"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "attachments/unlocked"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PASSWORDS = os.getenv("PDF_PASSWORDS", "").split(",")

def normalize_filename(bank_name, pdf_path: Path) -> str:
    name = pdf_path.stem.lower()

    month_names = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december"
    ]

    # Create month mapping
    month_map = {}
    for i, month in enumerate(month_names, 1):
        month_map[month] = month
        month_map[month[:3]] = month
        month_map[f"{i:02d}"] = month
        month_map[str(i)] = month
    month_map["sept"] = "september"

    # Find ALL potential matches
    candidates = []

    # Pattern 1: Word-based months (highest priority)
    word_months = "|".join([m for m in month_map.keys() if not m.isdigit()])
    for match in re.finditer(rf"({word_months})[-\s]*(\d{{2,4}})", name):
        month_str = match.group(1)
        year_str = match.group(2)
        if len(year_str) == 2:
            year_str = "20" + year_str
        candidates.append((match.start(), "word", month_map[month_str], year_str))

    # Pattern 2: YYYY-MM or YYYY-MM-DD
    for match in re.finditer(r"(\d{4})[-/](\d{2})", name):
        year_str = match.group(1)
        month_num = int(match.group(2))
        if 1 <= month_num <= 12 and 2000 <= int(year_str) <= 2099:
            candidates.append((match.start(), "yyyy-mm", month_names[month_num - 1], year_str))

    # Pattern 3: MM-YYYY or MM-YY
    for match in re.finditer(r"\b(\d{2})[-/](\d{2,4})", name):
        month_num = int(match.group(1))
        year_str = match.group(2)
        if 1 <= month_num <= 12:
            if len(year_str) == 2:
                year_str = "20" + year_str
            if 2000 <= int(year_str) <= 2099:
                candidates.append((match.start(), "mm-yyyy", month_names[month_num - 1], year_str))

    # Choose best match: word-based > earlier position
    if candidates:
        # Sort by priority: word matches first, then by position
        candidates.sort(key=lambda x: (0 if x[1] == "word" else 1, x[0]))
        _, match_type, month, year = candidates[0]

        normalized = f"{bank_name}bank_statement_{month}_{year}.pdf"
        print(f"Matched ({match_type}): {month}, year: {year}")
        print(f"Normalized filename: {normalized}")
        return normalized

    print(f"No match found, keeping original: {pdf_path.name}")
    return pdf_path.name

def normalize_filename_old(bank_name, pdf_path: Path) -> str:
    name = pdf_path.stem.lower()

    month_year_match = re.search(
        r"(january|february|march|april|may|june|"
        r"july|august|september|october|november|december)"
        r"\s+(\d{4})",
        name
    )

    if month_year_match:
        month, year = month_year_match.groups()
        return f"{bank_name}bank_statement_{month}_{year}.pdf"

    date_range_match = re.search(
        r"(\d{2})-(\d{2})-(\d{4})to(\d{2})-(\d{2})-(\d{4})",
        name
    )

    if date_range_match:
        # start date defines the month
        _, month, year, *_ = date_range_match.groups()
        month_map = {
            "01": "january", "02": "february", "03": "march",
            "04": "april", "05": "may", "06": "june",
            "07": "july", "08": "august", "09": "september",
            "10": "october", "11": "november", "12": "december"
        }
        return f"{bank_name}bank_statement_{month_map[month]}_{year}.pdf"

    return pdf_path.name
    # raise ValueError(f"Cannot normalize filename: {pdf_path.name}")

def main(bank_name):

    normalized_status = []

    for pdf_file in INPUT_DIR.glob("*.pdf"):
        print(f"Processing file: {pdf_file.name}")
        pdf = None

        if not bank_name:
            _tmpFileName = pdf_file.name.split("_")
            bank_name = _tmpFileName[0]


        for pwd in PASSWORDS:
            try:
                pdf = pikepdf.open(pdf_file, password=pwd)
                print("  ✔ Password matched")
                break
            except pikepdf.PasswordError:
                continue

        if pdf is None:
            print("  ✖ Failed to unlock PDF with provided passwords")
            continue
        normalize_filename(bank_name,pdf_file)
        output_name = normalize_filename(bank_name, pdf_file)
        #output_name = pdf_file.name  # Keep original name
        output_path = OUTPUT_DIR / output_name

        if output_name == pdf_file.name:
            normalize_status={"old": pdf_file.name, "new": output_name, "renamed": "N"}
        else:
            normalize_status={"old": pdf_file.name, "new": output_name, "renamed": "Y"}
        normalized_status.append(normalize_status)

        bank_name=""
        pdf.save(output_path)
        pdf.close()

        print(f"  ✔ Saved to: {output_path}")

    for status in normalized_status:
        print(f'File: {status["old"]} -> New Name: {status["new"]} | Renamed: {status["renamed"]}')

    print("Processing completed.")

if __name__ == "__main__":
    banker=""
    main(banker)