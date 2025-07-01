"""
pdf_to_db.py

Converts XRF outputs into pandas dataframes.

Ella Beck
01/07/25
"""

import pdfplumber
import logging
from pdfminer import settings as pdfminer_settings
import pandas as pd

# silence pdfminer/pdfplumber warnings
pdfminer_settings.STRICT = False
logging.getLogger("pdfminer").setLevel(logging.ERROR)
logging.getLogger("pdfplumber").setLevel(logging.ERROR)

PDF_PATH = "report.pdf"


def prompt_user(sample_name, field_name, cast=str):
    "prompt the user for (optional) additional information"

    val = input(f"{sample_name}: Add value for {field_name} "
                "(leave blank for NULL): ").strip()

    return None if val == "" else cast(val)


def extract_lines(page):
    "extract and clean non-empty lines from the results pdf"

    text = page.extract_text() or ''

    return [line.strip() for line in text.splitlines() if line.strip()]


def find_section_indices(lines):
    """
    finds indices of header and element sections in the results
    returns (label_idx, element_idx) or (None, None) if not found
    """
    label_idx = next((i for i, ln in enumerate(lines)
                      if ln.startswith("Name") and "Duration" in ln), None)
    element_idx = None
    if label_idx is not None:
        element_idx = next((j for j in range(label_idx + 1, len(lines))
                            if lines[j].startswith("Element")), None)

    return label_idx, element_idx


def extract_sample_info(tokens, class_token="Alloy_LE_FP"):
    "given tokenised header values, extract sample name, date, duration"\

    if class_token not in tokens:
        raise ValueError(f"class token '{class_token}' missing")

    class_index = tokens.index(class_token)

    name = (
        " ".join(tokens[:class_index]) + " " + " ".join(tokens[class_index+5:])
        if len(tokens) > class_index + 4 else " ".join(tokens[:class_index])
    )

    date = tokens[class_index + 1]
    duration = float(tokens[class_index + 3])

    return name, date, duration


def collect_sample_fields(sample_name):
    """
    prompt user for typed fields for a sample
    returns a dict of field values
    """

    return {
        "Mass_g": prompt_user(sample_name, "Mass_g", float),
        "Length_mm": prompt_user(sample_name, "Length_mm", float),
        "Width_mm": prompt_user(sample_name, "Width_mm", float),
        "Height_mm": prompt_user(sample_name, "Height_mm", float),
        "Notes": prompt_user(sample_name, "Notes", str),
        "Description": prompt_user(sample_name, "Description", str)
    }


def preview_sample(sample_info):
    """
    print a preview of sample information and confirm with user
    returns true if user proceeds, false otherwise
    """
    print("\n=== sample preview ===")
    for label, value in sample_info.items():
        print(f"{label:<12}: {value}")

    return input("proceed with this sample? "
                 "[y/n] ").strip().lower() == 'y'


def parse_measurements(lines, element_idx, duration):
    "returns a list of dicts for each measurement"
    measurements = []

    for i in range(element_idx, len(lines)):
        if not lines[i].startswith("Element"):
            continue

        parts = lines[i].split()
        symbols = [parts[m] for m in range(1, len(parts), 2)]
        pct_line = lines[i+1].split()
        unc_tokens = lines[i+2].lstrip("±").split()

        for sym, pct_txt, unc_txt in zip(symbols, pct_line, unc_tokens):
            if pct_txt.upper() == "ND":
                continue

            measurements.append({
                "ElementSymbol": sym,
                "Percentage Composition": float(pct_txt),
                "Uncertainty": None if unc_txt.upper() == "ND" else
                float(unc_txt),
                "Measurement_Length_s": duration
            })

    return measurements


def preview_measurements(measurements):
    """
    prints a preview of measurements and confirms with the user
    returns true if user proceeds, false otherwise
    """

    print("\n=== measurements preview ===")

    for i, m in enumerate(measurements, start=1):
        print(f"{i:2}: {m['ElementSymbol']:3} ",
              f"| %={m['Percentage Composition']:<6} ",
              f"| unc={m['Uncertainty']:<6} ",
              f"length_s={m['Measurement_Length_s']}")

    return input("proceed with measurements? [y/n]").strip().lower() == 'y'


def parse_page(page, sample_records, measurement_records, prompt_user=True):
    lines = extract_lines(page)
    label_idx, element_idx = find_section_indices(lines)

    if label_idx is None or element_idx is None:
        print("  → required sections not found; skipping page")
        return False

    tokens = " ".join(lines[label_idx+1:element_idx]).split()
    try:
        name, date, duration = extract_sample_info(tokens)
    except ValueError as e:
        print(f"  → {e}; skipping page")
        return False

    user_data = collect_sample_fields(name)
    sample_info = {
        "SampleID": len(sample_records),
        "SampleName": name,
        "CollectionDate": date,
        **user_data,
        "Duration_s": duration
    }

    if prompt_user and not preview_sample(sample_info):
        return False

    sample_records.append(sample_info)
    measurements = parse_measurements(lines, element_idx, duration)

    if prompt_user and not preview_measurements(measurements):
        sample_records.pop()
        return False

    for m in measurements:
        m["SampleID"] = sample_info["SampleID"]
        measurement_records.append(m)

    print(f"  → committed '{name}' with {len(measurements)} measurements")
    return True


def prompt_pages():
    "prompt the user for which pages to add to the database"

    page_input = input("enter pages/ranges (e.g. 1-3, 5): ").strip()
    pages = set()

    for part in page_input.split(","):
        if "-" in part:
            a, b = map(int, part.split("-", 1))
            pages.update(range(a, b+1))
        else:
            pages.add(int(part))

    return sorted(pages)


def parse_to_pdf(pdf_path, prompt_user=True):
    pages = prompt_pages()
    sample_records, measurement_records = [], []

    with pdfplumber.open(pdf_path) as pdf:
        for p in pages:
            if p < 1 or p > len(pdf.pages):
                print(f"page {p} out of range; skipping")
                continue

            try:
                parse_page(pdf.pages[p-1], sample_records, measurement_records,
                           prompt_user)
            except Exception as e:
                print(f"error on page {p}: {e}")

    return pd.DataFrame(sample_records), pd.DataFrame(measurement_records)


def save_dataframe(df_samples, df_measurements, prefix):
    "saves both dataframes to csv files"

    samples_name = f"{prefix}_samples.csv"
    measurements_name = f"{prefix}_measurements.csv"

    df_samples.to_csv(samples_name, index=False)
    df_measurements.to_csv(measurements_name, index=False)

    print(f"saved samples to working directory as {samples_name}")
    print(f"saved measurements to working directory as {measurements_name}")

    return


def save_dataframe_user_prompt(df_samples, df_measurements):
    "asks the user if they want to save the data frames"

    if input("\nsave these dataframes? [y/n] ").strip().lower() == "y":
        prefix = input("enter filename prefix (default is 'xrf'): ").strip() \
            or "xrf"
        save_dataframe(df_samples, df_measurements, prefix)

    else:
        print("skipped saving")
        return

    return


if __name__ == "__main__":
    df_samples, df_measurements = parse_to_pdf(PDF_PATH)
    print("\nSamples DataFrame Preview:")
    print(df_samples.head(5))
    print("\nMeasurements DataFrame Preview:")
    print(df_measurements.head(20))

    save_dataframe_user_prompt(df_samples, df_measurements)
