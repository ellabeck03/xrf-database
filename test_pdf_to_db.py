"""
test_pdf_to_db.py

test files for pdf_to_db.py

Ella Beck
01/07/25
"""

import os
import pandas as pd
import pytest

import pdf_to_db as p2d


class FakePage:
    """
    creates a fake results page to be used for testing
    """
    def __init__(self, text):
        self._text = text

    def extract_text(self):
        return self._text


@pytest.fixture
def sample_lines():
    "mimic a page text with header and two lines of results"

    header = "Name Sample1 Class Alloy_LE_FP 01/07/2025 Time 14:55:28" \
        "Duration 30s"
    element1 = "Element Bi %  Pb %"
    pct1 = "99.5 0.5"
    unc1 = "± 0.1 0.05"
    element2 = "Element Zr %  Se %"
    pct2 = "0.2  0.05"
    unc2 = "± 0.02 0.01"
    lines = [header, element1, pct1, unc1, element2, pct2, unc2]

    return lines


def test_whitespace_stripping():
    """
    tests that extract_lines strips the data of excess whitespace and blank
    entries
    """

    text = " line1 \n\n line2\n"
    page = FakePage(text)
    lines = p2d.extract_lines(page)

    assert lines == ["line1", "line2"]


def test_find_section_indices(sample_lines):
    """
    tests that the header is stored in index 0, and the data entries in index
    1
    """
    lines = sample_lines
    label_idx, element_idx = p2d.find_section_indices(lines)

    assert label_idx == 0
    assert element_idx == 1


def test_find_section_indices_fail():
    """
    tests that the code catches when the header isn't found
    """
    lines = ["not a header", "not a header either"]
    label_idx, element_idx = p2d.find_section_indices(lines)

    assert label_idx is None and element_idx is None


def test_extract_sample_info_valid():
    "tests that valid data is extracted successfully"

    tokens = ["Sample1", "Alloy_LE_FP", "01/07/2025", "14:55:28", "30"]
    name, date, duration = p2d.extract_sample_info(tokens)

    assert name == "Sample1"
    assert date == "01/07/2025"
    assert duration == 30.0


def test_extract_sample_info_missing_tokens():
    "tests that code fails when not all header tokens are present"

    tokens = ["bad", "tokens"]

    with pytest.raises(ValueError):
        p2d.extract_sample_info(tokens)


def test_parse_measurements_all_readings(sample_lines):
    "tests that all measurements of sample_lines are found successfully"

    measurements = p2d.parse_measurements(sample_lines, element_idx=1,
                                          duration=30)

    syms = [m["ElementSymbol"] for m in measurements]
    assert syms == ["Bi", "Pb", "Zr", "Se"]

    pcts = [m["Percentage Composition"] for m in measurements]
    assert all(isinstance(p, float) for p in pcts)


def test_prompt_pages_single(monkeypatch):
    "tests that the right page is called from user input"

    monkeypatch.setattr('builtins.input', lambda _: "3")
    pages = p2d.prompt_pages()

    assert pages == [3]


def test_prompt_pages_multiple(monkeypatch):
    "tests that the right range of pages is called from user input"

    monkeypatch.setattr('builtins.input', lambda _: "1-3, 5")
    pages = p2d.prompt_pages()

    assert pages == [1, 2, 3, 5]


def test_save_dataframe_creates_files(tmp_path):
    df_samples = pd.DataFrame([{"SampleID": 0}])
    df_measurements = pd.DataFrame([{"ElementSymbol": "Bi"}])

    prefix = tmp_path / "test"

    p2d.save_dataframe(df_samples, df_measurements, str(prefix))

    # check files exist
    sample_name = f"{prefix}_samples.csv"
    measurements_name = f"{prefix}_measurements.csv"

    assert os.path.exists(sample_name)
    assert os.path.exists(measurements_name)

    # check contents of files
    sample_data = pd.read_csv(sample_name)
    measurements_data = pd.read_csv(measurements_name)

    assert "SampleID" in sample_data.columns
    assert "ElementSymbol" in measurements_data.columns
