import pytest
from utils.transform import snake_case  # Adjust import if needed

@pytest.mark.parametrize("input_str,expected", [
    ("Patients’ rating of the facility linear mean score", "patients_rating_of_the_facility_linear_mean_score"),
    ("  Provider (Name)  ", "provider_name"),
    ("Discharge-Ready? Yes/No", "discharge_ready_yes_no"),
    ("Hospital's Overall Rating", "hospitals_overall_rating"),
    ("Patient_ID", "patient_id"),
    ("(Special) Notes", "special_notes"),
    ("  Multiple    Spaces ", "multiple_spaces"),
    ("", ""),  # edge: empty input
    ("Already_snake_case", "already_snake_case"),
])
def test_snake_case(input_str, expected):
    assert snake_case(input_str) == expected