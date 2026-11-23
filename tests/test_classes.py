import pytest

from app.Classes.classes import Drug


def test_from_json_keeps_backend_id():
    drug = Drug.from_json({"Id": "D0A1B2C3-0000-4000-8000-000000000001", "Name": "Омез", "Manufacturer": "Dr. Reddy's",
                           "CountryCodeId": "IN"})
    assert drug.id == "d0a1b2c3-0000-4000-8000-000000000001"
    assert drug.name == "Омез"
    assert drug.description_source == "generated"


def test_from_json_without_id_is_rejected():
    with pytest.raises(ValueError):
        Drug.from_json({"Name": "Омез"})
