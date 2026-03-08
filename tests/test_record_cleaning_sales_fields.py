from record_cleaning import normalize_record


def test_normalize_record_extracts_sales_fields():
    row = normalize_record(
        {
            "property_type": "Town House",
            "land_size": "405 sqm",
            "building_size": "180 sqm",
        }
    )

    assert row["property_category"] == "townhouse"
    assert row["land_area"] == 405.0
    assert row["land_area_unit"] == "sqm"
    assert row["building_area"] == 180.0
    assert row["building_area_unit"] == "sqm"
