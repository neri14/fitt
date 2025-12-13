import pytest
import os
from pathlib import Path
from fitt.tools.verify import main as verify

@pytest.fixture
def test_data_dir():
    return Path(__file__).parent / "data"


@pytest.fixture
def sample_fit_file(test_data_dir):
    path = test_data_dir / "sample.fit"
    if not path.exists():
        pytest.skip(f"Sample FIT file not found: {path}")
    return str(path)


@pytest.fixture
def invalid_fit_file(test_data_dir):
    path = test_data_dir / "invalid.fit"
    if not path.exists():
        pytest.skip(f"Invalid FIT file not found: {path}")
    return str(path)


def test_verification_with_valid_fit_file(sample_fit_file):
    result = verify(sample_fit_file)
    assert result is True, "Verification should succeed for a valid FIT file."


def test_verification_with_invalid_fit_file(invalid_fit_file):
    result = verify(invalid_fit_file)
    assert result is False, "Verification should fail for an invalid FIT file."


def test_main_with_nonexistent_fit_file():
    result = verify("nonexistent.fit")
    assert result is False, "Verification should fail for a nonexistent FIT file."
