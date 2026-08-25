from foundation_provider_acquisition.vworld_credentials import (
    normalize_vworld_credentials,
    resolve_vworld_credential,
)


def test_canonical_vworld_credential_wins_without_a_warning() -> None:
    warnings: list[str] = []

    value = resolve_vworld_credential(
        {
            "FOUNDATION_PLATFORM_VWORLD_USERNAME": "canonical-user",
            "VWORLD_USERNAME": "legacy-user",
        },
        "username",
        warn=warnings.append,
    )

    assert value == "canonical-user"
    assert warnings == []


def test_deprecated_vworld_credential_alias_is_accepted_with_names_only_warning() -> None:
    warnings: list[str] = []

    value = resolve_vworld_credential(
        {"FOUNDATION_PLATFORM_VWORLD_DATASET_PASSWORD": "secret-value"},
        "password",
        warn=warnings.append,
    )

    assert value == "secret-value"
    assert warnings == [
        "deprecated environment variable FOUNDATION_PLATFORM_VWORLD_DATASET_PASSWORD supplied "
        "the value; use FOUNDATION_PLATFORM_VWORLD_PASSWORD"
    ]


def test_normalization_keeps_canonical_value_and_removes_every_alias() -> None:
    warnings: list[str] = []

    normalized = normalize_vworld_credentials(
        {
            "FOUNDATION_PLATFORM_VWORLD_USERNAME": "canonical-user",
            "FOUNDATION_PLATFORM_VWORLD_DATASET_USERNAME": "dataset-user",
            "VWORLD_USERNAME": "legacy-user",
        },
        warn=warnings.append,
    )

    assert normalized["FOUNDATION_PLATFORM_VWORLD_USERNAME"] == "canonical-user"
    assert "FOUNDATION_PLATFORM_VWORLD_DATASET_USERNAME" not in normalized
    assert "VWORLD_USERNAME" not in normalized
    assert warnings == []


def test_normalization_promotes_first_alias_and_warns_names_only() -> None:
    warnings: list[str] = []

    normalized = normalize_vworld_credentials(
        {"VWORLD_PASSWORD": "secret-value"},
        warn=warnings.append,
    )

    assert normalized["FOUNDATION_PLATFORM_VWORLD_PASSWORD"] == "secret-value"
    assert "VWORLD_PASSWORD" not in normalized
    assert warnings == [
        "deprecated environment variable VWORLD_PASSWORD supplied the value; "
        "use FOUNDATION_PLATFORM_VWORLD_PASSWORD"
    ]
