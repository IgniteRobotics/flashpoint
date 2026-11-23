def check(version: str, folder: str) -> str:
    if version < "3.11":
        return f"Python {version} isn't supported (3.11 to 3.14)"
    return f"no robot configured in {folder}, so the lake's motors aren't matched to slots"
