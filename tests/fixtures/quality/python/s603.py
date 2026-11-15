import subprocess


def run(argv: list[str]) -> None:
    subprocess.run(argv, check=True)
