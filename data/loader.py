from pathlib import Path

import kagglehub

DST_FILE_PATH = Path(__file__).parent / "nyc_taxi_raw.csv"
LINES_LIMIT = 50000

print("Downloading dataset...")
src_file_path = (
    Path(kagglehub.dataset_download("raminhuseyn/new-york-city-taxi-and-limousine-project"))
    / "New York City TLC Data.csv"
)
print("Dataset downloaded:", src_file_path)

with (
    src_file_path.open("r", encoding="utf-8") as src,
    DST_FILE_PATH.open("w", encoding="utf-8") as dst,
):
    for i, line in enumerate(src):
        if i >= LINES_LIMIT:
            break
        dst.write(line)
print("Dataset added to the repo")
