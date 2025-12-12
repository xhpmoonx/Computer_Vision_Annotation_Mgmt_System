import csv
import sqlite3
from pathlib import Path

# -------- settings --------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "db" / "cv_datasets.db"

# adjust this if your files are in a different place
DATA_DIR = PROJECT_ROOT / "data" / "Openimages"

TARGET_IMAGE_COUNT = 17125   # or any number you want to sample

BOX_FILES = {
    "train":      DATA_DIR / "train-annotations-bbox.csv",
    "validation": DATA_DIR / "validation-annotations-bbox.csv",
    "test":       DATA_DIR / "test-annotations-bbox.csv",
}

IMAGE_INFO_FILES = {
    "train":      DATA_DIR / "train-images-boxable-with-rotation.csv",
    "validation": DATA_DIR / "validation-images-with-rotation.csv",
    "test":       DATA_DIR / "test-images-with-rotation.csv",
}

CLASS_DESCRIPTIONS = DATA_DIR / "oidv7-class-descriptions-boxable.csv"
OI_DATASET_NAME = "OpenImagesV7"   # must match the name in Dataset table
# --------------------------


def read_class_names(path: Path):
    """
    Read class descriptions CSV.
    Handles both:
      - LabelName,DisplayName
      - LabelMID,DisplayName
    """
    mid_to_name = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        first_row = next(reader, None)
        if first_row is None:
            return mid_to_name  # empty file

        # detect header
        header_like = any("label" in c.lower() or "display" in c.lower() for c in first_row)
        if header_like:
            # treat as header and switch to DictReader
            f.seek(0)
            dict_reader = csv.DictReader(f)
            # try different key names
            for row in dict_reader:
                label = row.get("LabelName") or row.get("LabelMID") or row.get("Label")
                display = row.get("DisplayName") or row.get("Display")
                if label and display:
                    mid_to_name[label] = display
        else:
            # no header, just two columns per row
            # first row was actual data
            if first_row and len(first_row) >= 2:
                mid_to_name[first_row[0]] = first_row[1]
            for row in reader:
                if not row or len(row) < 2:
                    continue
                mid_to_name[row[0]] = row[1]

    return mid_to_name


def iter_image_info(paths_by_split):
    """
    Iterate image info rows across splits.
    Tries to find the ImageID column even if the header name is slightly different.
    """
    for split, path in paths_by_split.items():
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # figure out which column holds the image id
            fieldnames = [fn.strip() for fn in reader.fieldnames or []]

            # try common variants
            id_key = None
            for candidate in ("ImageID", "ImageId", "image_id", "imageID"):
                for fn in fieldnames:
                    if fn == candidate:
                        id_key = fn
                        break
                if id_key:
                    break

            if id_key is None:
                raise KeyError(
                    f"No ImageID-like column found in {path}. "
                    f"Available columns: {fieldnames}"
                )

            for row in reader:
                # make sure we access the correct key as it appears in the CSV
                yield split, row[id_key], row


def iter_boxes(paths_by_split):
    """
    Iterate bbox annotations across splits.
    CSV columns include:
      ImageID,Source,LabelName,Confidence,XMin,XMax,YMin,YMax,
      IsOccluded,IsTruncated,IsGroupOf,IsDepiction,IsInside,...
    """
    for split, path in paths_by_split.items():
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield split, row


def choose_images(image_info_iter, limit):
    """
    Pick up to `limit` distinct images across splits.
    Returns dict: image_id -> (split, file_url)
    """
    chosen = {}
    for split, img_id, row in image_info_iter:
        if img_id in chosen:
            continue
        # prefer the smaller thumbnail if available; else fall back to OriginalURL
        file_url = row.get("Thumbnail300KURL") or row.get("OriginalURL")
        if not file_url:
            continue
        chosen[img_id] = (split, file_url)
        if len(chosen) >= limit:
            break
    return chosen


def get_openimages_dataset_id(conn: sqlite3.Connection) -> int:
    """
    Look up the dataset_id for OpenImagesV7 in the unified Dataset table.
    """
    cur = conn.execute(
        "SELECT dataset_id FROM Dataset WHERE name = ?",
        (OI_DATASET_NAME,),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(
            f"No Dataset row found with name='{OI_DATASET_NAME}'. "
            "Make sure you ran init_db.py and seeded the Dataset table."
        )
    return row[0]


def main():
    # -- 1) Sanity check files exist --
    required_files = [CLASS_DESCRIPTIONS, *BOX_FILES.values(), *IMAGE_INFO_FILES.values()]
    missing = [str(p) for p in required_files if not p.exists()]
    if missing:
        raise FileNotFoundError("Missing required CSV files:\n" + "\n".join(missing))

    # -- 2) Load class MID -> DisplayName mapping --
    print("Reading class descriptions...")
    mid_to_name = read_class_names(CLASS_DESCRIPTIONS)
    print(f"Loaded {len(mid_to_name)} class names.")

    # -- 3) Choose which images we will import (sample) --
    print(f"Selecting up to {TARGET_IMAGE_COUNT} images...")
    picked = choose_images(iter_image_info(IMAGE_INFO_FILES), TARGET_IMAGE_COUNT)
    print(f"Selected {len(picked)} images.")

    # -- 4) Connect to unified DB & look up dataset_id for OpenImagesV7 --
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    dataset_id = get_openimages_dataset_id(conn)
    print(f"Using dataset_id={dataset_id} for OpenImagesV7.")

    # -- 5) Insert Images into unified Image table --
    imageid_to_pk = {}      # OI ImageID -> Image.image_id PK

    with conn:
        print("Inserting Image rows...")
        for oid, (split, url) in picked.items():
            # width/height unknown from these CSVs, keep them NULL for now
            cur = conn.execute(
                """
                INSERT INTO Image (dataset_id, external_id, width, height, file_path, split)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (dataset_id, oid, None, None, url, split),
            )
            image_pk = cur.lastrowid
            imageid_to_pk[oid] = image_pk

    print(f"Inserted {len(imageid_to_pk)} images.")

    # -- 6) Insert Categories & Annotations --
    mid_to_category_id = {}   # LabelMID -> Category.category_id
    num_annotations = 0

    with conn:
        print("Inserting Category and Annotation rows...")
        for split, row in iter_boxes(BOX_FILES):
            oid = row["ImageID"]
            image_pk = imageid_to_pk.get(oid)
            if image_pk is None:
                # we didn't select this image in 'picked'
                continue

            mid = row["LabelName"]   # e.g. "/m/01g317"
            # 6a) ensure Category exists for this MID
            if mid not in mid_to_category_id:
                display_name = mid_to_name.get(mid, mid)
                cur = conn.execute(
                    """
                    INSERT INTO Category (dataset_id, name, supercategory, external_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    (dataset_id, display_name, None, mid),
                )
                mid_to_category_id[mid] = cur.lastrowid

            category_id = mid_to_category_id[mid]

            # 6b) bbox: OpenImages uses normalized [0,1] coords
            xmin = float(row["XMin"])
            xmax = float(row["XMax"])
            ymin = float(row["YMin"])
            ymax = float(row["YMax"])

            bbox_xmin = xmin
            bbox_ymin = ymin
            bbox_width = xmax - xmin
            bbox_height = ymax - ymin
            area = bbox_width * bbox_height

            # 6c) store some extra flags as a simple string in source_info
            flags = [
                f"IsOccluded={row.get('IsOccluded', '')}",
                f"IsTruncated={row.get('IsTruncated', '')}",
                f"IsGroupOf={row.get('IsGroupOf', '')}",
                f"IsDepiction={row.get('IsDepiction', '')}",
                f"IsInside={row.get('IsInside', '')}",
            ]
            source_info = ";".join(flags)

            conn.execute(
                """
                INSERT INTO Annotation
                    (image_id, category_id,
                     bbox_xmin, bbox_ymin, bbox_width, bbox_height,
                     area, is_crowd, difficulty, source_info)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    image_pk,
                    category_id,
                    bbox_xmin,
                    bbox_ymin,
                    bbox_width,
                    bbox_height,
                    area,
                    None,      # is_crowd (not used in OpenImages)
                    None,      # difficulty (not used in OpenImages)
                    source_info,
                ),
            )
            num_annotations += 1

    conn.close()
    print(f"Done. Inserted {len(mid_to_category_id)} categories and {num_annotations} annotations into unified DB.")


if __name__ == "__main__":
    main()