import json
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "db" / "cv_datasets.db"
DATA_DIR = PROJECT_ROOT / "data" / "COCO"

COCO_DATASET_NAME = "COCO"   # must match name in Dataset table

ANNOTATION_FILES = {
    "train": DATA_DIR / "instances_train2017.json",
    "val":   DATA_DIR / "instances_val2017.json",
}

def get_coco_dataset_id(conn):
    cur = conn.execute(
        "SELECT dataset_id FROM Dataset WHERE name = ?",
        (COCO_DATASET_NAME,)
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("COCO dataset not found in Dataset table.")
    return row[0]

def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def main():
    # Check files exist
    missing = [str(p) for p in ANNOTATION_FILES.values() if not p.exists()]
    if missing:
        raise FileNotFoundError("Missing COCO JSON files:\n" + "\n".join(missing))

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    coco_dataset_id = get_coco_dataset_id(conn)

    print(f"Using dataset_id={coco_dataset_id} for COCO.")

    image_count = 0
    ann_count = 0
    category_mapping = {}  # coco_category_id → category_id in DB

    with conn:
        for split, json_file in ANNOTATION_FILES.items():
            print(f"Processing {json_file}...")

            data = load_json(json_file)

            # Insert categories once
            for cat in data["categories"]:
                coco_id = cat["id"]
                if coco_id not in category_mapping:
                    cur = conn.execute(
                        """
                        INSERT INTO Category (dataset_id, name, supercategory, external_id)
                        VALUES (?, ?, ?, ?)
                        """,
                        (coco_dataset_id, cat["name"], cat["supercategory"], str(coco_id))
                    )
                    category_mapping[coco_id] = cur.lastrowid

            # Insert images
            image_id_map = {}  # COCO image id → our DB image_id
            for img in data["images"]:
                cur = conn.execute(
                    """
                    INSERT INTO Image (dataset_id, external_id, width, height, file_path, split)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (coco_dataset_id, str(img["id"]), img["width"], img["height"], None, split)
                )
                image_id_map[img["id"]] = cur.lastrowid
                image_count += 1

            # Insert annotations
            for ann in data["annotations"]:
                img_pk = image_id_map.get(ann["image_id"])
                cat_pk = category_mapping.get(ann["category_id"])

                bbox = ann["bbox"]  # [xmin, ymin, width, height]
                bbox_xmin = bbox[0]
                bbox_ymin = bbox[1]
                bbox_width = bbox[2]
                bbox_height = bbox[3]
                area = ann.get("area", bbox_width * bbox_height)

                conn.execute(
                    """
                    INSERT INTO Annotation
                        (image_id, category_id,
                         bbox_xmin, bbox_ymin,
                         bbox_width, bbox_height,
                         area, is_crowd, difficulty, source_info)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        img_pk, cat_pk,
                        bbox_xmin, bbox_ymin,
                        bbox_width, bbox_height,
                        area,
                        ann.get("iscrowd", None),
                        None,
                        None,
                    )
                )
                ann_count += 1

    conn.close()
    print(f"Inserted {image_count} COCO images, {len(category_mapping)} categories, {ann_count} annotations.")

if __name__ == "__main__":
    main()