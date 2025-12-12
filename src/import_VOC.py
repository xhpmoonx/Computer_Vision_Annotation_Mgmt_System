import sqlite3
import xml.etree.ElementTree as ET
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "db" / "cv_datasets.db"

# Root of the VOC2007 dataset (contains Annotations, JPEGImages, ImageSets)
DATA_DIR = PROJECT_ROOT / "data" / "voc" / "VOC2007"

VOC_DATASET_NAME = "VOC2007"   # must match the name in your Dataset table

# ImageSets files for splits
IMAGESETS_MAIN = DATA_DIR / "ImageSets" / "Main"
ANNOTATIONS_DIR = DATA_DIR / "Annotations"
IMAGES_DIR = DATA_DIR / "JPEGImages"


def get_voc_dataset_id(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        "SELECT dataset_id FROM Dataset WHERE name = ?",
        (VOC_DATASET_NAME,),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(
            f"No Dataset row found with name='{VOC_DATASET_NAME}'. "
            "Make sure you ran init_db.py and seeded the Dataset table."
        )
    return row[0]


def load_split_lists():
    """
    Read train/val/test image ids from ImageSets/Main.
    Returns dict: image_id (str) -> split ('train'/'val'/'test')
    """
    splits = {}

    def add_split(split_name: str, filename: str):
        path = IMAGESETS_MAIN / filename
        if not path.exists():
            print(f"[WARN] Missing split file: {path} (skipping {split_name})")
            return
        with path.open("r") as f:
            for line in f:
                img_id = line.strip()
                if not img_id:
                    continue
                # if an id appears in multiple files, keep the first split encountered
                splits.setdefault(img_id, split_name)

    add_split("train", "train.txt")
    add_split("val", "val.txt")
    add_split("test", "test.txt")

    return splits


def parse_annotation_xml(xml_path: Path):
    """
    Parse a VOC XML file.
    Returns:
      width, height, objects
    where objects is a list of dicts:
      { 'name', 'xmin', 'ymin', 'xmax', 'ymax', 'difficult', 'truncated', 'pose' }
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    size = root.find("size")
    width = int(size.find("width").text)
    height = int(size.find("height").text)

    objects = []
    for obj in root.findall("object"):
        name = obj.find("name").text
        difficult = int(obj.find("difficult").text or 0)
        truncated_el = obj.find("truncated")
        truncated = int(truncated_el.text) if truncated_el is not None else 0
        pose_el = obj.find("pose")
        pose = pose_el.text if pose_el is not None else ""

        bndbox = obj.find("bndbox")
        xmin = int(float(bndbox.find("xmin").text))
        ymin = int(float(bndbox.find("ymin").text))
        xmax = int(float(bndbox.find("xmax").text))
        ymax = int(float(bndbox.find("ymax").text))

        objects.append(
            {
                "name": name,
                "xmin": xmin,
                "ymin": ymin,
                "xmax": xmax,
                "ymax": ymax,
                "difficult": difficult,
                "truncated": truncated,
                "pose": pose,
            }
        )

    return width, height, objects


def main():
    # basic checks
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"VOC2007 directory not found: {DATA_DIR}")
    if not ANNOTATIONS_DIR.exists() or not IMAGES_DIR.exists():
        raise FileNotFoundError("Annotations or JPEGImages directory missing under VOC2007.")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")

    dataset_id = get_voc_dataset_id(conn)
    print(f"Using dataset_id={dataset_id} for VOC2007.")

    # 1) load split mapping
    imgid_to_split = load_split_lists()
    print(f"Loaded {len(imgid_to_split)} image ids with split info.")

    # 2) walk all annotation XMLs
    xml_files = sorted(ANNOTATIONS_DIR.glob("*.xml"))
    print(f"Found {len(xml_files)} annotation XML files.")

    category_name_to_id = {}  # 'person' -> category_id
    image_count = 0
    ann_count = 0

    with conn:
        for xml_path in xml_files:
            img_id = xml_path.stem  # e.g. "000001"
            split = imgid_to_split.get(img_id, None)
            # if you only want images that appear in some split, skip those without split
            if split is None:
                # uncomment this line if you want to skip unsplit images
                # continue
                split = "train"  # or default

            width, height, objects = parse_annotation_xml(xml_path)

            # insert Image row
            file_path = str((IMAGES_DIR / f"{img_id}.jpg").relative_to(PROJECT_ROOT))
            cur = conn.execute(
                """
                INSERT INTO Image (dataset_id, external_id, width, height, file_path, split)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (dataset_id, img_id, width, height, file_path, split),
            )
            image_pk = cur.lastrowid
            image_count += 1

            # insert Annotations & Categories
            for obj in objects:
                cat_name = obj["name"]
                if cat_name not in category_name_to_id:
                    cur = conn.execute(
                        """
                        INSERT INTO Category (dataset_id, name, supercategory, external_id)
                        VALUES (?, ?, ?, ?)
                        """,
                        (dataset_id, cat_name, None, cat_name),
                    )
                    category_name_to_id[cat_name] = cur.lastrowid

                category_id = category_name_to_id[cat_name]

                xmin = obj["xmin"]
                ymin = obj["ymin"]
                xmax = obj["xmax"]
                ymax = obj["ymax"]

                bbox_xmin = float(xmin)
                bbox_ymin = float(ymin)
                bbox_width = float(xmax - xmin)
                bbox_height = float(ymax - ymin)
                area = bbox_width * bbox_height

                source_info = f"truncated={obj['truncated']};pose={obj['pose']}"

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
                        None,                      # is_crowd (not used in VOC)
                        obj["difficult"],          # difficulty flag
                        source_info,
                    ),
                )
                ann_count += 1

    conn.close()
    print(
        f"Done. Inserted {image_count} VOC images, "
        f"{len(category_name_to_id)} categories, {ann_count} annotations."
    )


if __name__ == "__main__":
    main()