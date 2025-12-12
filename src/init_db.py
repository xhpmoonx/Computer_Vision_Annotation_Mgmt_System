import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "db" / "cv_datasets.db"
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def init_db():
    print(f"Creating database at: {DB_PATH}")

    # open a new connection (this also creates the file if it doesn't exist)
    conn = sqlite3.connect(DB_PATH)

    # read the schema.sql file
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        schema = f.read()

    # execute the schema
    conn.executescript(schema)
    conn.commit()
    conn.close()

    print("Database schema created successfully!")


def seed_datasets():
    """Insert the three dataset names into the Dataset table."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    datasets = [
        ("COCO", "2017", "COCO 2017 detection dataset"),
        ("VOC2007", "2007", "PASCAL VOC 2007 dataset"),
        ("OpenImagesV7", "v7", "OpenImages v7 boxable subset")
    ]

    for name, version, description in datasets:
        cur.execute(
            """INSERT OR IGNORE INTO Dataset (name, version, description)
               VALUES (?, ?, ?)""",
            (name, version, description)
        )

    conn.commit()
    conn.close()

    print("Dataset table seeded!")


if __name__ == "__main__":
    init_db()
    seed_datasets()