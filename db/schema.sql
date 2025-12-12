PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS Segmentation;
DROP TABLE IF EXISTS Annotation;
DROP TABLE IF EXISTS Category;
DROP TABLE IF EXISTS Image;
DROP TABLE IF EXISTS Dataset;

CREATE TABLE Dataset (
    dataset_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL UNIQUE,     -- 'COCO', 'VOC2007', 'OpenImagesV7'
    version      TEXT,
    description  TEXT
);

CREATE TABLE Image (
    image_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id   INTEGER NOT NULL,
    external_id  TEXT NOT NULL,            -- COCO id, VOC filename, OI ImageID
    width        INTEGER,
    height       INTEGER,
    file_path    TEXT,
    split        TEXT,                     -- 'train', 'val', 'test'

    FOREIGN KEY (dataset_id) REFERENCES Dataset(dataset_id)
);

CREATE UNIQUE INDEX idx_image_dataset_external
    ON Image(dataset_id, external_id);

CREATE TABLE Category (
    category_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id    INTEGER NOT NULL,
    name          TEXT NOT NULL,
    supercategory TEXT,
    external_id   TEXT,

    FOREIGN KEY (dataset_id) REFERENCES Dataset(dataset_id)
);

CREATE INDEX idx_category_dataset_name
    ON Category(dataset_id, name);

CREATE TABLE Annotation (
    annotation_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    image_id       INTEGER NOT NULL,
    category_id    INTEGER NOT NULL,
    bbox_xmin      REAL,
    bbox_ymin      REAL,
    bbox_width     REAL,
    bbox_height    REAL,
    area           REAL,
    is_crowd       INTEGER,
    difficulty     INTEGER,
    source_info    TEXT,

    FOREIGN KEY (image_id) REFERENCES Image(image_id),
    FOREIGN KEY (category_id) REFERENCES Category(category_id)
);

CREATE INDEX idx_annotation_image
    ON Annotation(image_id);

CREATE INDEX idx_annotation_category
    ON Annotation(category_id);

CREATE TABLE Segmentation (
    segmentation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    annotation_id   INTEGER NOT NULL,
    format          TEXT,
    data            TEXT,

    FOREIGN KEY (annotation_id) REFERENCES Annotation(annotation_id)
);