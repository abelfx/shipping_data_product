import os
import csv
from datetime import datetime
from pathlib import Path
from loguru import logger
from ultralytics import YOLO

IMAGE_ROOT = Path("data/raw/images")
OUTPUT_ROOT = Path("data/raw/yolo_detections")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

MODEL_PATH = "yolov8n.pt"
PRODUCT_CLASSES = {"bottle", "cup", "vase"}
PERSON_CLASS = "person"


def classify_image(detected_classes: set) -> str:
    has_person = PERSON_CLASS in detected_classes
    has_product = any(cls in PRODUCT_CLASSES for cls in detected_classes)
    if has_person and has_product:
        return "promotional"
    if has_product and not has_person:
        return "product_display"
    if has_person and not has_product:
        return "lifestyle"
    return "other"


def iter_image_files(root: Path):
    for channel_dir in root.iterdir():
        if channel_dir.is_dir():
            for img in channel_dir.glob("*.*"):
                if img.suffix.lower() in {".jpg", ".jpeg", ".png"}:
                    yield channel_dir.name, img


def main():
    logger.add(Path("logs") / "yolo_detect.log", rotation="1 MB")
    logger.info("Loading YOLO model: {}", MODEL_PATH)
    model = YOLO(MODEL_PATH)

    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    output_dir = OUTPUT_ROOT / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "detections.csv"

    total_images = 0
    total_rows = 0

    with output_csv.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "message_id",
                "channel_name",
                "image_path",
                "detected_class",
                "confidence",
                "image_category",
            ],
        )
        writer.writeheader()

        for channel_name, img_path in iter_image_files(IMAGE_ROOT):
            total_images += 1
            try:
                message_id_str = img_path.stem
                # message_id might include extra tokens; best-effort int conversion
                try:
                    message_id = int(message_id_str)
                except ValueError:
                    message_id = None

                results = model.predict(source=str(img_path), conf=0.25, verbose=False)
                detected_set = set()

                for r in results:
                    names = r.names
                    for b in r.boxes:
                        cls_idx = int(b.cls.item()) if hasattr(b.cls, "item") else int(b.cls)
                        conf = float(b.conf.item()) if hasattr(b.conf, "item") else float(b.conf)
                        cls_name = names.get(cls_idx, str(cls_idx))
                        detected_set.add(cls_name)
                        writer.writerow(
                            {
                                "message_id": message_id,
                                "channel_name": channel_name,
                                "image_path": str(img_path),
                                "detected_class": cls_name,
                                "confidence": round(conf, 4),
                                "image_category": classify_image(detected_set),
                            }
                        )
                        total_rows += 1

                # If no detections, still write a summary row
                if not detected_set:
                    writer.writerow(
                        {
                            "message_id": message_id,
                            "channel_name": channel_name,
                            "image_path": str(img_path),
                            "detected_class": "",
                            "confidence": 0.0,
                            "image_category": classify_image(detected_set),
                        }
                    )
                    total_rows += 1

            except Exception as e:
                logger.exception("Detection failed for {}: {}", img_path, e)

    logger.info("Processed {} images; wrote {} rows to {}", total_images, total_rows, output_csv)


if __name__ == "__main__":
    main()
