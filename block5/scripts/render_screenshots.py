"""
Рендер результатов запросов ClickHouse в PNG-"скриншоты" (терминальный стиль).
Источник — текстовые файлы результатов в results/, выход — screenshots/.
Запуск:  python scripts/render_screenshots.py
"""

import os
from PIL import Image, ImageDraw, ImageFont

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(ROOT, "results")
OUT = os.path.join(ROOT, "screenshots")

BG = (24, 26, 37)
FG = (205, 214, 244)
TITLE_BG = (49, 50, 68)
TITLE_FG = (166, 227, 161)

FONT_PATH = r"C:\Windows\Fonts\consola.ttf"

TASKS = [
    ("01_clusters.txt",         "01-clickhouse-clusters.png",  "SELECT * FROM system.clusters"),
    ("01_version.txt",          "02-clickhouse-version.png",   "SELECT version()  |  SHOW DATABASES"),
    ("02_task2_ddl.txt",        "03-task2-show-create.png",    "SHOW CREATE TABLE dwh.fact_space_events"),
    ("02_task2_select.txt",     "04-task2-select.png",         "SELECT * FROM dwh.fact_space_events (после ручных вставок)"),
    ("02_task2_parts.txt",      "05-task2-parts.png",          "system.parts: каждый INSERT создал свой парт"),
    ("03_task3_checks.txt",     "06-task3-generate.png",       "Задача 3: 5 000 000 строк за 2026 год — количество и распределения"),
    ("03_task3_parts.txt",      "07-task3-parts.png",          "Задача 3: партиции и парты после генерации"),
    ("task4/variant_A.txt",     "08-explain-A.png",            "Задача 4, вариант A: EXPLAIN indexes = 1 + system.query_log"),
    ("task4/variant_B.txt",     "09-explain-B.png",            "Задача 4, вариант B: EXPLAIN indexes = 1 + system.query_log"),
    ("task4/variant_C.txt",     "10-explain-C.png",            "Задача 4, вариант C: EXPLAIN indexes = 1 + system.query_log"),
    ("task4/variant_D.txt",     "11-explain-D.png",            "Задача 4, вариант D (итоговый): EXPLAIN indexes = 1 + system.query_log"),
    ("04_task4_parts_stats.txt","12-task4-parts-stats.png",    "Задача 4: system.parts по вариантам A/B/C/D"),
    ("task4/timings_min3.txt",  "13-task4-timings.png",        "Задача 4: минимальное время из 3 прогонов (ms | read_rows | read_bytes)"),
    ("06_block2_ddl.txt",       "14-block2-pipeline-ddl.png",  "Блок 2: DDL Kafka-таблицы, stage-таблицы и Materialized View"),
    ("06_block2_rows_after1.txt","15-block2-rows-after-simulate.png", "Блок 2: строки в stage.fact_telemetry после simulate (UPDATE: _state=-1/-1/+1)"),
    ("06_block2_counts_2runs.txt","16-block2-counts-before-after.png", "Блок 2: count() до и после 2-го запуска simulate: 1276 -> 2482"),
]


def render(txt_path, png_path, title):
    with open(os.path.join(RESULTS, txt_path), encoding="utf-8") as f:
        text = f.read().rstrip()

    font_size = 15
    font = ImageFont.truetype(FONT_PATH, font_size)
    probe = Image.new("RGB", (10, 10))
    draw = ImageDraw.Draw(probe)

    lines = text.split("\n")
    # подгоняем размер шрифта, чтобы строка влезала в 2200 px
    while font_size > 9:
        font = ImageFont.truetype(FONT_PATH, font_size)
        maxw = max((draw.textlength(l, font=font) for l in lines), default=0)
        if maxw <= 2170:
            break
        font_size -= 1

    char_h = font_size + 6
    pad = 18
    title_h = 44
    w = int(maxw) + pad * 2
    h = title_h + pad + len(lines) * char_h + pad
    w = max(w, 640)

    img = Image.new("RGB", (w, h), BG)
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, w, title_h], fill=TITLE_BG)
    d.text((pad, 10), title, font=ImageFont.truetype(FONT_PATH, 19), fill=TITLE_FG)

    y = title_h + pad - 4
    for line in lines:
        d.text((pad, y), line, font=font, fill=FG)
        y += char_h

    img.save(png_path)
    print(f"{png_path}  ({w}x{h})")


os.makedirs(OUT, exist_ok=True)
for src, dst, title in TASKS:
    render(src, os.path.join(OUT, dst), title)
print("done")
