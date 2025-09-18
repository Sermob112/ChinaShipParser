# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path

from LinkCatcher.shipbuilds_link_collector import ShipbuildsLinkCollector
from LinkCatcher.shipbuild_items_collector import ShipbuildItemsCollector
from Parser.fleet_table_collector import FleetTableCollector
# Подключаем вашу фабрику из прошлого ответа.
# Если вы положили её в файл chromedriver_factory.py — импортируйте оттуда:
from chromedriver_factory import ChromeDriverFactory  # <-- адаптируйте имя файла при необходимости
import json

INPUT_TXT = Path(__file__).with_name("links_builds.txt")  
OUTPUT_TXT = Path(__file__).with_name("links_ship_pages.txt")
FLEET_URL = "http://chinashipbuilding.cn/fleet.aspx?nmkhTk8Pl4ENaFLEET4J"
OUT_JSON = Path(__file__).with_name("fleet_list.json")
OUT_PAGES_JSON = Path(__file__).with_name("fleet_page_links.json")
OUT_DIR = Path(__file__).with_suffix("")  # папка рядом со скриптом
OUT_DIR.mkdir(exist_ok=True)
PAGES_DIR = OUT_DIR / "fleet_pages"       # сюда пофайлово строки
PAGES_DIR.mkdir(exist_ok=True)
PAGER_JSON = OUT_DIR / "fleet_pagination.json"  
def _save_rows_per_page(page_no: int, page_url: str, rows):
    """
    Сохраняем КАЖДУЮ страницу в отдельный JSON:
      fleet_pages/fleet_page_0001.json
    """
    safe_no = page_no if isinstance(page_no, int) and page_no > 0 else 0
    out_path = PAGES_DIR / f"fleet_page_{safe_no:04d}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "page_no": page_no,
            "page_url": page_url,
            "rows": rows,
        }, f, ensure_ascii=False, indent=2)
    print(f"[SAVE] page {page_no} -> {out_path} (rows: {len(rows)})")
def _save_pager_block_unique(page_no: int, page_url: str, block_links):
    """
    На страницах 1,11,21,... накапливаем ссылки пагинации в fleet_pagination.json
    без дублей по href.
    """
    try:
        with open(PAGER_JSON, "r", encoding="utf-8") as f:
            existing = json.load(f)
            if not isinstance(existing, list):
                existing = []
    except Exception:
        existing = []

    seen = {it["href"] for it in existing if isinstance(it, dict) and "href" in it}
    new_items = []
    for it in block_links:
        href = it.get("href")
        text = it.get("text")
        if not href or href in seen:
            continue
        seen.add(href)
        new_items.append({"text": text, "href": href, "source_page_no": page_no})

    if new_items:
        existing.extend(new_items)
        with open(PAGER_JSON, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        print(f"[SAVE] pager block @ page {page_no}: +{len(new_items)} links -> {PAGER_JSON}")
    else:
        print(f"[SKIP] pager block @ page {page_no}: no new links")


def main():
    factory = ChromeDriverFactory.with_default_windows_profile(profile_name="Default")
    factory.use_profile_clone = True  # <- включаем клон, чтобы не закрывать ваш Chrome
    all_links = set()
    driver = factory.create()
    # try:
    #     collector = ShipbuildsLinkCollector(driver, base_url="http://chinashipbuilding.cn/")
    #     links = collector.collect_category_links("http://chinashipbuilding.cn/shipbuilds.aspx?nmkhTk8Pl4EN")

    #     print(f"Найдено ссылок: {len(links)}")
    #     for it in links:
    #         print(f"- {it['text'] or '(no text)'} -> {it['href']}")
    # finally:
    #     pass  # driver.quit() по желанию

    # try:
    #     collector = ShipbuildItemsCollector(driver=driver, wait_sec=25)

    #     # читаем входные ссылки категорий
    #     if not INPUT_TXT.exists():
    #         raise FileNotFoundError(f"Не найден файл со ссылками: {INPUT_TXT}")

    #     with open(INPUT_TXT, "r", encoding="utf-8") as f:
    #         category_urls = [ln.strip() for ln in f.readlines() if ln.strip()]

    #     print(f"Категорий для обхода: {len(category_urls)}")
    #     for i, url in enumerate(category_urls, 1):
    #         print(f"[{i}/{len(category_urls)}] {url}")
    #         try:
    #             links = collector.collect_item_links(url)
    #             print(f"  найдено карточек: {len(links)}")
    #             all_links.update(links)
    #         except Exception as e:
    #             print(f"  ошибка на {url}: {e}")

    #     # сохраняем общий txt без дублей
    #     ShipbuildItemsCollector.save_txt(sorted(all_links), str(OUTPUT_TXT))
    #     print(f"Ссылки карточек сохранены: {OUTPUT_TXT}  (всего: {len(all_links)})")

    # finally:
    #     # по желанию закрывайте браузер
    #     # driver.quit()
    #     pass

    # try:
    #     collector = FleetTableCollector(driver=driver, base_url="http://chinashipbuilding.cn/")

    #     items = collector.collect_rows(FLEET_URL)
    #     print(f"Строк собрано: {len(items)}")
    #     for it in items[:5]:
    #         print(f"- #{it['no']} {it['name']} -> {it['link']}")
    #     page_links = collector.collect_all_pagination_links(FLEET_URL)
    #     print(f"Собрано ссылок страниц: {len(page_links)}")
    #     for it in page_links[:10]:
    #         print(f"- p.{it['text']}: {it['href']}")

    #     collector.save_json(page_links, str(OUT_PAGES_JSON))
    #     print(f"JSON со ссылками пагинации сохранён: {OUT_PAGES_JSON}")
    #     collector.save_json(items, str(OUT_JSON))
    #     print(f"JSON сохранён: {OUT_JSON}")
    # finally:
    #     # по желанию:
    #     # driver.quit()
    #     pass
    try:
        collector = FleetTableCollector(driver=driver, base_url="http://chinashipbuilding.cn/")

        # Идём инкрементально по страницам; сохраняем сразу после каждой
        collector.walk_pages_incremental(
            start_url=FLEET_URL,
            save_rows_cb=_save_rows_per_page,
            save_pager_cb=_save_pager_block_unique,
            max_pages=None  # можно ограничить для теста, например 25
        )

        print("Готово.")
    finally:
        # driver.quit()  # по желанию
        pass
if __name__ == "__main__":
    main()