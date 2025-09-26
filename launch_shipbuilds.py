# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import argparse
from LinkCatcher.shipbuilds_link_collector import ShipbuildsLinkCollector
from LinkCatcher.shipbuild_items_collector import ShipbuildItemsCollector
from Parser.fleet_table_collector import FleetTableCollector
from YardParser.Yard_ship_details_collector_5 import ShipDetailsCollectorManager
from chromedriver_factory import ChromeDriverFactory  
from YardParser.yards_link_collector_1 import YardsCollector
from YardParser.yard_info_collector_3 import ShipyardDetailsCollector
from YardParser.Yard_order_collector_2 import OrderbookCollectorManager
from YardParser.yard_recurser_link_catcher_4 import SisterGraphCrawler
from YardParser.rotating_guarded_ship_details_collector_6 import ShipDetailsCollectorManager as RotatingShipMgr
from Parser.fleet_pages_ship_details_collector_2 import FleetPagesShipDetailsParallelRunner

from Parser.fleet_table_collector_parallel_1 import FleetParallelRunner
import json


# ==== общие пути/константы ====
BASE_URL = "http://chinashipbuilding.cn/"

# Shipbuilds (категории и карточки построенных судов)
SHIPBUILDS_START = "http://chinashipbuilding.cn/shipbuilds.aspx?nmkhTk8Pl4EN"
BUILD_INPUT_TXT  = Path(__file__).with_name("links_builds.txt")
BUILD_OUTPUT_TXT = Path(__file__).with_name("links_ship_pages.txt")

# Fleet (флот: инкрементальный проход)
FLEET_URL = "http://chinashipbuilding.cn/fleet.aspx?nmkhTk8Pl4ENaFLEET4J"
OUT_DIR = Path(__file__).with_suffix("")    # папка рядом со скриптом
PAGES_DIR = OUT_DIR / "fleet_pages"
PAGES_DIR.mkdir(parents=True, exist_ok=True)
PAGER_JSON = OUT_DIR / "fleet_pagination.json"

# Yards (список верфей + детали)
YARDS_URL = "http://chinashipbuilding.cn/shipyards.aspx?nmkhTk8Pl4ENaoklppLwi94cg"
YARDS_LIST_JSON = Path(__file__).with_name("shipyards_list.json")
YARDS_DETAILS_JSON = Path(__file__).with_name("shipyards_details.json")


OUT_DIR_FOR_YARD_ORDERBOOK  = Path(__file__).parent / "orderbook"
OUT_DIR_FOR_YARD_ORDERBOOK.mkdir(parents=True, exist_ok=True)


ORDERBOOK_DIR = Path(__file__).parent / "orderbook"           # уже есть папка с 43 файлами
SISTERS_DIR   = Path(__file__).parent / "sisters_nodes"       # сюда будем писать узлы
SISTERS_DIR.mkdir(parents=True, exist_ok=True)
DISCOVERED_JSON = Path(__file__).parent / "sisters_discovered.json"



SISTERS_JSON = Path(__file__).parent / "sisters_discovered.json"   # если у тебя JSON
SISTERS_TXT  = Path(__file__).parent / "sisters_discovered.txt"    # запасной вариант
SHIP_DETAILS_DIR = Path(__file__).parent / "ship_details"
SHIP_DETAILS_DIR.mkdir(parents=True, exist_ok=True)
ACCOUNTS_JSON = Path(__file__).parent / "shipbuilding_accounts.json"
ACCOUNT_CURSOR = Path(__file__).parent / "account_cursor.json"



FLEET_PAR_OUT_DIR   = OUT_DIR / "fleet_pages_par"
FLEET_PAR_OUT_DIR.mkdir(parents=True, exist_ok=True)
FLEET_PAR_PROGRESS  = OUT_DIR / "fleet_progress.json"

# ==== вспомогательные коллбеки для Fleet ====
def _save_rows_per_page(page_no: int, page_url: str, rows):
    safe_no = page_no if isinstance(page_no, int) and page_no > 0 else 0
    out_path = PAGES_DIR / f"fleet_page_{safe_no:04d}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"page_no": page_no, "page_url": page_url, "rows": rows},
                  f, ensure_ascii=False, indent=2)
    print(f"[SAVE] page {page_no} -> {out_path} (rows: {len(rows)})")

def _save_pager_block_unique(page_no: int, page_url: str, block_links):
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

def driver_factory():
    # твой класс фабрики; можно включить клон-профиля, если нужно
    fac = ChromeDriverFactory.with_default_windows_profile(profile_name="Default")
    fac.use_profile_clone = True
    return fac.create()


# ==== фабрика драйвера ====
def _make_driver():
    factory = ChromeDriverFactory.with_default_windows_profile(profile_name="Default")
    factory.use_profile_clone = True  # можно не закрывать ваш Chrome
    driver = factory.create()
    return driver


# ==== ЗАДАЧИ ====

def task_shipbuilds_categories():
    """Собрать ссылки категорий ShipBuilding (верхнеуровневые)."""
    driver = _make_driver()
    try:
        collector = ShipbuildsLinkCollector(driver, base_url=BASE_URL)
        links = collector.collect_category_links(SHIPBUILDS_START)
        print(f"Найдено ссылок категорий: {len(links)}")
        for it in links:
            print(f"- {it['text'] or '(no text)'} -> {it['href']}")
    finally:
        # driver.quit()  # по желанию
        pass


def task_shipbuild_items():
    """
    Пройтись по ссылкам категорий из links_builds.txt и собрать ссылки карточек
    в links_ship_pages.txt (без дублей).
    """
    if not BUILD_INPUT_TXT.exists():
        raise FileNotFoundError(f"Не найден файл со ссылками категорий: {BUILD_INPUT_TXT}")

    with open(BUILD_INPUT_TXT, "r", encoding="utf-8") as f:
        category_urls = [ln.strip() for ln in f.readlines() if ln.strip()]

    driver = _make_driver()
    all_links = set()
    try:
        collector = ShipbuildItemsCollector(driver=driver, wait_sec=25)
        print(f"Категорий для обхода: {len(category_urls)}")

        for i, url in enumerate(category_urls, 1):
            print(f"[{i}/{len(category_urls)}] {url}")
            try:
                links = collector.collect_item_links(url)
                print(f"  найдено карточек: {len(links)}")
                all_links.update(links)
            except Exception as e:
                print(f"  ошибка на {url}: {e}")

        ShipbuildItemsCollector.save_txt(sorted(all_links), str(BUILD_OUTPUT_TXT))
        print(f"OK: {len(all_links)} ссылок -> {BUILD_OUTPUT_TXT}")
    finally:
        # driver.quit()
        pass


def task_fleet_incremental(max_pages: int | None = None):
    """Инкрементальный проход флота: сохраняем JSON после каждой страницы; пагинацию — только на 1,11,21,..."""
    driver = _make_driver()
    try:
        collector = FleetTableCollector(driver=driver, base_url=BASE_URL)
        collector.walk_pages_incremental(
            start_url=FLEET_URL,
            save_rows_cb=_save_rows_per_page,
            save_pager_cb=_save_pager_block_unique,
            max_pages=max_pages
        )
        print("Готово (fleet incremental).")
    finally:
        # driver.quit()
        pass


def task_yards_list(dedupe: bool = False):
    """Собрать список верфей (номер, название, ссылка) в shipyards_list.json."""
    driver = _make_driver()
    try:
        collector = YardsCollector(driver=driver, base_url=BASE_URL)
        items = collector.collect_yards(YARDS_URL, dedupe=dedupe)
        print(f"Верфей собрано: {len(items)}")
        for it in items[:10]:
            print(f"- #{it['no']}: {it['name']} -> {it['link']}")
        collector.save_json(items, str(YARDS_LIST_JSON))
        print(f"OK: {YARDS_LIST_JSON}")
    finally:
        # driver.quit()
        pass


def task_yards_details():
    """Пройтись по shipyards_list.json и собрать текст из span#content_lb_yard в shipyards_details.json (инкрементально)."""
    if not YARDS_LIST_JSON.exists():
        raise FileNotFoundError(f"Не найден {YARDS_LIST_JSON}. Сначала выполните --task yards_list")

    with open(YARDS_LIST_JSON, "r", encoding="utf-8") as f:
        yards = json.load(f)
        if not isinstance(yards, list):
            raise ValueError("shipyards_list.json должен быть JSON-массивом")

    driver = _make_driver()
    collected = []
    try:
        collector = ShipyardDetailsCollector(driver=driver, wait_sec=30)
        print(f"Всего верфей для обхода: {len(yards)}")

        for i, y in enumerate(yards, 1):
            name = y.get("name", "")
            link = y.get("link", "")
            no   = y.get("no", "")

            if not link:
                print(f"[{i}/{len(yards)}] пропуск (нет ссылки) — {name}")
                continue

            print(f"[{i}/{len(yards)}] {name} -> {link}")
            text = collector.collect_details(link)
            collected.append({"no": no, "name": name, "link": link, "details": text})

            # инкрементальное сохранение
            with open(YARDS_DETAILS_JSON, "w", encoding="utf-8") as f:
                json.dump(collected, f, ensure_ascii=False, indent=2)

        print(f"OK: {YARDS_DETAILS_JSON} (записей: {len(collected)})")
    finally:
        # driver.quit()
        pass


# ==== CLI ====
def parse_args():
    p = argparse.ArgumentParser(description="ChinaShipbuilding Parsers Launcher")
    p.add_argument(
        "--task",
        required=True,
        choices=[
            "shipbuilds_categories",
            "shipbuild_items",
            "fleet_incremental",
            "yards_list",
            "yards_details",
            "yards_orderbook",
            "sisters_crawl",   
            "ship_details",
            "ship_details_guard",
            "ship_details_rotate",
            "fleet_parallel",  
            "fleet_info_parallel", 
            # <--- НОВОЕ
        ],
    )
    p.add_argument("--max-pages", type=int, default=None)
    p.add_argument("--dedupe", action="store_true")
    p.add_argument("--workers", type=int, default=4)       # для orderbook/sisters
    p.add_argument("--wait-sec", type=int, default=30)     # для orderbook/sisters
    p.add_argument("--reuse-profile", action="store_true") # для orderbook/sisters
    # parse_args()
   
    p.add_argument("--batch-every", type=int, default=50, help="Сколько страниц обрабатывать за одну сессию перед ротацией аккаунта")
    p.add_argument("--min-tables", type=int, default=7, help="Если на странице меньше — считаем как ограничение и ротируем аккаунт")
    p.add_argument("--relogin-manual", action="store_true", help="Логин вручную (паузы вместо автологина)")
    p.add_argument("--first-login-wait", type=int, default=60, help="Секунды на первый ручной вход")
    p.add_argument("--relogin-wait", type=int, default=30, help="Секунды на ручной повторный вход (каждый батч)")
    p.add_argument("--max-items", type=int, default=None, help="Глобальный предел страниц за запуск")
    p.add_argument("--rebuild-index", action="store_true",
                   help="Перестроить индекс страниц перед запуском (иначе возьмём из fleet_progress.json, если ессть)")

    return p.parse_args()


def task_yard_orderbook(workers: int, wait_sec: int, reuse_profile: bool):
    mgr = OrderbookCollectorManager(
        input_json=YARDS_LIST_JSON,
        out_dir=OUT_DIR_FOR_YARD_ORDERBOOK,
        workers=workers,
        wait_sec=wait_sec,
        use_profile_clone=(not reuse_profile),
    )
    mgr.run()

def task_sisters_crawl(workers: int, wait_sec: int, reuse_profile: bool):
    mgr = SisterGraphCrawler(
        orderbook_dir=ORDERBOOK_DIR,
        out_nodes_dir=SISTERS_DIR,
        discovered_file_jsonl=DISCOVERED_JSON,
        workers=workers,
        wait_sec=wait_sec,
        use_profile_clone=(not reuse_profile),
    )
    mgr.run()

def task_ship_details(workers: int, wait_sec: int, reuse_profile: bool, login_wait: int):
    mgr = ShipDetailsCollectorManager(
        input_json=SISTERS_JSON,
        input_txt=SISTERS_TXT,
        out_dir=SHIP_DETAILS_DIR,
        workers=workers,
        wait_sec=wait_sec,
        use_profile_clone=(not reuse_profile),
        login_wait_sec=login_wait,                          # <--- НОВОЕ
        login_url_fallback="http://chinashipbuilding.cn/shipbuilds.aspx?nmkhTk8Pl4EN",
        batch_logout_every=50,                  # <-- после 50 страниц делаем Logout
    relogin_wait_sec=30, 
    )
    mgr.run()
def task_ship_details_guard(workers: int, wait_sec: int, reuse_profile: bool, login_wait: int, max_items: int | None, min_tables: int):
    mgr = ShipDetailsCollectorManager(
        input_json=SISTERS_JSON,
        input_txt=SISTERS_TXT,
        out_dir=SHIP_DETAILS_DIR,
        workers=workers,
        wait_sec=wait_sec,
        use_profile_clone=(not reuse_profile),
        login_wait_sec=login_wait,
        max_items_per_run=max_items,
        min_tables_required=min_tables,
    )
    mgr.run()

def task_ship_details_rotating(wait_sec: int, reuse_profile: bool,
                               batch_every: int, min_tables: int,
                               relogin_manual: bool, first_login_wait: int, relogin_wait: int,
                               max_items: int | None):
    mgr = RotatingShipMgr(
        input_json=SISTERS_JSON,
        input_txt=SISTERS_TXT,
        out_dir=SHIP_DETAILS_DIR,
        wait_sec=wait_sec,
        use_profile_clone=(not reuse_profile),
        login_url_fallback="http://chinashipbuilding.cn/shipbuilds.aspx?nmkhTk8Pl4EN",

        min_tables_required=min_tables,
        batch_logout_every=batch_every,

        accounts_file=ACCOUNTS_JSON,
        account_cursor_file=ACCOUNT_CURSOR,

        # режим логина:
        relogin_manual=relogin_manual,         # True = ручной логин; False = авто по accounts.json
        first_login_wait_sec=first_login_wait, # при ручном: время на первый вход
        relogin_wait_sec=relogin_wait,         # при ручном: пауза на re-login

        max_items_per_run=max_items,
    )
    mgr.run()
def task_fleet_parallel(workers: int, wait_sec: int, rebuild_index: bool):
    """
    Многопоточная выгрузка таблицы флота.
    - Строит индекс всех страниц по пагинации.
    - Бежит в N потоков, свой WebDriver на поток.
    - Сохраняет страницу сразу после парса.
    - Прогресс пишет в fleet_progress.json (возобновление поддерживается).
    """
    runner = FleetParallelRunner(
        driver_factory=driver_factory,          # один драйвер на поток
        base_url=BASE_URL,
        out_dir=FLEET_PAR_OUT_DIR,
        progress_path=FLEET_PAR_PROGRESS,
        wait_sec=wait_sec,
        workers=max(1, workers),
    )
    runner.run(FLEET_URL, rebuild_index=rebuild_index)


def task_ship_details_from_fleet_pages(workers: int, wait_sec: int, rebuild_index: bool):
    base = Path(__file__).resolve().parent

    pages_dir = base / "launch_shipbuilds" / "fleet_pages_par"
    out_dir   = base / "10Л_ship_details"

    runner = FleetPagesShipDetailsParallelRunner(
        pages_dir=pages_dir,
        out_dir=out_dir,
        wait_sec=wait_sec,
        workers=max(1, workers),
        use_profile_clone=True,

        # аккаунты
        accounts_file=base / "Registrator",                           # папка с jsonl/txt
        account_cursor_base=base / "Registrator" / "account_cursor.json",

        # поведение
        first_login_wait_sec=60,
        relogin_wait_sec=30,
        relogin_manual=False,
        batch_logout_every=50,
        min_tables_required=7,
        rotate_on_low_tables=False,  # НЕ ротировать, если <7 таблиц
    )
    runner.run()
def main():
    args = parse_args()
    if args.task == "shipbuilds_categories":
        task_shipbuilds_categories()
    elif args.task == "shipbuild_items":
        task_shipbuild_items()
    elif args.task == "fleet_incremental":
        task_fleet_incremental(max_pages=args.max_pages)
    elif args.task == "yards_list":
        task_yards_list(dedupe=args.dedupe)
    elif args.task == "yards_details":
        task_yards_details()
    elif args.task == "yards_orderbook":
        task_yard_orderbook(args.workers, args.wait_sec, args.reuse_profile)
    elif args.task == "sisters_crawl":                     # <--- НОВОЕ
        task_sisters_crawl(args.workers, args.wait_sec, args.reuse_profile)
    elif args.task == "ship_details":
        task_ship_details(args.workers, args.wait_sec, args.reuse_profile, args.login_wait)
    elif args.task == "ship_details_guard":
        task_ship_details_guard(args.workers, args.wait_sec, args.reuse_profile, args.login_wait, args.max_items, args.min_tables)
    elif args.task == "ship_details_rotate":
        task_ship_details_rotating(
            wait_sec=args.wait_sec,
            reuse_profile=args.reuse_profile,
            batch_every=args.batch_every,
            min_tables=args.min_tables,
            relogin_manual=args.relogin_manual,
            first_login_wait=args.first_login_wait,
            relogin_wait=args.relogin_wait,
            max_items=args.max_items,
        )
    elif args.task == "fleet_parallel":
        task_fleet_parallel(args.workers, args.wait_sec, args.rebuild_index)
    elif args.task == "fleet_info_parallel":
        task_ship_details_from_fleet_pages(args.workers, args.wait_sec, args.rebuild_index)

    else:
        raise SystemExit("Неизвестная задача")


if __name__ == "__main__":
    main()