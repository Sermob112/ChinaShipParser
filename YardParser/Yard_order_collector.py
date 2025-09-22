# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin
import json
import re
import threading
import time
from queue import Queue, Empty
from pathlib import Path

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ожидаем, что у вас уже есть фабрика из предыдущих шагов
from chromedriver_factory import ChromeDriverFactory


def _slug(s: str, max_len: int = 60) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\s-]+", "", s, flags=re.U)  # убрать все «не-слова»
    s = re.sub(r"[\s_-]+", "-", s)               # пробелы/подчёркивания в дефисы
    s = s.strip("-_")
    if len(s) > max_len:
        s = s[:max_len].rstrip("-_")
    return s or "item"


@dataclass
class OrderbookRow:
    index: str
    name: str                # текст ссылки в колонке "Ship Name (Hull)"
    link: str                # абсолютная ссылка на ship.aspx?... (может быть "")
    ship_type: str
    owner_company: str
    date_delivery: str
    date_contract: str


class ShipyardOrderbookParser:
    """Парсит таблицу #content_tb_orderbook на странице одной верфи."""
    def __init__(self, driver: WebDriver, wait_sec: int = 25):
        self.driver = driver
        self.wait_sec = wait_sec

    def _open(self, url: str) -> None:
        self.driver.get(url)
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

    @staticmethod
    def _norm(t: Optional[str]) -> str:
        return (t or "").replace("\xa0", " ").replace("\r", "\n").strip()

    def _wait_table_or_absence(self) -> bool:
        """
        Ждём либо появления таблицы, либо таймаута (когда таблицы нет).
        Возвращает True, если таблица найдена.
        """
        try:
            WebDriverWait(self.driver, self.wait_sec).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table#content_tb_orderbook"))
            )
            return True
        except Exception:
            return False

    def parse_orderbook(self, page_url: str) -> List[Dict[str, str]]:
        """
        Возвращает список словарей с полями:
         index, name, link, ship_type, owner_company, date_delivery, date_contract
        Если таблицы нет — вернёт пустой список (это не ошибка).
        """
        self._open(page_url)
        # сайт медленный — небольшая пауза, чтобы дорисовать
        time.sleep(0.25)

        has_table = self._wait_table_or_absence()
        if not has_table:
            return []

        table = self.driver.find_element(By.CSS_SELECTOR, "table#content_tb_orderbook")
        rows = table.find_elements(By.CSS_SELECTOR, "tbody > tr")
        if not rows:
            return []

        # первая строка — шапка
        data_rows = rows[1:] if len(rows) > 1 else []
        out: List[Dict[str, str]] = []

        for tr in data_rows:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 6:
                continue

            index = self._norm(tds[0].text)

            # колонка "Ship Name (Hull)" с <a>
            name_td = tds[1]
            name_text = self._norm(name_td.text)
            link_abs = ""
            try:
                a = name_td.find_element(By.CSS_SELECTOR, 'a[href*="ship.aspx"]')
                href = a.get_attribute("href") or ""
                if href:
                    link_abs = urljoin(page_url, href)
                # текст надёжнее брать из самого <a>, если есть
                name_text = self._norm(a.text) or name_text
            except Exception:
                pass

            ship_type     = self._norm(tds[2].text)
            owner_company = self._norm(tds[3].text)
            date_delivery = self._norm(tds[4].text)
            date_contract = self._norm(tds[5].text)

            out.append({
                "index": index,
                "name": name_text,
                "link": link_abs,
                "ship_type": ship_type,
                "owner_company": owner_company,
                "date_delivery": date_delivery,
                "date_contract": date_contract
            })

        return out


class OrderbookCollectorManager:
    """
    Менеджер многопоточного обхода верфей:
    - читает shipyards_list.json
    - параллельно (по воркерам) заходит на каждую верфь и парсит таблицу orderbook
    - сохраняет результат по каждой верфи в отдельный файл orderbook/{no}_{slug}.json
    - поддерживает возобновление: уже существующие файлы пропускаются
    """
    def __init__(self,
                 input_json: Path,
                 out_dir: Path,
                 base_url: str = "http://chinashipbuilding.cn/",
                 workers: int = 4,
                 wait_sec: int = 30,
                 use_profile_clone: bool = True):
        self.input_json = input_json
        self.out_dir = out_dir
        self.base_url = base_url
        self.workers = max(1, int(workers))
        self.wait_sec = wait_sec
        self.use_profile_clone = use_profile_clone

        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._queue: "Queue[Dict]" = Queue()
        self._lock = threading.Lock()  # для аккуратного принта/учёта

    @staticmethod
    def _driver_factory(use_profile_clone: bool) -> WebDriver:
        factory = ChromeDriverFactory.with_default_windows_profile(profile_name="Default")
        factory.use_profile_clone = use_profile_clone
        return factory.create()

    def _yard_output_path(self, yard: Dict) -> Path:
        no = yard.get("no", "")
        name = yard.get("name", "") or "yard"
        slug = _slug(f"{no}_{name}")
        return self.out_dir / f"{slug}.json"

    def _load_input(self) -> List[Dict]:
        with open(self.input_json, "r", encoding="utf-8") as f:
            yards = json.load(f)
        if not isinstance(yards, list):
            raise ValueError("Ожидался JSON-массив в shipyards_list.json")
        return yards

    def _enqueue_tasks(self, yards: List[Dict]) -> int:
        cnt = 0
        for y in yards:
            out_path = self._yard_output_path(y)
            # резюмируемость: если файл уже есть — пропускаем
            if out_path.exists():
                continue
            self._queue.put(y)
            cnt += 1
        return cnt

    def _worker(self, wid: int):
        driver = None
        try:
            driver = self._driver_factory(self.use_profile_clone)
            parser = ShipyardOrderbookParser(driver, wait_sec=self.wait_sec)

            while True:
                try:
                    yard = self._queue.get(timeout=2.0)
                except Empty:
                    break

                name = yard.get("name", "")
                link = yard.get("link", "")
                no   = yard.get("no", "")

                out_path = self._yard_output_path(yard)
                try:
                    with self._lock:
                        print(f"[W{wid}] start: #{no} {name} -> {link}")

                    rows = []
                    if link:
                        rows = parser.parse_orderbook(link)

                    # сохраняем по-штучно (инкрементально)
                    out_obj = {
                        "no": no,
                        "name": name,
                        "link": link,
                        "orderbook_rows": rows,
                        "ts": int(time.time())
                    }
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(out_obj, f, ensure_ascii=False, indent=2)

                    with self._lock:
                        print(f"[W{wid}] saved: {out_path} (rows: {len(rows)})")

                except Exception as e:
                    # лог ошибки рядом с целью (и всё равно помечаем как «сделано», чтобы не зациклиться)
                    err_path = out_path.with_suffix(".error.json")
                    with open(err_path, "w", encoding="utf-8") as f:
                        json.dump({
                            "no": no, "name": name, "link": link,
                            "error": repr(e), "ts": int(time.time())
                        }, f, ensure_ascii=False, indent=2)
                    with self._lock:
                        print(f"[W{wid}] ERROR on #{no} {name}: {e} -> {err_path}")
                finally:
                    self._queue.task_done()

        finally:
            if driver:
                try:
                    # по желанию можно закрывать
                    # driver.quit()
                    pass
                except Exception:
                    pass

    def run(self) -> Tuple[int, int]:
        yards = self._load_input()
        total = len(yards)
        to_do = self._enqueue_tasks(yards)

        print(f"Всего верфей в списке: {total}. К обработке: {to_do}. Потоков: {self.workers}")
        if to_do == 0:
            print("Нечего делать: все файлы уже существуют (резюмируемость).")
            return total, 0

        threads = []
        for i in range(self.workers):
            t = threading.Thread(target=self._worker, args=(i + 1,), daemon=True)
            t.start()
            threads.append(t)

        # дождаться выполнения
        for t in threads:
            t.join()

        print("Готово.")
        return total, to_do
