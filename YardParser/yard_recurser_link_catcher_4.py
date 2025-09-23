# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin
from pathlib import Path
from queue import Queue, Empty
import hashlib
import json
import threading
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from chromedriver_factory import ChromeDriverFactory  # ваша фабрика


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


@dataclass
class SisterRow:
    name: str
    link: str
    ship_type: str
    owner_company: str
    shipyard: str
    date_contract: str


class ShipSisterParser:
    """Парсер таблицы #content_tb_sister с одной страницы ship.aspx?..."""
    def __init__(self, driver: WebDriver, wait_sec: int = 30):
        self.driver = driver
        self.wait_sec = wait_sec

    @staticmethod
    def _norm(t: Optional[str]) -> str:
        return (t or "").replace("\xa0", " ").strip()

    def _open(self, url: str) -> None:
        self.driver.get(url)
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

    def _table_exists(self) -> bool:
        try:
            WebDriverWait(self.driver, self.wait_sec).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table#content_tb_sister"))
            )
            return True
        except Exception:
            return False

    def parse_sisters(self, page_url: str) -> List[Dict[str, str]]:
        """
        Возвращает список словарей:
          {"name","link","ship_type","owner_company","shipyard","date_contract"}
        Если таблицы нет — вернёт [] (это валидно).
        """
        self._open(page_url)
        time.sleep(0.2)  # сайт медленный — дать дорисоваться

        if not self._table_exists():
            return []

        table = self.driver.find_element(By.CSS_SELECTOR, "table#content_tb_sister")
        rows = table.find_elements(By.CSS_SELECTOR, "tbody > tr")
        if not rows:
            return []

        data_rows = rows[1:] if len(rows) > 1 else []  # пропускаем шапку

        out: List[Dict[str, str]] = []
        for tr in data_rows:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 5:
                continue

            # name + link
            name_td = tds[0]
            name_text = self._norm(name_td.text)
            link_abs = ""
            try:
                a = name_td.find_element(By.CSS_SELECTOR, 'a[href*="ship.aspx"]')
                href = a.get_attribute("href") or ""
                if href:
                    link_abs = urljoin(page_url, href)
                name_text = self._norm(a.text) or name_text
            except Exception:
                pass

            ship_type     = self._norm(tds[1].text)
            owner_company = self._norm(tds[2].text)
            shipyard      = self._norm(tds[3].text)
            date_contract = self._norm(tds[4].text)

            out.append({
                "name": name_text,
                "link": link_abs,
                "ship_type": ship_type,
                "owner_company": owner_company,
                "shipyard": shipyard,
                "date_contract": date_contract,
            })
        return out


class SisterGraphCrawler:
    """
    Рекурсивный сбор ссылок из таблицы #content_tb_sister со всех судов.
    Стартовые ссылки — из orderbook/*.json.
    Узел (страница ship.aspx?...): сохраняется в sisters_nodes/ship_<md5>.json.
    При повторном запуске уже сохранённые узлы пропускаются (возобновление).
    Все новые найденные ссылки дополнительно пишутся в sisters_discovered.txt (append-only).
    """
    def __init__(
        self,
        orderbook_dir: Path,
        out_nodes_dir: Path,
        discovered_file: Path,
        workers: int = 4,
        wait_sec: int = 30,
        use_profile_clone: bool = True,
    ):
        self.orderbook_dir = orderbook_dir
        self.out_nodes_dir = out_nodes_dir
        self.discovered_file = discovered_file
        self.workers = max(1, int(workers))
        self.wait_sec = wait_sec
        self.use_profile_clone = use_profile_clone

        self.out_nodes_dir.mkdir(parents=True, exist_ok=True)
        self._queue: "Queue[str]" = Queue()
        self._seen_lock = threading.Lock()
        self._append_lock = threading.Lock()
        self._print_lock = threading.Lock()

        # множество уже существующих узлов (по md5 URL)
        self._done_nodes: set[str] = set(self._scan_done_nodes())

    # ---------- вспомогательное ----------
    def _node_path(self, url: str) -> Path:
        return self.out_nodes_dir / f"ship_{md5_hex(url)}.json"

    def _error_path(self, url: str) -> Path:
        return self.out_nodes_dir / f"ship_{md5_hex(url)}.error.json"

    def _scan_done_nodes(self) -> List[str]:
        done = []
        for p in self.out_nodes_dir.glob("ship_*.json"):
            done.append(p.stem.replace("ship_", ""))
        return done

    def _append_discovered(self, urls: List[str]) -> None:
        if not urls:
            return
        with self._append_lock:
            with open(self.discovered_file, "a", encoding="utf-8") as f:
                for u in urls:
                    f.write(u.strip() + "\n")

    def _driver_factory(self) -> WebDriver:
        factory = ChromeDriverFactory.with_default_windows_profile(profile_name="Default")
        factory.use_profile_clone = self.use_profile_clone
        return factory.create()

    # ---------- загрузка семян/фронтира ----------
    def _load_seed_urls(self) -> List[str]:
        seeds = set()

        # 1) все ссылки из orderbook/*.json
        for p in self.orderbook_dir.glob("*.json"):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    obj = json.load(f)
                rows = obj.get("orderbook_rows") or []
                for r in rows:
                    u = (r.get("link") or "").strip()
                    if u:
                        seeds.add(u)
            except Exception:
                continue

        # 2) ранее найденные ссылки (append-only)
        if self.discovered_file.exists():
            try:
                with open(self.discovered_file, "r", encoding="utf-8") as f:
                    for ln in f:
                        u = ln.strip()
                        if u:
                            seeds.add(u)
            except Exception:
                pass

        return sorted(seeds)

    def _enqueue_pending(self, urls: List[str]) -> int:
        cnt = 0
        for url in urls:
            node_file = self._node_path(url)
            if node_file.exists():  # уже скачивали — пропуск
                continue
            self._queue.put(url)
            cnt += 1
        return cnt

    # ---------- воркеры ----------
    def _worker(self, wid: int):
        driver = None
        try:
            driver = self._driver_factory()
            parser = ShipSisterParser(driver, wait_sec=self.wait_sec)

            while True:
                try:
                    url = self._queue.get(timeout=1.5)
                except Empty:
                    break

                try:
                    with self._print_lock:
                        print(f"[W{wid}] open: {url}")

                    sisters = parser.parse_sisters(url)  # [] если таблицы нет

                    # новые ссылки для рекурсии
                    new_urls = []
                    for s in sisters:
                        u = s.get("link", "").strip()
                        if not u:
                            continue
                        node_file = self._node_path(u)
                        if not node_file.exists():
                            self._queue.put(u)
                            new_urls.append(u)

                    # сохраняем текущий узел
                    node = {
                        "url": url,
                        "ts": int(time.time()),
                        "sisters": sisters,
                    }
                    with open(self._node_path(url), "w", encoding="utf-8") as f:
                        json.dump(node, f, ensure_ascii=False, indent=2)

                    # зафиксируем найденные ссылки (для резюма)
                    self._append_discovered(new_urls)

                    with self._print_lock:
                        print(f"[W{wid}] saved {len(sisters)} rows; enqueued +{len(new_urls)}")

                except Exception as e:
                    # падение сайта/таймаут — пишем error.json и идём дальше
                    with open(self._error_path(url), "w", encoding="utf-8") as f:
                        json.dump({"url": url, "error": repr(e), "ts": int(time.time())},
                                  f, ensure_ascii=False, indent=2)
                    with self._print_lock:
                        print(f"[W{wid}] ERROR on {url}: {e}")
                finally:
                    self._queue.task_done()

        finally:
            if driver:
                try:
                    # driver.quit()  # по желанию
                    pass
                except Exception:
                    pass

    # ---------- запуск ----------
    def run(self) -> Tuple[int, int]:
        seeds = self._load_seed_urls()
        todo = self._enqueue_pending(seeds)
        total_known = len(seeds)

        print(f"Старт: всего известных ссылок: {total_known}; к обработке сейчас: {todo}; потоков: {self.workers}")
        if todo == 0:
            print("Нечего делать — все узлы уже скачаны. Можно удалить *.error.json и перезапустить для ретрая.")
            return total_known, 0

        threads = []
        for i in range(self.workers):
            t = threading.Thread(target=self._worker, args=(i + 1,), daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        print("Готово.")
        return total_known, todo
