# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple, Iterable
from urllib.parse import urljoin, urlsplit, urlunsplit
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


# ----------------- утилиты -----------------
def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def canonical(url: str) -> str:
    """Аккуратная канонизация URL для дедупликации."""
    u = (url or "").strip().replace("\\", "/").strip('\'"<> ')
    if not u:
        return ""
    parts = urlsplit(u)
    scheme = (parts.scheme or "").lower()
    netloc = (parts.netloc or "").lower()
    if netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]
    if netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]
    # без фрагмента
    return urlunsplit((scheme, netloc, parts.path, parts.query, ""))


@dataclass
class SisterRow:
    name: str
    link: str
    ship_type: str
    owner_company: str
    shipyard: str
    date_contract: str


# ----------------- парсер страницы ship.aspx -----------------
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


# ----------------- рекурсивный краулер -----------------
class SisterGraphCrawler:
    """
    Рекурсивный сбор ссылок из таблицы #content_tb_sister со всех судов.

    Улучшения:
      • сохраняем происхождение: origin_yard (верфь) из orderbook/*.json;
      • жёсткая дедупликация по каноническому URL (в памяти + по диску);
      • лог “фронтира” — JSONL (url, origin_yard), только новые записи;
      • поддержка расширения семян из пагинации ордербука, если линки страниц
        пагера у вас есть в JSON (напр. 'orderbook_pages': [...]).
    """
    def __init__(
        self,
        orderbook_dir: Path,
        out_nodes_dir: Path,
        discovered_file_jsonl: Path,   # NEW: JSONL вместо txt
        workers: int = 4,
        wait_sec: int = 30,
        use_profile_clone: bool = True,
    ):
        self.orderbook_dir = orderbook_dir
        self.out_nodes_dir = out_nodes_dir
        self.discovered_file = discovered_file_jsonl
        self.workers = max(1, int(workers))
        self.wait_sec = wait_sec
        self.use_profile_clone = use_profile_clone

        self.out_nodes_dir.mkdir(parents=True, exist_ok=True)
        self._queue: "Queue[Dict[str,str]]" = Queue()  # item: {"url","origin_yard"}  # NEW
        self._append_lock = threading.Lock()
        self._print_lock = threading.Lock()

        # набор уже увиденных (канонических) URL — чтобы не плодить дубликаты  # NEW
        self._seen_urls: set[str] = set()
        # инициализация _seen_urls по диску
        for p in self.out_nodes_dir.glob("ship_*.json"):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    obj = json.load(f)
                u = canonical(obj.get("url", ""))
                if u:
                    self._seen_urls.add(u)
            except Exception:
                continue
        # если есть .error.json — считаем их “не сделано”, чтобы ретраиться (не добавляем в seen)

        # инициализация по уже накопленному JSONL фронтира (если был)  # NEW
        if self.discovered_file.exists():
            try:
                with open(self.discovered_file, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            rec = json.loads(line)
                            u = canonical(rec.get("url", ""))
                            if u:
                                # видеть = не писать дубликаты в discovered, но обрабатывать будем
                                pass
                        except Exception:
                            continue
            except Exception:
                pass

    # ---------- пути ----------
    def _node_path(self, url: str) -> Path:
        return self.out_nodes_dir / f"ship_{md5_hex(url)}.json"

    def _error_path(self, url: str) -> Path:
        return self.out_nodes_dir / f"ship_{md5_hex(url)}.error.json"

    # ---------- запись discovered JSONL ----------
    def _append_discovered(self, items: Iterable[Dict[str, str]]) -> None:
        """
        items: [{"url": ..., "origin_yard": ...}, ...]
        Пишем только те, чей canonical(url) ещё не добавлен в self._seen_urls.
        """
        to_write = []
        for it in items:
            u = canonical(it.get("url", ""))
            if not u:
                continue
            if u in self._seen_urls:
                continue
            self._seen_urls.add(u)
            to_write.append({"url": u, "origin_yard": it.get("origin_yard", "")})

        if not to_write:
            return

        with self._append_lock:
            with open(self.discovered_file, "a", encoding="utf-8") as f:
                for rec in to_write:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # ---------- фабрика драйвера ----------
    def _driver_factory(self) -> WebDriver:
        factory = ChromeDriverFactory.with_default_windows_profile(profile_name="Default")
        factory.use_profile_clone = self.use_profile_clone
        return factory.create()

    # ---------- загрузка семян ----------
    def _load_seed_items(self) -> List[Dict[str, str]]:
        """
        Возвращает список {"url","origin_yard"}.
        Берём:
          • все orderbook_rows[*].link из каждого orderbook/*.json
          • origin_yard = объект "name" из файла ордербука
          • (опц.) если в файле есть 'orderbook_pages': [...], можно тоже прочитать и
            дополнительно загрузить строки (когда начнёте это сохранять в JSON).
        """
        items: Dict[str, str] = {}  # canonical(url) -> origin_yard
        for p in self.orderbook_dir.glob("*.json"):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    obj = json.load(f)
            except Exception:
                continue

            origin_yard = (obj.get("name") or "").strip()

            for r in (obj.get("orderbook_rows") or []):
                u = canonical((r.get("link") or "").strip())
                if not u:
                    continue
                # если ссылка повторяется у разных верфей — запомним ПЕРВОГО источника
                items.setdefault(u, origin_yard)

            # --- опционально: если когда-то начнёте сохранять пагинацию ордербука в JSON,
            # здесь можно дочитать дополнительные страницы и добавить их строки.
            # for page_url in (obj.get("orderbook_pages") or []):
            #     ... подгрузить и добавить r['link'] -> items ...

        # преобразуем в список уникальных семян
        seeds = [{"url": u, "origin_yard": items[u]} for u in sorted(items.keys())]
        return seeds

    def _enqueue_pending(self, seeds: List[Dict[str, str]]) -> int:
        cnt = 0
        for it in seeds:
            url = it["url"]
            if not url:
                continue
            # если уже скачан узел — пропуск
            if self._node_path(url).exists():
                self._seen_urls.add(canonical(url))  # чтобы не писать в discovered ещё раз
                continue
            self._queue.put(it)
            cnt += 1
        return cnt

    # ---------- worker ----------
    def _worker(self, wid: int):
        driver = None
        try:
            driver = self._driver_factory()
            parser = ShipSisterParser(driver, wait_sec=self.wait_sec)

            while True:
                try:
                    task = self._queue.get(timeout=1.5)
                except Empty:
                    break

                url = task["url"]
                origin_yard = task.get("origin_yard", "")

                try:
                    with self._print_lock:
                        print(f"[W{wid}] open: {url}  | origin: {origin_yard}")

                    sisters = parser.parse_sisters(url)  # [] если таблицы нет

                    # новые URL для рекурсии — с тем же origin_yard  # NEW
                    discovered_batch = []
                    for s in sisters:
                        u_raw = (s.get("link") or "").strip()
                        if not u_raw:
                            continue
                        u = canonical(u_raw)
                        if not u:
                            continue
                        if self._node_path(u).exists():
                            # уже скачан — считаем увиденным, но не добавляем в очередь
                            self._seen_urls.add(u)
                            continue
                        # если в текущем ранe ещё не видели — добавим
                        if u not in self._seen_urls:
                            self._queue.put({"url": u, "origin_yard": origin_yard})
                            discovered_batch.append({"url": u, "origin_yard": origin_yard})
                            # в _append_discovered() мы добавим в self._seen_urls

                    # сохраняем текущий узел
                    node = {
                        "url": url,
                        "origin_yard": origin_yard,  # NEW: происхождение
                        "ts": int(time.time()),
                        "sisters": sisters,
                    }
                    with open(self._node_path(url), "w", encoding="utf-8") as f:
                        json.dump(node, f, ensure_ascii=False, indent=2)

                    # фиксируем найденные (уникальные) для резюмируемости
                    self._append_discovered(discovered_batch)

                    with self._print_lock:
                        print(f"[W{wid}] saved {len(sisters)} rows; enqueued +{len(discovered_batch)}")

                except Exception as e:
                    with open(self._error_path(url), "w", encoding="utf-8") as f:
                        json.dump({
                            "url": url, "origin_yard": origin_yard,
                            "error": repr(e), "ts": int(time.time())
                        }, f, ensure_ascii=False, indent=2)
                    with self._print_lock:
                        print(f"[W{wid}] ERROR on {url}: {e}")
                finally:
                    self._queue.task_done()

        finally:
            if driver:
                try:
                    # driver.quit()
                    pass
                except Exception:
                    pass

    # ---------- запуск ----------
    def run(self) -> Tuple[int, int]:
        seeds = self._load_seed_items()              # [{"url","origin_yard"}, ...]
        todo = self._enqueue_pending(seeds)
        total_known = len(seeds)

        print(f"Старт: известных уникальных ссылок: {total_known}; к обработке: {todo}; потоков: {self.workers}")
        if todo == 0:
            print("Нечего делать — все узлы уже скачаны. Удалите *.error.json для ретрая при необходимости.")
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
