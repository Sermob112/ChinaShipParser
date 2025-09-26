# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Callable, Optional, Tuple
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium.webdriver.remote.webdriver import WebDriver

# Берём твой одиночный сборщик
from Parser.fleet_table_collector import FleetTableCollector


@dataclass
class FleetParallelRunner:
    """
    Параллельный обход таблицы флота с возобновлением прогресса.

    - Строит индекс страниц по пагинации (1..N).
    - Обрабатывает страницы в N потоков, у каждого свой WebDriver.
    - Сохраняет постранично результат и прогресс, чтобы можно было продолжить.

    Аргументы:
      driver_factory: Callable[[], WebDriver]  — фабрика драйверов (например, ChromeDriverFactory.create)
      base_url: str                            — базовый URL сайта
      out_dir: Path                            — куда писать результаты
      progress_path: Path                      — json-файл прогресса
      wait_sec: int                            — таймауты ожиданий
      workers: int                             — число потоков
    """
    driver_factory: Callable[[], WebDriver]
    base_url: str = "http://chinashipbuilding.cn/"
    out_dir: Path = Path("fleet_out")
    progress_path: Path = Path("fleet_progress.json")
    wait_sec: int = 25
    workers: int = 4

    _progress_lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _print_lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    # ---------- прогресс ----------
    def _load_progress(self) -> Dict[str, Dict]:
        if not self.progress_path.exists():
            return {"done": {}, "meta": {}}
        try:
            with open(self.progress_path, "r", encoding="utf-8") as f:
                obj = json.load(f)
                if not isinstance(obj, dict):
                    return {"done": {}, "meta": {}}
                if "done" not in obj:
                    obj["done"] = {}
                if "meta" not in obj:
                    obj["meta"] = {}
                return obj
        except Exception:
            return {"done": {}, "meta": {}}

    def _save_progress(self, prog: Dict[str, Dict]) -> None:
        tmp = self.progress_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(prog, f, ensure_ascii=False, indent=2)
        tmp.replace(self.progress_path)

    def _mark_done(self, page_no: int, url: str, rows_count: int) -> None:
        with self._progress_lock:
            prog = self._load_progress()
            prog["done"][str(page_no)] = {"url": url, "rows": rows_count, "ts": int(time.time())}
            self._save_progress(prog)

    # ---------- индекс страниц ----------
    def _build_page_index(self, driver: WebDriver, start_url: str) -> List[Tuple[int, str]]:
        """
        Возвращает список [(page_no, url), ...] включая первую страницу.
        """
        coll = FleetTableCollector(driver=driver, base_url=self.base_url, wait_sec=self.wait_sec)
        # соберём все ссылки из блоков (2..N), первую добавим сами
        block_links = coll.collect_all_pagination_links(start_url)  # [{'text': '2', 'href': '...'}, ...]
        index: List[Tuple[int, str]] = [(1, start_url)]
        for it in block_links:
            t = it.get("text", "")
            if t.isdigit():
                index.append((int(t), it["href"]))
        # уберём дубли и отсортируем
        index = list({(n, u) for (n, u) in index})
        index.sort(key=lambda x: x[0])
        return index

    # ---------- worker ----------
    def _process_page(self, job: Tuple[int, str]) -> Tuple[int, str, int, Optional[str]]:
        """
        Обрабатывает одну страницу: собирает строки и сохраняет.
        Возвращает (page_no, url, rows_count, error or None).
        """
        page_no, url = job
        driver = None
        try:
            driver = self.driver_factory()
            coll = FleetTableCollector(driver=driver, base_url=self.base_url, wait_sec=self.wait_sec)

            rows = coll.collect_rows(url)
            rows_count = len(rows)

            # постраничный файл с данными
            self.out_dir.mkdir(parents=True, exist_ok=True)
            page_file = self.out_dir / f"fleet_page_{page_no:05d}.json"
            with open(page_file, "w", encoding="utf-8") as f:
                json.dump({"page_no": page_no, "url": url, "rows": rows}, f, ensure_ascii=False, indent=2)

            # отметить как сделанный
            self._mark_done(page_no, url, rows_count)

            return page_no, url, rows_count, None
        except Exception as e:
            return page_no, url, 0, str(e)
        finally:
            try:
                if driver is not None:
                    driver.quit()
            except Exception:
                pass

    # ---------- run ----------
    def run(self, start_url: str, rebuild_index: bool = True) -> None:
        """
        Основной метод:
          1) создаёт/пересоздаёт индекс страниц (если rebuild_index=True);
          2) загружает прогресс и фильтрует — оставляет только незавершённые страницы;
          3) запускает пул потоков и обрабатывает задачи;
          4) после каждой завершённой — пишет прогресс.
        """
        # 0. первичная проверка: чтобы построить индекс, нужен один временный драйвер
        with self._print_lock:
            print("[PAR] Создание индекса страниц...")

        if rebuild_index:
            drv = self.driver_factory()
            try:
                index = self._build_page_index(drv, start_url)
            finally:
                try:
                    drv.quit()
                except Exception:
                    pass
            # сохраним индекс в meta, чтобы можно было посмотреть
            with self._progress_lock:
                prog = self._load_progress()
                prog["meta"]["index"] = [{"page_no": n, "url": u} for (n, u) in index]
                self._save_progress(prog)
        else:
            # Попробуем взять прошлый индекс
            prog = self._load_progress()
            meta_idx = prog.get("meta", {}).get("index", [])
            index = [(int(it["page_no"]), it["url"]) for it in meta_idx if "page_no" in it and "url" in it]
            if not index:
                # если пусто — построим
                drv = self.driver_factory()
                try:
                    index = self._build_page_index(drv, start_url)
                finally:
                    try:
                        drv.quit()
                    except Exception:
                        pass

        with self._print_lock:
            print(f"[PAR] Страниц всего в индексе: {len(index)}")

        # 1. отфильтруем уже сделанные
        prog = self._load_progress()
        done = set(int(k) for k in prog.get("done", {}).keys() if str(k).isdigit())
        jobs = [(n, u) for (n, u) in index if n not in done]
        with self._print_lock:
            print(f"[PAR] К обработке страниц: {len(jobs)}; потоков: {self.workers}")

        if not jobs:
            with self._print_lock:
                print("[PAR] Нечего делать — все страницы уже обработаны.")
            return

        # 2. пул потоков
        with ThreadPoolExecutor(max_workers=max(1, self.workers)) as ex:
            fut2job = {ex.submit(self._process_page, job): job for job in jobs}
            for fut in as_completed(fut2job):
                job = fut2job[fut]
                try:
                    page_no, url, rows_cnt, err = fut.result()
                    with self._print_lock:
                        if err:
                            print(f"[PAR][ERR] p.{page_no}: {url} -> ERROR: {err}")
                        else:
                            print(f"[PAR][OK ] p.{page_no}: {url} -> rows={rows_cnt}")
                except Exception as e:
                    with self._print_lock:
                        print(f"[PAR][FUT] job {job} -> raised: {e}")

        with self._print_lock:
            print("[PAR] Готово.")
