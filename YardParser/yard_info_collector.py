# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Optional
import json
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


@dataclass
class ShipyardDetailsCollector:
    """
    Заходит на страницу shipyard.aspx?... и вытягивает весь текст из <span id="content_lb_yard">.
    """
    driver: WebDriver
    wait_sec: int = 25

    def _open(self, url: str) -> None:
        self.driver.get(url)
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

    def _wait_span(self) -> None:
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#content_lb_yard"))
        )

    @staticmethod
    def _norm(text: Optional[str]) -> str:
        if not text:
            return ""
        t = text.replace("\r", "\n").replace("\xa0", " ")
        # нормализуем множественные пробелы/переносы, но оставим абзацы
        lines = [ln.strip() for ln in t.splitlines()]
        compact = "\n".join([ln for ln in lines if ln != ""])
        return compact.strip()

    def collect_details(self, page_url: str) -> str:
        """
        Возвращает нормализованный текст из #content_lb_yard.
        Если элемента нет — пустую строку.
        """
        self._open(page_url)
        # сайт медленный — маленькая пауза
        time.sleep(0.2)

        try:
            self._wait_span()
            el = self.driver.find_element(By.CSS_SELECTOR, "#content_lb_yard")
            return self._norm(el.text)
        except Exception:
            return ""

    # утилиты сохранения
    @staticmethod
    def save_json(items: list[Dict], out_path: str) -> None:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
