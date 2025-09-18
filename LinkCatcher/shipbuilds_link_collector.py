# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional
from urllib.parse import urljoin
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains


@dataclass
class ShipbuildsLinkCollector:
    """
    Сборщик ссылок категорий из меню ShipBuilding на chinashipbuilding.cn.

    Использование:
        driver = factory.create()
        collector = ShipbuildsLinkCollector(driver, base_url="http://chinashipbuilding.cn/")
        links = collector.collect_category_links(
            page_url="http://chinashipbuilding.cn/shipbuilds.aspx?nmkhTk8Pl4EN"
        )
    """
    driver: WebDriver
    base_url: str = "http://chinashipbuilding.cn/"

    # селекторы, вынесены, чтобы легко подправить
    _menu_root_id: str = "content_hrd_web_mnu_sysn0"
    _menu_dropdown_id: str = "content_hrd_web_mnu_sysn0Items"

    def _open(self, url: str, timeout: int = 30) -> None:
        self.driver.get(url)
        WebDriverWait(self.driver, timeout).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

    def _ensure_menu_open(self, timeout: int = 10) -> None:
        """Наводим курсор на пункт ShipBuilding, чтобы показать его выпадающее меню (WebForms-стиль)."""
        try:
            menu_root = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.ID, self._menu_root_id))
            )
        except Exception:
            return

        # Наводим мышь на пункт меню, чтобы появился блок Items
        try:
            ActionChains(self.driver).move_to_element(menu_root).perform()
            # Дадим фронту время на показ (JS onmouseover)
            time.sleep(0.4)
        except Exception:
            pass

    def _find_category_anchors(self, timeout: int = 10) -> List:
        """Ищем все <a> внутри выпадающего блока ShipBuilding."""
        try:
            dropdown = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.ID, self._menu_dropdown_id))
            )
        except Exception:
            return []

        # Внутри блока есть таблица с множеством <a> (BulkCarrier, Container, Tanker и т.п.)
        anchors = dropdown.find_elements(By.CSS_SELECTOR, "a[href]")
        return anchors

    def collect_category_links(self, page_url: str) -> List[Dict[str, str]]:
        """
        Открывает страницу и возвращает список словарей:
            { "text": "<Название>", "href": "<абсолютный URL>" }
        """
        self._open(page_url)
        self._ensure_menu_open()

        anchors = self._find_category_anchors()
        results: List[Dict[str, str]] = []

        for a in anchors:
            try:
                text = (a.text or "").strip()
                href_raw: Optional[str] = a.get_attribute("href")  # может быть абсолютной
                # На странице ссылки часто относительные (например: shipbuilds.aspx?nmkhTk8Pl4ENaclppkLL0p4J)
                href_abs = urljoin(self.base_url, href_raw) if href_raw else None
                if not href_abs:
                    continue
                if not text:
                    # иногда текст пустой, можно попробовать взять title/alt
                    text = (a.get_attribute("title") or a.get_attribute("alt") or "").strip()
                results.append({"text": text, "href": href_abs})
            except Exception:
                continue

        # Удалим дубликаты по href, сохраняя первый текст
        seen = set()
        unique: List[Dict[str, str]] = []
        for item in results:
            if item["href"] in seen:
                continue
            seen.add(item["href"])
            unique.append(item)

        return unique

    # (опционально) быстрый метод для дампа в CSV
    def save_to_csv(self, items: List[Dict[str, str]], csv_path: str) -> None:
        import csv
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["text", "href"])
            writer.writeheader()
            writer.writerows(items)
