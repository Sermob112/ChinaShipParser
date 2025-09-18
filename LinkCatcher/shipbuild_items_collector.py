# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Set
from urllib.parse import urljoin
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


@dataclass
class ShipbuildItemsCollector:
    """
    Обходит страницу категории (BulkCarrier / Container / Tanker ...)
    и собирает ссылки карточек из таблицы #content_tb_shipbuilds.

    Метод:
      collect_item_links(page_url) -> List[str]
    """
    driver: WebDriver
    wait_sec: int = 20

    def _open(self, url: str):
        self.driver.get(url)
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

    def _wait_grid(self):
        # ждём, пока появится таблица с плитками
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table#content_tb_shipbuilds"))
        )

    def collect_item_links(self, page_url: str) -> List[str]:
        """
        Возвращает уникальные абсолютные ссылки на карточки вида:
            http://chinashipbuilding.cn/shipbuild.aspx?....
        """
        self._open(page_url)
        self._wait_grid()

        # иногда у них WebForms обновляет кусок DOM, дадим полсекунды на дорисовку
        time.sleep(0.3)

        anchors = self.driver.find_elements(
            By.CSS_SELECTOR,
            '#content_tb_shipbuilds a[href*="shipbuild.aspx"]'
        )

        links: Set[str] = set()
        for a in anchors:
            try:
                href = a.get_attribute("href") or ""
                # На сайте часто относительные href: "shipbuild.aspx?..."
                if href and "shipbuild.aspx" in href:
                    abs_url = urljoin(page_url, href)
                    links.add(abs_url)
            except Exception:
                continue

        return sorted(links)

    @staticmethod
    def save_txt(lines: List[str], out_path: str) -> None:
        with open(out_path, "w", encoding="utf-8") as f:
            for line in lines:
                f.write(line.strip() + "\n")
