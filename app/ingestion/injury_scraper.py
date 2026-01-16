"""Playwright-based scraper for real-time NBA injury reports"""

import asyncio
import logging
from datetime import datetime, date
from typing import List, Optional
from urllib.parse import urljoin

from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeoutError

from app.ingestion.validators import RawInjuryReport

logger = logging.getLogger(__name__)


class InjuryScraper:
    """Scrape NBA injury reports using Playwright"""
    
    def __init__(self, headless: bool = True, timeout: int = 30000):
        """
        Initialize injury scraper
        
        Args:
            headless: Run browser in headless mode
            timeout: Page load timeout in milliseconds
        """
        self.headless = headless
        self.timeout = timeout
        self.browser: Optional[Browser] = None
        self.playwright = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
    
    async def scrape_espn_injuries(self) -> List[RawInjuryReport]:
        """Scrape injury reports from ESPN"""
        if not self.browser:
            raise RuntimeError("Browser not initialized. Use async context manager.")
        
        page = await self.browser.new_page()
        injuries = []
        
        try:
            # ESPN NBA injuries page
            url = "https://www.espn.com/nba/injuries"
            logger.info(f"Scraping injuries from {url}")
            
            await page.goto(url, wait_until="networkidle", timeout=self.timeout)
            
            # Wait for injury table to load
            await page.wait_for_selector("table", timeout=self.timeout)
            
            # Extract injury data from table
            injury_rows = await page.query_selector_all("tbody tr")
            
            for row in injury_rows:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 3:
                        continue
                    
                    # Extract player name and team
                    player_cell = await cells[0].inner_text()
                    player_parts = player_cell.strip().split("\n")
                    player_name = player_parts[0] if player_parts else "Unknown"
                    team_name = player_parts[1] if len(player_parts) > 1 else None
                    
                    # Extract status
                    status_cell = await cells[1].inner_text()
                    status = status_cell.strip()
                    
                    # Extract injury details
                    injury_cell = await cells[2].inner_text()
                    injury_parts = injury_cell.strip().split("\n")
                    injury_type = injury_parts[0] if injury_parts else None
                    diagnosis = injury_parts[1] if len(injury_parts) > 1 else None
                    
                    injuries.append(RawInjuryReport(
                        player_name=player_name,
                        team_name=team_name,
                        reported_at=datetime.now(),  # Use current time as reported_at
                        injury_type=injury_type,
                        diagnosis=diagnosis,
                        status=status,
                        source_url=url
                    ))
                except Exception as e:
                    logger.warning(f"Error parsing injury row: {e}")
                    continue
            
            logger.info(f"Scraped {len(injuries)} injuries from ESPN")
        except PlaywrightTimeoutError:
            logger.error(f"Timeout loading {url}")
        except Exception as e:
            logger.error(f"Error scraping ESPN injuries: {e}")
        finally:
            await page.close()
        
        return injuries
    
    async def scrape_rotowire_injuries(self) -> List[RawInjuryReport]:
        """Scrape injury reports from Rotowire"""
        if not self.browser:
            raise RuntimeError("Browser not initialized. Use async context manager.")
        
        page = await self.browser.new_page()
        injuries = []
        
        try:
            url = "https://www.rotowire.com/basketball/injury-report.php"
            logger.info(f"Scraping injuries from {url}")
            
            await page.goto(url, wait_until="networkidle", timeout=self.timeout)
            
            # Wait for injury table
            await page.wait_for_selector("table.injury-table", timeout=self.timeout)
            
            # Extract injury data
            injury_rows = await page.query_selector_all("tbody tr")
            
            for row in injury_rows:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 4:
                        continue
                    
                    # Extract player name
                    player_cell = await cells[0].inner_text()
                    player_name = player_cell.strip()
                    
                    # Extract team
                    team_cell = await cells[1].inner_text()
                    team_name = team_cell.strip()
                    
                    # Extract status
                    status_cell = await cells[2].inner_text()
                    status = status_cell.strip()
                    
                    # Extract injury details
                    injury_cell = await cells[3].inner_text()
                    injury_parts = injury_cell.strip().split(" - ")
                    injury_type = injury_parts[0] if injury_parts else None
                    diagnosis = injury_parts[1] if len(injury_parts) > 1 else None
                    
                    injuries.append(RawInjuryReport(
                        player_name=player_name,
                        team_name=team_name,
                        reported_at=datetime.now(),
                        injury_type=injury_type,
                        diagnosis=diagnosis,
                        status=status,
                        source_url=url
                    ))
                except Exception as e:
                    logger.warning(f"Error parsing injury row: {e}")
                    continue
            
            logger.info(f"Scraped {len(injuries)} injuries from Rotowire")
        except PlaywrightTimeoutError:
            logger.error(f"Timeout loading {url}")
        except Exception as e:
            logger.error(f"Error scraping Rotowire injuries: {e}")
        finally:
            await page.close()
        
        return injuries
    
    async def scrape_all_sources(self) -> List[RawInjuryReport]:
        """Scrape injuries from all available sources"""
        all_injuries = []
        
        # Scrape from multiple sources concurrently
        tasks = [
            self.scrape_espn_injuries(),
            self.scrape_rotowire_injuries()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, list):
                all_injuries.extend(result)
            elif isinstance(result, Exception):
                logger.error(f"Error in scraping task: {result}")
        
        # Deduplicate injuries (same player, similar status)
        unique_injuries = []
        seen_players = set()
        
        for injury in all_injuries:
            key = (injury.player_name.lower(), injury.status.lower())
            if key not in seen_players:
                seen_players.add(key)
                unique_injuries.append(injury)
        
        logger.info(f"Total unique injuries: {len(unique_injuries)}")
        return unique_injuries
