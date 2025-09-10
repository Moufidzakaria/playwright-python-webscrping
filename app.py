import asyncio
import csv
import json
from playwright.async_api import async_playwright
from pymongo import MongoClient
import os

async def scrape_jumia(query="gas cooker", output_format="csv", max_pages=3):
    # الاتصال بـ MongoDB في GitHub Action (env variable)
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27018")
    client = MongoClient(mongo_uri)
    db = client["jumia_db"]
    collection = db["products"]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # headless=True pour GitHub Action
        page = await browser.new_page()
        await page.goto("https://www.jumia.co.ke/")

        await page.wait_for_selector('input[name="q"]')
        await page.fill('input[name="q"]', query)
        await page.press('input[name="q"]', 'Enter')
        await page.wait_for_selector('article.prd')

        products = []
        page_number = 1

        while page_number <= max_pages:
            print(f"Scraping page {page_number}...")
            product_elements = await page.query_selector_all('article.prd')

            for product in product_elements:
                try:
                    title = await (await product.query_selector('.name')).inner_text()
                    price = await (await product.query_selector('.prc')).inner_text()
                    discount_elem = await product.query_selector('.bdg._dsct')
                    discount = await discount_elem.inner_text() if discount_elem else "No discount"
                    link = await (await product.query_selector('a.core')).get_attribute('href')
                    image = await (await product.query_selector('img.img')).get_attribute('src')

                    product_data = {
                        "title": title,
                        "price": price,
                        "discount": discount,
                        "link": f"https://www.jumia.co.ke{link}",
                        "image": image
                    }

                    products.append(product_data)
                except Exception as e:
                    print(f"Error: {e}")
                    continue

            next_button = await page.query_selector('a[aria-label="Next Page"]')
            if not next_button:
                break
            await next_button.click()
            await page.wait_for_selector('article.prd')
            page_number += 1

        await browser.close()

        # حفظ البيانات في MongoDB
        if products:
            collection.insert_many(products)
            print(f"Inserted {len(products)} products into MongoDB")

        # حفظ CSV أو JSON
        if output_format == "csv":
            with open(f"{query}_products.csv", "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=["title", "price", "discount", "link", "image"])
                writer.writeheader()
                writer.writerows(products)
        elif output_format == "json":
            with open(f"{query}_products.json", "w", encoding="utf-8") as file:
                json.dump(products, file, indent=4)

        print(f"Scraped {len(products)} products. Data saved to {query}_products.{output_format}")

if __name__ == "__main__":
    asyncio.run(scrape_jumia())
