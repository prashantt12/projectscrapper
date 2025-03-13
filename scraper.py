import time
import pickle
import os
import json
import mysql.connector
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

# Load environment variables
load_dotenv()

# MySQL Configuration from .env
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

COOKIE_FILE = "linkedin_cookies.pkl"


# Initialize MySQL Connection
def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as err:
        print(f"Database connection error: {err}")
        return None


# Initialize Web Driver
def init_driver():
    options = uc.ChromeOptions()
    options.headless = False
    driver = uc.Chrome(options=options)
    return driver


# Load cookies into the browser
def load_cookies(driver):
    if os.path.exists(COOKIE_FILE):
        driver.get("https://www.linkedin.com")
        time.sleep(3)
        try:
            cookies = pickle.load(open(COOKIE_FILE, "rb"))
            for cookie in cookies:
                driver.add_cookie(cookie)
            driver.refresh()
            time.sleep(3)
        except Exception as e:
            print(f"Error loading cookies: {e}")


# Insert Company Data
def insert_company_data(cursor, data):
    query = """
        INSERT INTO companies (page_id, name, profile_picture, description, website, industry, head_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        name = VALUES(name), profile_picture = VALUES(profile_picture), 
        description = VALUES(description), website = VALUES(website), 
        industry = VALUES(industry), head_count = VALUES(head_count);
    """
    cursor.execute(query, (data["Page ID"], data["Page Name"], data["Profile Picture"],
                           data["Description"], data["Website"], data["Industry"], data["Head Count"]))


# Insert Employee Data
def insert_employee_data(cursor, company_id, employees):
    query = """
        INSERT INTO employees (company_id, name, profile_url, profile_picture, description)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        name = VALUES(name), profile_picture = VALUES(profile_picture), 
        description = VALUES(description);
    """
    for emp in employees:
        cursor.execute(query, (company_id, emp["Name"], emp["Profile URL"], emp["Profile Picture"], emp["Description"]))


# Insert Post Data
def insert_post_data(cursor, company_id, posts):
    query = """
        INSERT INTO posts (company_id, post_id, text, likes, comments, reposts)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE text = VALUES(text), likes = VALUES(likes), 
        comments = VALUES(comments), reposts = VALUES(reposts);
    """
    for post in posts:
        cursor.execute(query, (company_id, post["Post ID"], post["Text"], post["Likes"], post["Comments"], post["Reposts"]))


# Insert Media Data
def insert_media_data(cursor, post_id, media_links):
    query = """
        INSERT INTO media (post_id, media_url) VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE media_url = VALUES(media_url);
    """
    for media_url in media_links:
        cursor.execute(query, (post_id, media_url))


# Extract text based on label
def extract_text_by_label(soup, label):
    dt_tags = soup.find_all("dt", class_="mb1")
    for dt in dt_tags:
        if dt.find("h3") and dt.find("h3").text.strip() == label:
            dd_tag = dt.find_next_sibling("dd")
            return dd_tag.text.strip() if dd_tag else None
    return None


# Scrape Company Details
def scrape_main_page(driver, page_id):
    url = f"https://www.linkedin.com/company/{page_id}/"
    driver.get(url)
    time.sleep(5)
    
    soup = BeautifulSoup(driver.page_source, "html.parser")

    profile_img_tag = soup.find("img", {"class": "org-top-card-primary-content__logo"})
    profile_picture = profile_img_tag["src"] if profile_img_tag else None

    description_tag = soup.find("div", {"class": "organization-about-module__content-consistant-cards-description"})
    description = " ".join([span.text.strip() for span in description_tag.find_all("span")]) if description_tag else None

    return {
        "Page Name": soup.find("h1").text.strip() if soup.find("h1") else None,
        "Page ID": page_id,
        "Profile Picture": profile_picture,
        "Description": description,
    }


# Scrape About Page
def scrape_about_page(driver, page_id):
    url = f"https://www.linkedin.com/company/{page_id}/about/"
    driver.get(url)
    time.sleep(5)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    return {
        "Website": extract_text_by_label(soup, "Website"),
        "Industry": extract_text_by_label(soup, "Industry"),
        "Head Count": extract_text_by_label(soup, "Company size"),
    }


# Scrape Employees
def scrape_employees(driver, page_id):
    url = f"https://www.linkedin.com/company/{page_id}/people/"
    driver.get(url)
    time.sleep(5)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    employees = []

    for card in soup.find_all("li", class_="org-people-profile-card__profile-card-spacing"):
        profile_link_tag = card.find("a", {"data-test-app-aware-link": True})
        profile_link = profile_link_tag["href"].split("?")[0] if profile_link_tag else None

        img_tag = card.find("img", class_="evi-image")
        profile_picture = img_tag["src"] if img_tag else None

        name_tag = card.find("div", class_="lt-line-clamp--single-line")
        name = name_tag.get_text(strip=True) if name_tag else None

        description_tag = card.find("div", class_="lt-line-clamp--multi-line")
        description = description_tag.get_text(strip=True) if description_tag else None

        if name != "LinkedIn Member":
            employees.append({
                "Name": name,
                "Profile URL": profile_link,
                "Profile Picture": profile_picture,
                "Description": description
            })

    return employees


# Scrape Posts
def scrape_posts(driver, page_id, limit=20):
    url = f"https://www.linkedin.com/company/{page_id}/posts/"
    driver.get(url)
    time.sleep(5)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    posts_data = []

    for post in soup.find_all("div", class_="feed-shared-update-v2")[:limit]:
        post_id = post.get("data-urn", "").split(":")[-1] if post.get("data-urn") else None
        text_container = post.find("div", class_="update-components-text")
        post_text = text_container.text.strip() if text_container else None
        media_links = [img["src"] for img in post.find_all("img", class_="update-components-image__image")]

        posts_data.append({
            "Post ID": post_id,
            "Text": post_text,
            "Media Links": media_links,
            "Likes": 0,
            "Comments": 0,
            "Reposts": 0
        })

    return posts_data


# Scrape and Save to DB
def scrape_linkedin_page(page_id):
    driver = init_driver()
    load_cookies(driver)

    conn = get_db_connection()
    if conn is None:
        return

    cursor = conn.cursor()
    try:
        company_data = {**scrape_main_page(driver, page_id), **scrape_about_page(driver, page_id)}
        insert_company_data(cursor, company_data)
        company_id = cursor.lastrowid

        insert_employee_data(cursor, company_id, scrape_employees(driver, page_id))
        insert_post_data(cursor, company_id, scrape_posts(driver, page_id))

        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error:", e)
    finally:
        cursor.close()
        conn.close()
        driver.quit()


if __name__ == "__main__":
    scrape_linkedin_page("deepsolv")