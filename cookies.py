import time
import pickle
import os
import undetected_chromedriver as uc

COOKIE_FILE = "linkedin_cookies.pkl"

def save_cookies():
    driver = uc.Chrome()
    driver.get("https://www.linkedin.com/login")
    
    print("🔵 Please log in manually, then press Enter here...")
    input("⏳ Waiting for manual login...")

    # Save cookies
    pickle.dump(driver.get_cookies(), open(COOKIE_FILE, "wb"))
    print("✅ Cookies saved successfully!")

    driver.quit()

# Run this function once to save cookies
if __name__ == "__main__":
    save_cookies()
