from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def get_contact_data():
    """Reads contact data from the file."""
    with open("datos.contacto.whatsapp.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()
    data = {}
    for line in lines:
        if "Your Name:" in line:
            data["name"] = line.split(":")[1].strip()
        elif "Your Email:" in line:
            data["email"] = line.split(":")[1].strip()
        elif "Your Phone Number:" in line:
            data["phone"] = line.split(":")[1].strip()
        elif "The message you want to send:" in line:
            data["message"] = line.split(":")[1].strip()
    return data

def main():
    """Main function to automate contacting leads."""
    contact_data = get_contact_data()

    with open("urls.txt", "r") as f:
        urls = [line.strip() for line in f.readlines()]

    for url in urls:
        driver = webdriver.Chrome()
        driver.get(url)

        try:
            # 1. Click "Más información" button
            mas_info_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-testid='contact-button']"))
            )
            mas_info_button.click()

            # 2. Fill the form
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "name"))
            )
            driver.find_element(By.NAME, "name").send_keys(contact_data["name"])
            driver.find_element(By.NAME, "email").send_keys(contact_data["email"])
            driver.find_element(By.NAME, "phone").send_keys(contact_data["phone"])
            driver.find_element(By.NAME, "comment").send_keys(contact_data["message"])

            # 3. Accept terms and conditions
            terms_checkbox = driver.find_element(By.ID, "terms")
            driver.execute_script("arguments[0].click();", terms_checkbox)


            # 4. Click "Contactar por WhatsApp" button
            whatsapp_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "btn-lead-whatsapp-mobile"))
            )
            whatsapp_button.click()

            print(f"Successfully processed {url}")
            # Wait for a few seconds to allow the user to see the result
            # and for the whatsapp tab to open
            time.sleep(5)

        except Exception as e:
            print(f"An error occurred while processing {url}: {e}")
        finally:
            driver.quit()

if __name__ == "__main__":
    main()
