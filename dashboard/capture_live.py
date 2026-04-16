from playwright.sync_api import sync_playwright
import time

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1400, "height": 1800})
    page.goto('https://averroes-capital-123.streamlit.app/')
    print("Waiting for network idle...")
    page.wait_for_timeout(5000)
    print("Scrolling...")
    
    # Streamlit places main content in iframe or div with class 'stApp' or data-testid='stAppViewContainer'
    # Use javascript to scroll down
    page.evaluate("window.scrollTo(0, 2000)")
    page.mouse.wheel(0, 2000)
    page.wait_for_timeout(1000)
    
    # Also try to specifically locate section D
    try:
        page.locator("text='D. Product Usage'").scroll_into_view_if_needed()
    except:
        pass
        
    page.wait_for_timeout(1000)

    print("Taking screenshot...")
    page.screenshot(path='live_screenshot2.png')
    print("Screenshot saved to live_screenshot2.png")
    browser.close()

