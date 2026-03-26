from playwright.sync_api import sync_playwright

def test():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto('https://app.kiro.dev/signin', wait_until='networkidle')
        buttons = page.locator('button').all_text_contents()
        links = page.locator('a').all_text_contents()
        print("Buttons:", buttons)
        print("Links:", links)
        browser.close()

if __name__ == "__main__":
    test()
