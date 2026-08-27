"""Test script to examine the Neowin registration page form structure."""
import asyncio
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from patchright.async_api import async_playwright
from app.common.config.settings import settings


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=settings.DEEP_CHROMIUM_ARGS,
        )
        ctx = await browser.new_context(**settings.DEEP_BROWSER_CONTEXT_KWARGS)
        await ctx.add_init_script(settings.DEEP_STEALTH_INIT_SCRIPT)
        page = await ctx.new_page()

        print("Navigating to Neowin registration...")
        await page.goto("https://www.neowin.net/forum/register/", wait_until="commit", timeout=30000)
        await page.wait_for_load_state("domcontentloaded", timeout=15000)

        # Humanize
        try:
            import random
            for _ in range(3):
                await page.mouse.move(random.randint(200, 800), random.randint(100, 500), steps=10)
                await asyncio.sleep(0.3)
        except Exception:
            pass

        await asyncio.sleep(5)

        # Wait for networkidle
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        await asyncio.sleep(3)

        title = await page.title()
        print(f"Page title: {title}")

        # Check for forms and inputs via JS
        result = await page.evaluate("""() => {
            const inputs = document.querySelectorAll('input');
            const forms = document.querySelectorAll('form');
            const selects = document.querySelectorAll('select');
            const textareas = document.querySelectorAll('textarea');
            const buttons = document.querySelectorAll('button');
            
            const inputDetails = [];
            inputs.forEach(i => {
                inputDetails.push({
                    name: i.name,
                    id: i.id,
                    type: i.type,
                    placeholder: i.placeholder,
                    visible: i.offsetParent !== null,
                    rect: i.getBoundingClientRect ? {
                        x: Math.round(i.getBoundingClientRect().x),
                        y: Math.round(i.getBoundingClientRect().y),
                        w: Math.round(i.getBoundingClientRect().width),
                        h: Math.round(i.getBoundingClientRect().height)
                    } : null
                });
            });
            
            const formDetails = [];
            forms.forEach(f => {
                formDetails.push({
                    action: f.action,
                    method: f.method,
                    id: f.id,
                    class: f.className,
                    innerHTML: f.innerHTML.substring(0, 2000)
                });
            });

            // Check shadow DOM
            const shadowHosts = [];
            function walkShadow(el, depth) {
                if (depth > 5) return;
                for (const child of el.children) {
                    if (child.shadowRoot) {
                        shadowHosts.push({
                            tag: child.tagName,
                            id: child.id,
                            class: child.className,
                            inputCount: child.shadowRoot.querySelectorAll('input').length,
                            formCount: child.shadowRoot.querySelectorAll('form').length
                        });
                        walkShadow(child.shadowRoot, depth + 1);
                    }
                    walkShadow(child, depth);
                }
            }
            walkShadow(document.body, 0);
            
            return {
                inputs: inputDetails,
                forms: formDetails,
                selects: selects.length,
                textareas: textareas.length,
                buttons: buttons.length,
                shadowHosts: shadowHosts,
                bodyTextLength: (document.body?.innerText || '').length,
                bodyTextSample: (document.body?.innerText || '').substring(0, 1000),
                title: document.title,
                url: window.location.href
            };
        }""")
        
        print(f"\nURL: {result['url']}")
        print(f"Title: {result['title']}")
        print(f"Body text length: {result['bodyTextLength']}")
        print(f"Body text sample:\n{result['bodyTextSample']}")
        print(f"\nForms: {len(result['forms'])}")
        for f in result['forms']:
            print(f"  Form action={f['action']} method={f['method']} id={f['id']}")
            print(f"  InnerHTML (first 500): {f['innerHTML'][:500]}")
        print(f"\nInputs: {len(result['inputs'])}")
        for i in result['inputs']:
            print(f"  Input name={i['name']} id={i['id']} type={i['type']} placeholder={i['placeholder']} visible={i['visible']} rect={i['rect']}")
        print(f"\nSelects: {result['selects']}")
        print(f"Textareas: {result['textareas']}")
        print(f"Buttons: {result['buttons']}")
        print(f"\nShadow DOM hosts: {len(result['shadowHosts'])}")
        for s in result['shadowHosts']:
            print(f"  Host tag={s['tag']} id={s['id']} class={s['class']} inputs={s['inputCount']} forms={s['formCount']}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
