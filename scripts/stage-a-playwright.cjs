// Runs inside a Seat only. The offline kit supplies pinned playwright@1.56.1.
const { chromium } = require('./browser-kit/node_modules/playwright');
const assert = require('node:assert/strict');
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const fs = require('node:fs/promises');

(async () => {
  const start = process.hrtime.bigint();
  const browser = await chromium.launch({headless: false, executablePath: '/usr/bin/chromium',
    args: ['--no-sandbox', '--disable-dev-shm-usage']});
  try {
    const page = await browser.newPage({viewport: {width: 1280, height: 700}, acceptDownloads: true});
    // Exactly the ten semantic steps used in SC-2. No remote page or DNS.
    await page.goto(pathToFileURL(path.join(__dirname, 'stage-a-browser.html')).href); // 1
    assert.equal(await page.title(), 'Stage A offline browser test');
    await page.locator('#name').fill('Ada'); // 2
    await page.locator('#code').fill('42'); // 3
    await page.locator('#offline').check(); // 4
    await page.locator('#color').selectOption('blue'); // 5
    await page.locator('#add').click(); // 6
    await page.locator('#add').click(); // 7
    await page.locator('#remove').click(); // 8
    await page.getByRole('button', {name: 'Submit', exact: true}).click(); // 9
    assert.match(await page.locator('#result').textContent(), /^PASS /);
    const waiting = page.waitForEvent('download');
    await page.locator('#download').click(); // 10
    await (await waiting).saveAs(path.join(__dirname, 'stage-a-playwright-result.json'));
    const result = JSON.parse(await fs.readFile(path.join(__dirname, 'stage-a-playwright-result.json'), 'utf8'));
    assert.deepEqual(result, {name: 'Ada', code: '42', offline: true, color: 'blue', count: 1, pass: true});
    await page.screenshot({path: path.join(__dirname, 'stage-a-playwright.png')});
    console.log(JSON.stringify({test: 'SC-3', steps: 10, elapsed_seconds: Number(process.hrtime.bigint() - start) / 1e9, result}));
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
