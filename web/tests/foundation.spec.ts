import { expect, test } from '@playwright/test';

test('anonymous browser can reach the app and proxied API without a database', async ({ page, request }) => {
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto('/');
  await expect(page.getByRole('heading', { level: 1, name: 'UrbanLens' })).toBeVisible();
  const response = await request.get('/health');
  expect(response.ok()).toBeTruthy();
  expect(await response.json()).toEqual({ status: 'ok', service: 'urbanlens-api' });
  expect(errors).toEqual([]);
});
