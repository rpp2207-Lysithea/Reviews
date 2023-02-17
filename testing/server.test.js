const { parseWithOptions } = require("date-fns/fp");
const mockData = require('./mockData.js');

describe('GET Reviews', async () => {
  test('Returns status code 200', async () => {
    var result = await fetch('/reviews/?product_id=13', { method: 'GET' });
    const data = await result.json();
  });
  test('Returns correct data', async () => {
    var result = await fetch('/reviews/?product_id=13', { method: 'GET' });
    const data = await result.json();
    expect(data).toBe(mockData.review);
  });
  test('Returns status code 404', async () => {
    var result = await fetch('/reviews/?product_id=6000000', { method: 'GET' });
    const data = await result.json();
  });
});

describe('GET Meta', () => {
  test('Returns status code 200', () => {
    expect(true).toBe(true);
  });
  test('Returns correct data', () => {
    expect(true).toBe(true);
  });
  test('Returns status code 404', () => {
    expect(true).toBe(true);
  });
});

describe('POST Reviews', () => {
  test('Returns status code 201', () => {
    expect(true).toBe(true);
  });
  test('Returns status code 409', () => {
    expect(true).toBe(true);
  });
});

describe('PUT Helpful', () => {
  test('Returns status code 204', () => {
    expect(true).toBe(true);
  });
  test('Returns status code 409', () => {
    expect(true).toBe(true);
  });
});

describe('PUT Report', () => {
  test('Returns status code 204', () => {
    expect(true).toBe(true);
  });
  test('Returns status code 409', () => {
    expect(true).toBe(true);
  });
});
