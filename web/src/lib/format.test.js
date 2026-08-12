import { describe, it, expect } from 'vitest';
import {
  formatUptime,
  formatKb,
  formatMib,
  memUsedPct,
  formatTimestamp,
  formatMetricValue,
  metricRows,
} from './format.js';

describe('formatUptime', () => {
  it('returns em dash for null', () => {
    expect(formatUptime(null)).toBe('—');
  });

  it('formats minutes only', () => {
    expect(formatUptime(5 * 60)).toBe('5m');
  });

  it('formats hours and minutes', () => {
    expect(formatUptime(2 * 3600 + 5 * 60)).toBe('2h 5m');
  });

  it('formats days and hours', () => {
    expect(formatUptime(3 * 86400 + 4 * 3600 + 5 * 60)).toBe('3d 4h');
  });
});

describe('formatKb', () => {
  it('returns em dash for null', () => {
    expect(formatKb(null)).toBe('—');
  });

  it('rounds to whole GiB at/above the 10-GiB threshold', () => {
    expect(formatKb(10 * 1024 * 1024)).toBe('10 GiB');
    expect(formatKb(15.6 * 1024 * 1024)).toBe('16 GiB');
  });

  it('shows one decimal below the 10-GiB threshold', () => {
    expect(formatKb(5 * 1024 * 1024)).toBe('5.0 GiB');
    expect(formatKb(9.5 * 1024 * 1024)).toBe('9.5 GiB');
  });
});

describe('formatMib', () => {
  it('returns em dash for null', () => {
    expect(formatMib(null)).toBe('—');
  });

  it('rounds to whole GiB at/above the 10-GiB threshold', () => {
    expect(formatMib(10 * 1024)).toBe('10 GiB');
    expect(formatMib(15.6 * 1024)).toBe('16 GiB');
  });

  it('shows one decimal below the 10-GiB threshold', () => {
    expect(formatMib(5 * 1024)).toBe('5.0 GiB');
    expect(formatMib(9.5 * 1024)).toBe('9.5 GiB');
  });
});

describe('memUsedPct', () => {
  it('returns null when total is null or zero', () => {
    expect(memUsedPct(null, 100)).toBeNull();
    expect(memUsedPct(0, 100)).toBeNull();
  });

  it('returns null when available is null', () => {
    expect(memUsedPct(1000, null)).toBeNull();
  });

  it('computes percentage used', () => {
    expect(memUsedPct(1000, 250)).toBe(75);
  });
});

describe('formatTimestamp', () => {
  it('returns em dash for null or empty string', () => {
    expect(formatTimestamp(null)).toBe('—');
    expect(formatTimestamp('')).toBe('—');
  });

  it('returns the original string for an invalid ISO string', () => {
    expect(formatTimestamp('not-a-date')).toBe('not-a-date');
  });

  it('returns a locale time string for a valid ISO string', () => {
    const result = formatTimestamp('2024-01-01T12:00:00Z');
    expect(typeof result).toBe('string');
    expect(result).not.toBe('2024-01-01T12:00:00Z');
    expect(result.length).toBeGreaterThan(0);
  });
});

describe('formatMetricValue', () => {
  it('localizes integers', () => {
    expect(formatMetricValue(1234567)).toBe((1234567).toLocaleString());
  });

  it('uses exponential notation for very small floats', () => {
    expect(formatMetricValue(0.0005)).toBe((0.0005).toExponential(2));
  });

  it('uses 4 decimal places for floats >= 0.01', () => {
    expect(formatMetricValue(0.123456)).toBe((0.123456).toFixed(4));
  });

  it('passes strings through unchanged', () => {
    expect(formatMetricValue('epoch-3')).toBe('epoch-3');
  });

  it('JSON-stringifies objects', () => {
    expect(formatMetricValue({ a: 1 })).toBe(JSON.stringify({ a: 1 }));
  });
});

describe('metricRows', () => {
  it('returns [] for non-object, array, or null input', () => {
    expect(metricRows(null)).toEqual([]);
    expect(metricRows(undefined)).toEqual([]);
    expect(metricRows('str')).toEqual([]);
    expect(metricRows(42)).toEqual([]);
    expect(metricRows([1, 2, 3])).toEqual([]);
  });

  it('flattens an object into [key, formattedValue] pairs', () => {
    expect(metricRows({ loss: 0.123456, epoch: 3, tag: 'x' })).toEqual([
      ['loss', formatMetricValue(0.123456)],
      ['epoch', formatMetricValue(3)],
      ['tag', 'x'],
    ]);
  });
});
