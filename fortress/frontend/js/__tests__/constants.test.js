/**
 * constants.test.js — Sanity checks for French department constants
 */
import { describe, it, expect } from 'vitest';
import { DEPARTMENTS, DEPT_NAMES, deptName, deptLabel } from '../constants.js';

describe('DEPARTMENTS array', () => {
    it('contains at least 96 entries (metropolitan France + Corsica)', () => {
        expect(DEPARTMENTS.length).toBeGreaterThanOrEqual(96);
    });

    it('each entry is a [code, name] tuple', () => {
        for (const entry of DEPARTMENTS) {
            expect(Array.isArray(entry)).toBe(true);
            expect(entry).toHaveLength(2);
            expect(typeof entry[0]).toBe('string');
            expect(typeof entry[1]).toBe('string');
            expect(entry[0].length).toBeGreaterThanOrEqual(1);
            expect(entry[1].length).toBeGreaterThan(0);
        }
    });

    it('has no duplicate department codes', () => {
        const codes = DEPARTMENTS.map(d => d[0]);
        expect(new Set(codes).size).toBe(codes.length);
    });

    it('includes Corsica departments 2A and 2B', () => {
        const codes = DEPARTMENTS.map(d => d[0]);
        expect(codes).toContain('2A');
        expect(codes).toContain('2B');
    });
});

describe('DEPT_NAMES lookup', () => {
    it('is an object with all department codes as keys', () => {
        expect(typeof DEPT_NAMES).toBe('object');
        expect(Object.keys(DEPT_NAMES).length).toBe(DEPARTMENTS.length);
    });

    it('maps "75" to "Paris"', () => {
        expect(DEPT_NAMES['75']).toBe('Paris');
    });

    it('maps "2A" to "Corse-du-Sud"', () => {
        expect(DEPT_NAMES['2A']).toBe('Corse-du-Sud');
    });
});

describe('deptName()', () => {
    it('returns "Paris" for code "75"', () => {
        expect(deptName('75')).toBe('Paris');
    });

    it('returns "Aude" for code "11"', () => {
        expect(deptName('11')).toBe('Aude');
    });

    it('returns the code itself for unknown codes', () => {
        expect(deptName('99')).toBe('99');
        expect(deptName('ZZ')).toBe('ZZ');
    });
});

describe('deptLabel()', () => {
    it('returns "75 — Paris" for code "75"', () => {
        expect(deptLabel('75')).toBe('75 — Paris');
    });

    it('returns "2A — Corse-du-Sud" for code "2A"', () => {
        expect(deptLabel('2A')).toBe('2A — Corse-du-Sud');
    });

    it('returns just the code for unknown codes', () => {
        expect(deptLabel('ZZ')).toBe('ZZ');
    });
});
