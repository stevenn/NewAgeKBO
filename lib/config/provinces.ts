/**
 * Belgian Province Configuration
 * Maps postal code ranges to provinces for statistical analysis
 */

export interface ProvinceConfig {
  name: string;
  postalCodeRanges: { min: number; max: number }[];
}

export const BELGIAN_PROVINCES: ProvinceConfig[] = [
  {
    name: 'Brussels-Capital Region',
    postalCodeRanges: [{ min: 1000, max: 1299 }],
  },
  {
    name: 'Walloon Brabant',
    postalCodeRanges: [{ min: 1300, max: 1499 }],
  },
  {
    name: 'Flemish Brabant',
    postalCodeRanges: [
      { min: 1500, max: 1999 },
      { min: 3000, max: 3499 },
    ],
  },
  {
    name: 'Antwerp',
    postalCodeRanges: [{ min: 2000, max: 2999 }],
  },
  {
    name: 'Limburg',
    postalCodeRanges: [{ min: 3500, max: 3999 }],
  },
  {
    name: 'Liège',
    postalCodeRanges: [{ min: 4000, max: 4999 }],
  },
  {
    name: 'Namur',
    postalCodeRanges: [{ min: 5000, max: 5680 }],
  },
  {
    name: 'Hainaut',
    postalCodeRanges: [
      { min: 6000, max: 6599 },
      { min: 7000, max: 7999 },
    ],
  },
  {
    name: 'Luxembourg',
    postalCodeRanges: [{ min: 6600, max: 6999 }],
  },
  {
    name: 'West Flanders',
    postalCodeRanges: [{ min: 8000, max: 8999 }],
  },
  {
    name: 'East Flanders',
    postalCodeRanges: [{ min: 9000, max: 9999 }],
  },
];

/**
 * Generates a SQL CASE statement for mapping postal codes to provinces
 * Handles multiple ranges per province
 * Note: zipcode field is VARCHAR, so we cast to INTEGER for comparison
 */
export function generateProvinceSQLCase(): string {
  const cases = BELGIAN_PROVINCES.map((province) => {
    const conditions = province.postalCodeRanges
      .map((range) => `(TRY_CAST(zipcode AS INTEGER) BETWEEN ${range.min} AND ${range.max})`)
      .join(' OR ');
    return `WHEN ${conditions} THEN '${province.name}'`;
  }).join('\n      ');

  return `CASE
      ${cases}
      ELSE 'Unknown'
    END`;
}
