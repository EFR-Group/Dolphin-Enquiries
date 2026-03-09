export type PerDateEnquiryCount = { date: string; leisureCount: number; golfCount: number };

export function totalEnquiries(perDateCounts: PerDateEnquiryCount[]): number {
  return perDateCounts.reduce((sum, d) => sum + d.leisureCount + d.golfCount, 0);
}

/**
 * Merges multiple cached snapshots of per-day enquiry counts, preferring the latest snapshot per date.
 *
 * This is used for weekly reporting because each day's cache can include a lookback window (e.g. the last N days)
 * to catch late-arriving files; we want one final value per date, not duplicates or sums across snapshots.
 *
 * @param snapshots - Array of cached snapshots, identified by `snapshotKey` in YYYYMMDD format.
 * @param allowedDates - Optional whitelist of `date` values (YYYY-MM-DD) to include in the output.
 */
export function mergePerDateCountsPreferLatestSnapshot(
  snapshots: Array<{ snapshotKey: string; perDateCounts: PerDateEnquiryCount[] }>,
  allowedDates?: Set<string>
): PerDateEnquiryCount[] {
  const latestByDate = new Map<string, { snapshotKey: string; leisureCount: number; golfCount: number }>();

  for (const snapshot of snapshots) {
    for (const entry of snapshot.perDateCounts) {
      if (allowedDates && !allowedDates.has(entry.date)) continue;

      const existing = latestByDate.get(entry.date);
      if (!existing || snapshot.snapshotKey >= existing.snapshotKey) {
        latestByDate.set(entry.date, {
          snapshotKey: snapshot.snapshotKey,
          leisureCount: entry.leisureCount,
          golfCount: entry.golfCount,
        });
      }
    }
  }

  return Array.from(latestByDate.entries())
    .map(([date, v]) => ({ date, leisureCount: v.leisureCount, golfCount: v.golfCount }))
    .sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
}

