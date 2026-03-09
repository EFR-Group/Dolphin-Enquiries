import { mergePerDateCountsPreferLatestSnapshot } from "./reporting";

describe("mergePerDateCountsPreferLatestSnapshot", () => {
  test("prefers the latest snapshot per date (no double counting)", () => {
    const merged = mergePerDateCountsPreferLatestSnapshot(
      [
        {
          snapshotKey: "20260303",
          perDateCounts: [
            { date: "2026-03-03", leisureCount: 1, golfCount: 2 },
            { date: "2026-03-04", leisureCount: 3, golfCount: 4 },
          ],
        },
        {
          snapshotKey: "20260304",
          perDateCounts: [
            { date: "2026-03-03", leisureCount: 10, golfCount: 20 }, // updated
          ],
        },
      ],
      new Set(["2026-03-03", "2026-03-04"])
    );

    expect(merged).toEqual([
      { date: "2026-03-03", leisureCount: 10, golfCount: 20 },
      { date: "2026-03-04", leisureCount: 3, golfCount: 4 },
    ]);
  });

  test("filters to allowedDates when provided", () => {
    const merged = mergePerDateCountsPreferLatestSnapshot(
      [
        {
          snapshotKey: "20260305",
          perDateCounts: [
            { date: "2026-03-05", leisureCount: 1, golfCount: 1 },
            { date: "2026-03-06", leisureCount: 2, golfCount: 2 },
          ],
        },
      ],
      new Set(["2026-03-06"])
    );

    expect(merged).toEqual([{ date: "2026-03-06", leisureCount: 2, golfCount: 2 }]);
  });
});
