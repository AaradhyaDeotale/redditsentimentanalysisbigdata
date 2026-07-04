import { describe, it, expect } from "vitest";
import { bucketPoints } from "./aggregate.js";

const pt = (end, ratio, count) => ({
  window_end: end,
  positive_ratio: ratio,
  comment_count: count,
});

describe("bucketPoints", () => {
  it("returns points unchanged for empty input", () => {
    expect(bucketPoints([], 60)).toEqual([]);
  });

  it("is a no-op for bucketSeconds <= 1", () => {
    const points = [pt(1, 0.5, 10)];
    expect(bucketPoints(points, 1)).toBe(points);
    expect(bucketPoints(points, 0)).toBe(points);
  });

  it("merges points into fixed-size buckets, weighted by comment_count", () => {
    // Two 1-minute windows inside the same 5-minute bucket [0, 300).
    const points = [pt(60, 0.2, 10), pt(120, 0.8, 30)];
    const bucketed = bucketPoints(points, 300);
    expect(bucketed).toHaveLength(1);
    expect(bucketed[0].window_end).toBe(0);
    expect(bucketed[0].comment_count).toBe(40);
    // Weighted average: (0.2*10 + 0.8*30) / 40 = 0.65
    expect(bucketed[0].positive_ratio).toBeCloseTo(0.65);
  });

  it("keeps points in separate buckets when they fall in different windows", () => {
    const points = [pt(60, 0.2, 10), pt(400, 0.8, 10)];
    const bucketed = bucketPoints(points, 300);
    expect(bucketed.map((b) => b.window_end)).toEqual([0, 300]);
  });

  it("sorts output by window_end", () => {
    const points = [pt(400, 0.8, 10), pt(60, 0.2, 10)];
    const bucketed = bucketPoints(points, 300);
    expect(bucketed.map((b) => b.window_end)).toEqual([0, 300]);
  });

  it("treats zero comment_count as a weight of 1, not 0", () => {
    const points = [pt(60, 1, 0)];
    const bucketed = bucketPoints(points, 300);
    expect(bucketed[0].positive_ratio).toBe(1);
    expect(bucketed[0].comment_count).toBe(0);
  });
});
