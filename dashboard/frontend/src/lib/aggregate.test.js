import { describe, it, expect } from "vitest";
import {
  bucketPoints,
  bucketCommentsBySentiment,
  buildSentimentChartRows,
} from "./aggregate.js";

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

describe("bucketCommentsBySentiment", () => {
  const comment = (utc, kw, label) => ({
    created_utc: utc,
    matched_keywords: [kw],
    sentiment_label: label,
  });

  it("counts labels per keyword and bucket", () => {
    const rows = bucketCommentsBySentiment(
      [
        comment(65, "apple", "positive"),
        comment(70, "apple", "negative"),
        comment(80, "android", "positive"),
      ],
      "apple",
      60,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].window_end).toBe(60);
    expect(rows[0]).toMatchObject({ positive: 1, negative: 1, neutral: 0 });
  });
});

describe("buildSentimentChartRows", () => {
  it("merges comment bars and window lines", () => {
    const comments = [
      {
        created_utc: 65,
        matched_keywords: ["apple"],
        sentiment_label: "positive",
      },
      {
        created_utc: 66,
        matched_keywords: ["apple"],
        sentiment_label: "negative",
      },
    ];
    const seriesA = [{ window_end: 60, positive_ratio: 0.5, comment_count: 2 }];
    const rows = buildSentimentChartRows(
      comments,
      seriesA,
      [],
      "apple",
      "android",
      60,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].a_positive).toBe(1);
    expect(rows[0].a_negative).toBe(1);
    expect(rows[0].apple).toBe(50);
  });
});
