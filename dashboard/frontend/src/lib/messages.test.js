import { describe, it, expect } from "vitest";
import {
  applyMessage,
  filterCommentsBySense,
  filterSeriesBySense,
  initialState,
  mergeSeries,
  seriesForBase,
} from "./messages.js";
import { wsUrl } from "./useWebSocket.js";

const win = (kw, end, ratio = 0.5, count = 10) => ({
  type: "window",
  keyword: kw,
  window_end: end,
  positive_ratio: ratio,
  comment_count: count,
});
const cmt = (id, kws = ["apple"], senses) => ({
  type: "comment",
  id,
  author: "u",
  body: "hi",
  created_utc: 1,
  matched_keywords: kws,
  ...(senses ? { keyword_senses: senses } : {}),
  sentiment_label: "positive",
  sentiment_score: 0.5,
});

describe("applyMessage - windows", () => {
  it("appends a point for a keyword", () => {
    const s = applyMessage(initialState, win("apple", 100));
    expect(s.windows.apple).toHaveLength(1);
    expect(s.windows.apple[0].positive_ratio).toBe(0.5);
  });

  it("replaces (not duplicates) the same window_end", () => {
    let s = applyMessage(initialState, win("apple", 100, 0.4));
    s = applyMessage(s, win("apple", 100, 0.9));
    expect(s.windows.apple).toHaveLength(1);
    expect(s.windows.apple[0].positive_ratio).toBe(0.9);
  });

  it("bounds the series to maxPoints", () => {
    let s = initialState;
    for (let i = 0; i < 10; i++) s = applyMessage(s, win("apple", i), { maxPoints: 3 });
    expect(s.windows.apple).toHaveLength(3);
    expect(s.windows.apple.map((p) => p.window_end)).toEqual([7, 8, 9]);
  });

  it("keys a sense-qualified window under its own literal key, not the base", () => {
    let s = applyMessage(initialState, win("apple:company", 100));
    s = applyMessage(s, win("apple:fruit", 100, 0.2));
    expect(s.windows["apple:company"]).toHaveLength(1);
    expect(s.windows["apple:fruit"]).toHaveLength(1);
    expect(s.windows.apple).toBeUndefined();
  });
});

describe("applyMessage - comments", () => {
  it("prepends newest first", () => {
    let s = applyMessage(initialState, cmt("a"));
    s = applyMessage(s, cmt("b"));
    expect(s.comments.map((c) => c.id)).toEqual(["b", "a"]);
  });

  it("dedupes by id", () => {
    let s = applyMessage(initialState, cmt("a"));
    s = applyMessage(s, cmt("a"));
    expect(s.comments).toHaveLength(1);
  });

  it("bounds the feed to maxComments", () => {
    let s = initialState;
    for (let i = 0; i < 10; i++) s = applyMessage(s, cmt(String(i)), { maxComments: 4 });
    expect(s.comments).toHaveLength(4);
  });

  it("passes through keyword_senses", () => {
    const s = applyMessage(
      initialState,
      cmt("a", ["apple"], { apple: "company" }),
    );
    expect(s.comments[0].keyword_senses).toEqual({ apple: "company" });
    // matched_keywords itself stays plain, never sense-qualified
    expect(s.comments[0].matched_keywords).toEqual(["apple"]);
  });

  it("defaults keyword_senses to an empty object when absent", () => {
    const s = applyMessage(initialState, cmt("a", ["android"]));
    expect(s.comments[0].keyword_senses).toEqual({});
  });
});

describe("seriesForBase", () => {
  it("returns a single plain entry for an unambiguous keyword", () => {
    const windows = { android: [{ window_end: 1, positive_ratio: 0.5, comment_count: 5 }] };
    const result = seriesForBase(windows, "android");
    expect(result).toEqual([
      { key: "android", base: "android", sense: null, points: windows.android },
    ]);
  });

  it("collects every sense-qualified series for an ambiguous keyword", () => {
    const windows = {
      "apple:company": [{ window_end: 1, positive_ratio: 0.8, comment_count: 5 }],
      "apple:fruit": [{ window_end: 1, positive_ratio: 0.3, comment_count: 2 }],
      android: [{ window_end: 1, positive_ratio: 0.5, comment_count: 5 }],
    };
    const result = seriesForBase(windows, "apple");
    expect(result.map((s) => s.key)).toEqual(["apple:company", "apple:fruit"]);
    expect(result.map((s) => s.sense)).toEqual(["company", "fruit"]);
  });

  it("returns an empty array when nothing is known for that base yet", () => {
    expect(seriesForBase({}, "apple")).toEqual([]);
  });
});

describe("mergeSeries", () => {
  it("weights positive_ratio by comment_count across senses at the same window", () => {
    const named = [
      { points: [{ window_end: 100, positive_ratio: 0.8, comment_count: 30 }] },
      { points: [{ window_end: 100, positive_ratio: 0.2, comment_count: 10 }] },
    ];
    const merged = mergeSeries(named);
    expect(merged).toHaveLength(1);
    // (0.8*30 + 0.2*10) / 40 = 26/40 = 0.65
    expect(merged[0].positive_ratio).toBeCloseTo(0.65);
    expect(merged[0].comment_count).toBe(40);
  });

  it("sorts the merged series chronologically by window_end", () => {
    const named = [
      { points: [{ window_end: 200, positive_ratio: 0.5, comment_count: 1 }] },
      { points: [{ window_end: 100, positive_ratio: 0.5, comment_count: 1 }] },
    ];
    const merged = mergeSeries(named);
    expect(merged.map((p) => p.window_end)).toEqual([100, 200]);
  });

  it("returns an empty array for no series", () => {
    expect(mergeSeries([])).toEqual([]);
  });
});

describe("filterSeriesBySense", () => {
  const named = [
    { key: "apple:company", base: "apple", sense: "company", points: [] },
    { key: "apple:fruit", base: "apple", sense: "fruit", points: [] },
    { key: "apple:ambiguous", base: "apple", sense: "ambiguous", points: [] },
  ];

  it("passes every series through when sense is 'all'", () => {
    expect(filterSeriesBySense(named, "all")).toEqual(named);
  });

  it("passes every series through when sense is falsy", () => {
    expect(filterSeriesBySense(named, null)).toEqual(named);
    expect(filterSeriesBySense(named, undefined)).toEqual(named);
  });

  it("narrows to the exact matching sense", () => {
    expect(filterSeriesBySense(named, "fruit")).toEqual([named[1]]);
  });

  it("treats 'ambiguous' as a normal, equal sense option", () => {
    expect(filterSeriesBySense(named, "ambiguous")).toEqual([named[2]]);
  });

  it("returns an empty array when nothing matches", () => {
    expect(filterSeriesBySense(named, "nope")).toEqual([]);
  });
});

describe("filterCommentsBySense", () => {
  const c = (id, kws, senses) => ({
    id,
    matched_keywords: kws,
    keyword_senses: senses || {},
  });

  it("matches today's behavior when every filter is 'all'", () => {
    const comments = [c("1", ["apple"], { apple: "fruit" }), c("2", ["android"])];
    const result = filterCommentsBySense(comments, ["apple", "android"], {
      apple: "all",
      android: "all",
    });
    expect(result.map((x) => x.id)).toEqual(["1", "2"]);
  });

  it("defaults to 'all' behavior when senseFilters is omitted entirely", () => {
    const comments = [c("1", ["apple"], { apple: "fruit" })];
    expect(filterCommentsBySense(comments, ["apple"]).map((x) => x.id)).toEqual(["1"]);
  });

  it("keeps only comments whose keyword_senses matches the selected sense", () => {
    const comments = [
      c("fruit", ["apple"], { apple: "fruit" }),
      c("company", ["apple"], { apple: "company" }),
    ];
    const result = filterCommentsBySense(comments, ["apple"], { apple: "fruit" });
    expect(result.map((x) => x.id)).toEqual(["fruit"]);
  });

  it("excludes a comment missing a keyword_senses entry for the filtered keyword", () => {
    const comments = [c("no-sense", ["apple"], {})];
    const result = filterCommentsBySense(comments, ["apple"], { apple: "fruit" });
    expect(result).toEqual([]);
  });

  it("ORs across two independently-filtered keywords", () => {
    const comments = [
      c("a-fruit", ["apple"], { apple: "fruit" }),
      c("a-company", ["apple"], { apple: "company" }),
      c("b-only", ["android"]),
    ];
    // apple filtered to "fruit", android left at "all"
    const result = filterCommentsBySense(comments, ["apple", "android"], {
      apple: "fruit",
      android: "all",
    });
    expect(result.map((x) => x.id).sort()).toEqual(["a-fruit", "b-only"]);
  });

  it("is case-insensitive on keyword matching, matching keyword_senses lookup by lowercase key", () => {
    const comments = [c("1", ["Apple"], { apple: "fruit" })];
    const result = filterCommentsBySense(comments, ["APPLE"], { apple: "fruit" });
    expect(result.map((x) => x.id)).toEqual(["1"]);
  });
});

describe("applyMessage - misc", () => {
  it("ignores unknown / empty messages", () => {
    expect(applyMessage(initialState, { type: "nope" })).toBe(initialState);
    expect(applyMessage(initialState, null)).toBe(initialState);
  });
});

describe("wsUrl", () => {
  it("uses ws:// for http", () => {
    expect(wsUrl({ protocol: "http:", host: "localhost:8000" })).toBe(
      "ws://localhost:8000/ws",
    );
  });
  it("uses wss:// for https", () => {
    expect(wsUrl({ protocol: "https:", host: "x.dev" })).toBe("wss://x.dev/ws");
  });
});
