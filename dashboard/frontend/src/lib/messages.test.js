import { describe, it, expect } from "vitest";
import { applyMessage, initialState } from "./messages.js";
import { wsUrl } from "./useWebSocket.js";

const win = (kw, end, ratio = 0.5) => ({
  type: "window",
  keyword: kw,
  window_end: end,
  positive_ratio: ratio,
  comment_count: 10,
});
const cmt = (id, kws = ["apple"]) => ({
  type: "comment",
  id,
  author: "u",
  body: "hi",
  created_utc: 1,
  matched_keywords: kws,
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
