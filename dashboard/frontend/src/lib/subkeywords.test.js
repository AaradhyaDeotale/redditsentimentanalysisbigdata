import { describe, it, expect } from "vitest";
import { parseSubkeywords, formatSubkeywords } from "./subkeywords.js";

describe("parseSubkeywords", () => {
  it("returns an empty list for empty/whitespace input", () => {
    expect(parseSubkeywords("")).toEqual([]);
    expect(parseSubkeywords("   ")).toEqual([]);
    expect(parseSubkeywords(undefined)).toEqual([]);
  });

  it("splits on commas and trims whitespace", () => {
    expect(parseSubkeywords("iphone,  ipad , macbook")).toEqual([
      "iphone",
      "ipad",
      "macbook",
    ]);
  });

  it("lowercases like main keyword normalization", () => {
    expect(parseSubkeywords("iPhone, IPAD")).toEqual(["iphone", "ipad"]);
  });

  it("drops empty entries from stray/trailing commas", () => {
    expect(parseSubkeywords("iphone,,  ,macbook,")).toEqual([
      "iphone",
      "macbook",
    ]);
  });

  it("dedupes while preserving first-seen order", () => {
    expect(parseSubkeywords("iphone, IPhone, ipad, iphone")).toEqual([
      "iphone",
      "ipad",
    ]);
  });
});

describe("formatSubkeywords", () => {
  it("joins with comma-space", () => {
    expect(formatSubkeywords(["iphone", "ipad"])).toBe("iphone, ipad");
  });

  it("handles empty/undefined", () => {
    expect(formatSubkeywords([])).toBe("");
    expect(formatSubkeywords(undefined)).toBe("");
  });
});
