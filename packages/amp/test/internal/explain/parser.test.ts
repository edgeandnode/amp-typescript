import type { ExplainRow, PlanNode } from "@edgeandnode/amp/arrow-flight"
import { parsePlan, planToTable, prefixExplain } from "@edgeandnode/amp/internal/explain/parser"
import { describe, expect, it } from "vitest"

const cell = (row: ExplainRow, key: string): unknown => (row as Record<string, unknown>)[key]

// Real EXPLAIN ANALYZE output captured from Amp. Two nodes:
// `CoalescePartitionsExec` at depth 0, `DataSourceExec` at depth 1.
const REAL_PLAN =
  "CoalescePartitionsExec: fetch=10, metrics=[output_rows=10, elapsed_compute=9.59µs, output_bytes=1376.0 B, output_batches=1]\n" +
  "  DataSourceExec: file_groups={4 groups: [[000000000-4b156bc5f8c4b5c9.parquet:0..34765529, 046041222-7dc8e8e88196fb16.parquet:0..3289, 046041316-214bfa0c4faa7b3c.parquet:0..12946, 046041384-7686b0192d63ea56.parquet:0..3188, 046041393-58281870e72806fe.parquet:0..3189, ...], [035000000-3804cfa716034dd4.parquet:2062293511..4159378225], [035000000-3804cfa716034dd4.parquet:4159378225..6256462939], [035000000-3804cfa716034dd4.parquet:6256462939..6599305879, 046041223-c398ae24960243cf.parquet:0..3190, 046041379-42c6761e45c71a94.parquet:0..3188, 046041385-9130b047b7c4a25f.parquet:0..3061, 046041394-cf85f4d98090f0a3.parquet:0..3062, ...]]}, projection=[_block_num, timestamp, block_num, tx_hash, buyer_address, seller_address, value_usdc, nonce], limit=10, file_type=parquet, metrics=[output_rows=10, elapsed_compute=4ns, output_bytes=1376.0 B, output_batches=1, files_ranges_pruned_statistics=6 total → 6 matched, row_groups_pruned_statistics=74 total → 74 matched, row_groups_pruned_bloom_filter=74 total → 74 matched, page_index_pages_pruned=0 total → 0 matched, page_index_rows_pruned=0 total → 0 matched, limit_pruned_row_groups=0 total → 0 matched, batches_split=0, bytes_scanned=13.37 M, file_open_errors=0, file_scan_errors=0, num_predicate_creation_errors=0, predicate_evaluation_errors=0, pushdown_rows_matched=0, pushdown_rows_pruned=0, predicate_cache_inner_records=0, predicate_cache_records=0, bloom_filter_eval_time=12ns, metadata_load_time=288.01µs, page_index_eval_time=12ns, row_pushdown_eval_time=12ns, statistics_eval_time=12ns, time_elapsed_opening=415.35µs, time_elapsed_processing=43.90ms, time_elapsed_scanning_total=230.61ms, time_elapsed_scanning_until_data=230.60ms, scan_efficiency_ratio=N/A (0/0)]\n"

describe("parsePlan", () => {
  it("returns an empty list for empty input", () => {
    expect(parsePlan("")).toEqual([])
    expect(parsePlan("   \n  \n")).toEqual([])
  })

  it("parses the real plan into 2 nodes with depths and properties", () => {
    const nodes = parsePlan(REAL_PLAN)
    expect(nodes).toHaveLength(2)

    const root = nodes[0]!
    expect(root.name).toBe("CoalescePartitionsExec")
    expect(root.depth).toBe(0)
    expect(root.properties).toEqual({ fetch: "10" })
    expect(Object.keys(root.metrics)).toEqual([
      "output_rows",
      "elapsed_compute",
      "output_bytes",
      "output_batches"
    ])

    const child = nodes[1]!
    expect(child.name).toBe("DataSourceExec")
    expect(child.depth).toBe(1)
    expect(child.properties["limit"]).toBe("10")
    expect(child.properties["file_type"]).toBe("parquet")
    expect(child.properties["projection"]).toBe(
      "[_block_num, timestamp, block_num, tx_hash, buyer_address, seller_address, value_usdc, nonce]"
    )
    // file_groups is a structured value with nested brackets — must survive
    // the parser without being split on internal commas.
    expect(child.properties["file_groups"]?.startsWith("{4 groups:")).toBe(true)
    expect(child.properties["file_groups"]?.endsWith("]]}")).toBe(true)
  })

  it("parses a node with no properties", () => {
    const nodes = parsePlan("ProjectionExec\n")
    expect(nodes).toHaveLength(1)
    expect(nodes[0]).toMatchObject<Partial<PlanNode>>({
      name: "ProjectionExec",
      depth: 0,
      properties: {},
      metrics: {}
    })
  })

  it("parses a node with metrics but no extra properties", () => {
    const nodes = parsePlan("CoalesceBatchesExec: target_batch_size=8192, metrics=[output_rows=42]\n")
    expect(nodes[0]!.properties).toEqual({ target_batch_size: "8192" })
    expect(nodes[0]!.metrics).toEqual({ output_rows: "42" })
  })
})

describe("planToTable", () => {
  it("returns empty rows + columns for an empty plan", () => {
    const result = planToTable([])
    expect(result.rows).toEqual([])
    expect(result.columns).toEqual([])
  })

  it("normalizes the real plan into a table with expected cells", () => {
    const result = planToTable(parsePlan(REAL_PLAN))
    expect(result.rows).toHaveLength(2)

    const row0 = result.rows[0]!
    expect(cell(row0, "node")).toBe("CoalescePartitionsExec")
    expect(cell(row0, "depth")).toBe(0)
    expect(cell(row0, "fetch")).toBe(10)
    expect(cell(row0, "output_rows")).toBe(10)
    expect(cell(row0, "elapsed_compute_secs")).toBeCloseTo(9.59e-6, 12)
    expect(cell(row0, "output_bytes")).toBe("1376.0 B")
    expect(cell(row0, "output_batches")).toBe(1)
    // Duration was renamed; the unsuffixed key must not also be present.
    expect("elapsed_compute" in row0).toBe(false)

    const row1 = result.rows[1]!
    expect(cell(row1, "node")).toBe("DataSourceExec")
    expect(cell(row1, "depth")).toBe(1)
    expect(cell(row1, "limit")).toBe(10)
    // file_groups is "{4 groups: ..." — leading-number extraction yields 4.
    expect(cell(row1, "file_groups")).toBe(4)
    // Non-numeric properties are dropped from the table.
    expect("file_type" in row1).toBe(false)
    expect("projection" in row1).toBe(false)

    expect(cell(row1, "output_rows")).toBe(10)
    expect(cell(row1, "output_bytes")).toBe("1376.0 B")
    expect(cell(row1, "output_batches")).toBe(1)
    expect(cell(row1, "elapsed_compute_secs")).toBeCloseTo(4e-9, 18)

    // total → matched expansions
    expect(cell(row1, "files_ranges_pruned_statistics_total")).toBe(6)
    expect(cell(row1, "files_ranges_pruned_statistics_matched")).toBe(6)
    expect(cell(row1, "row_groups_pruned_statistics_total")).toBe(74)
    expect(cell(row1, "row_groups_pruned_statistics_matched")).toBe(74)
    expect(cell(row1, "row_groups_pruned_bloom_filter_total")).toBe(74)
    expect(cell(row1, "row_groups_pruned_bloom_filter_matched")).toBe(74)
    expect(cell(row1, "page_index_pages_pruned_total")).toBe(0)
    expect(cell(row1, "page_index_pages_pruned_matched")).toBe(0)
    expect(cell(row1, "page_index_rows_pruned_total")).toBe(0)
    expect(cell(row1, "page_index_rows_pruned_matched")).toBe(0)
    expect(cell(row1, "limit_pruned_row_groups_total")).toBe(0)
    expect(cell(row1, "limit_pruned_row_groups_matched")).toBe(0)

    // Plain numbers
    expect(cell(row1, "batches_split")).toBe(0)
    expect(cell(row1, "file_open_errors")).toBe(0)
    expect(cell(row1, "file_scan_errors")).toBe(0)
    expect(cell(row1, "num_predicate_creation_errors")).toBe(0)
    expect(cell(row1, "predicate_evaluation_errors")).toBe(0)
    expect(cell(row1, "pushdown_rows_matched")).toBe(0)
    expect(cell(row1, "pushdown_rows_pruned")).toBe(0)
    expect(cell(row1, "predicate_cache_inner_records")).toBe(0)
    expect(cell(row1, "predicate_cache_records")).toBe(0)

    // M-suffix expansion
    expect(cell(row1, "bytes_scanned")).toBe(13.37e6)

    // Duration metrics
    expect(cell(row1, "bloom_filter_eval_time_secs")).toBeCloseTo(12e-9, 18)
    expect(cell(row1, "metadata_load_time_secs")).toBeCloseTo(288.01e-6, 12)
    expect(cell(row1, "page_index_eval_time_secs")).toBeCloseTo(12e-9, 18)
    expect(cell(row1, "row_pushdown_eval_time_secs")).toBeCloseTo(12e-9, 18)
    expect(cell(row1, "statistics_eval_time_secs")).toBeCloseTo(12e-9, 18)
    expect(cell(row1, "time_elapsed_opening_secs")).toBeCloseTo(415.35e-6, 12)
    expect(cell(row1, "time_elapsed_processing_secs")).toBeCloseTo(43.9e-3, 9)
    expect(cell(row1, "time_elapsed_scanning_total_secs")).toBeCloseTo(230.61e-3, 9)
    expect(cell(row1, "time_elapsed_scanning_until_data_secs")).toBeCloseTo(230.6e-3, 9)

    // N/A → null
    expect(cell(row1, "scan_efficiency_ratio")).toBeNull()
  })

  it("emits columns in first-appearance order across rows", () => {
    const result = planToTable(parsePlan(REAL_PLAN))
    // Root-only columns come first, then DataSourceExec-only columns appear
    // when they're first introduced by row 1.
    expect(result.columns.slice(0, 7)).toEqual([
      "node",
      "depth",
      "fetch",
      "output_rows",
      "elapsed_compute_secs",
      "output_bytes",
      "output_batches"
    ])
    // `limit` is the first row-1-only property, immediately after the shared
    // root metric columns.
    const limitIdx = result.columns.indexOf("limit")
    expect(limitIdx).toBeGreaterThan(6)
    // No duplicates.
    expect(result.columns.length).toBe(new Set(result.columns).size)
    // Every row's keys are a subset of `columns`.
    for (const row of result.rows) {
      for (const key of Object.keys(row)) {
        expect(result.columns).toContain(key)
      }
    }
  })

  it("parses 'P% (out/in)' selectivity metrics as plain percent floats", () => {
    const result = planToTable(parsePlan("Foo: x=1, metrics=[selectivity=12.5% (1/8)]\n"))
    expect(cell(result.rows[0]!, "selectivity")).toBe(12.5)
  })

  it("falls back to the raw string for an unparseable metric", () => {
    const result = planToTable(parsePlan("Foo: x=1, metrics=[shape=triangle]\n"))
    expect(cell(result.rows[0]!, "shape")).toBe("triangle")
  })
})

describe("prefixExplain", () => {
  it("prepends EXPLAIN when the SQL has no leading EXPLAIN", () => {
    expect(prefixExplain("SELECT 1", false)).toBe("EXPLAIN SELECT 1")
  })

  it("prepends EXPLAIN ANALYZE when analyze is true", () => {
    expect(prefixExplain("SELECT 1", true)).toBe("EXPLAIN ANALYZE SELECT 1")
  })

  it("returns the SQL unchanged if it already starts with EXPLAIN", () => {
    expect(prefixExplain("EXPLAIN SELECT 1", false)).toBe("EXPLAIN SELECT 1")
    // The user's own EXPLAIN wins, even when analyze is requested — we don't
    // try to upgrade EXPLAIN to EXPLAIN ANALYZE silently.
    expect(prefixExplain("EXPLAIN SELECT 1", true)).toBe("EXPLAIN SELECT 1")
  })

  it("returns the SQL unchanged if it already starts with EXPLAIN ANALYZE", () => {
    expect(prefixExplain("EXPLAIN ANALYZE SELECT 1", true)).toBe("EXPLAIN ANALYZE SELECT 1")
    expect(prefixExplain("EXPLAIN ANALYZE SELECT 1", false)).toBe("EXPLAIN ANALYZE SELECT 1")
  })

  it("matches case-insensitively and tolerates leading whitespace", () => {
    expect(prefixExplain("explain analyze select 1", false)).toBe("explain analyze select 1")
    expect(prefixExplain("  ExPlAiN SELECT 1", true)).toBe("  ExPlAiN SELECT 1")
  })

  it("does not match identifiers that merely start with the letters EXPLAIN", () => {
    // No word boundary after EXPLAIN — should be treated as a normal query.
    expect(prefixExplain("EXPLAINER FROM foo", false)).toBe("EXPLAIN EXPLAINER FROM foo")
  })
})
