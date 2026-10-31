"""Apply the confirmed-usage UI contract to a separate code checkout after validation."""

import argparse
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from typing import Final


def replace(source: str, old: str, new: str) -> str:
    """Accept the original or already-patched form, rejecting unexpected source drift."""
    if old in source:
        if source.count(old) != 1:
            raise ValueError(f"Ambiguous patch anchor: {old!r}")
        return source.replace(old, new, 1)
    if new in source:
        return source
    raise ValueError(f"Patch anchor not found: {old!r}")


MODEL_EDITS: Final = (
    (
        "    input_tokens: number;\n    output_tokens: number;",
        "    input_tokens: number;\n    all_input_tokens: number;\n    output_tokens: number;",
    ),
    ("    cost_estimate_usd: string;", "    cost_estimate_usd: string | null;"),
    (
        "    estimated_calls: number;",
        "    confirmed_calls: number;\n    priced_calls: number;",
    ),
    (
        "    incomplete_calls: number;\n  };",
        "    incomplete_calls: number;\n  } | null;",
    ),
    (
        "    input_tokens_estimate: number | null;",
        "    input_tokens: number;\n    output_tokens: number;\n    catalog_model: string | null;",
    ),
)

COMPOSER_EDITS: Final = (
    ('import { object, text } from "../models/json";\n', ""),
    (
        "function cost(value: string | undefined)",
        "function cost(value: string | null | undefined)",
    ),
    ("  session,\n  history,\n", "  session,\n"),
    (
        "  const step = history.find(\n    (item) => object(item.step).id === context?.step_id,\n  );\n  const metrics = object(object(object(step?.step).metrics).llm);\n  const model = text(metrics.catalog_model);",
        "  const model = context?.catalog_model;",
    ),
    (
        "Latest main workflow input estimate / model context window.",
        "Last provider-measured main workflow input (including cached tokens) / model context window. New messages and in-flight requests are not included until measured.",
    ),
    (
        "Estimated total cost for this execution, including child workflows and retained branches.",
        "Estimated cost from finalized provider usage for this execution, including child workflows and retained branches.",
    ),
)

STATS_EDITS: Final = (
    (
        "        <dt>Input tokens</dt>",
        '        <dt>All input tokens (including cache)</dt>\n        <dd>{totals?.all_input_tokens.toLocaleString() ?? "—"}</dd>\n        <dt>Uncached input tokens</dt>',
    ),
    (
        '        <dd>{totals ? `$${totals.cost_estimate_usd}` : "—"}</dd>',
        '        <dd>{totals?.cost_estimate_usd != null ? `$${totals.cost_estimate_usd}` : "—"}</dd>',
    ),
    ("No usage reported yet.", "No finalized usage reported yet."),
    (
        "Usage includes child workflows and retained branches.",
        "Finalized provider usage includes child workflows and retained branches. In-flight estimates are excluded.",
    ),
)

REGRESSIONS: Final = """import { render, cleanup } from "@testing-library/react";
import { afterEach, expect, test } from "vitest";
import { ComposerUsage } from "../src/components/ComposerUsage";
import { WorkflowStats } from "../src/components/WorkflowStats";
import type { Session } from "../src/models/runner";

afterEach(cleanup);

const session: Session = {
  execution_id: "measured", reference: null, status: "running", active: true,
  pending_inputs: 0,
  usage: {
    totals: { input_tokens: 2000, all_input_tokens: 707000, output_tokens: 20,
      cost_estimate_usd: "0.07", confirmed_calls: 1, priced_calls: 1, incomplete_calls: 0 },
    root_context: { step_id: "measured", input_tokens: 704000, output_tokens: 20,
      catalog_model: "main-model", context_window: 1000000 },
  },
};

test("null totals and pricing are unavailable rather than measured zero", () => {
  const view = render(<ComposerUsage session={{usage: {totals: null, root_context: null}}} history={[]} />);
  expect(view.container.querySelector(".composer-cost")?.textContent).toBe("~$—");
  view.rerender(<ComposerUsage session={{usage: {...session.usage!, totals: {...session.usage!.totals!, cost_estimate_usd: null}}}} history={[]} />);
  expect(view.container.querySelector(".composer-cost")?.textContent).toBe("~$—");
  view.rerender(<ComposerUsage session={{usage: {...session.usage!, totals: {...session.usage!.totals!, cost_estimate_usd: "0"}}}} history={[]} />);
  expect(view.container.querySelector(".composer-cost")?.textContent).toBe("~$0");
});

test("pending estimates do not replace measurements; smaller measurements are accepted", () => {
  const view = render(<ComposerUsage session={session} history={[]} />);
  expect(view.getByLabelText("Workflow usage").textContent).toContain("main-model 704k / 1M");
  view.rerender(<ComposerUsage session={session} history={[{position: 1, step: {id: "pending", metrics: {llm: {input_tokens_estimate: 511000}}}}]} />);
  expect(view.getByLabelText("Workflow usage").textContent).toContain("main-model 704k / 1M");
  expect(view.getByLabelText("Workflow usage").title).toContain("Last provider-measured");
  view.rerender(<ComposerUsage session={{usage: {...session.usage!, root_context: {...session.usage!.root_context!, input_tokens: 100000}}}} history={[]} />);
  expect(view.getByLabelText("Workflow usage").textContent).toContain("100k / 1M");
});

test("stats show cache-inclusive input without losing uncached input", () => {
  const view = render(<WorkflowStats session={session} />);
  expect(view.getByText("All input tokens (including cache)").nextElementSibling?.textContent).toBe((707000).toLocaleString());
  expect(view.getByText("Uncached input tokens").nextElementSibling?.textContent).toBe((2000).toLocaleString());
  view.rerender(<WorkflowStats session={{...session, usage: {totals: null, root_context: null}}} />);
  expect(view.getByText("No finalized usage reported yet.")).toBeTruthy();
  view.rerender(<WorkflowStats session={{...session, usage: {...session.usage!, totals: {...session.usage!.totals!, cost_estimate_usd: null}}}} />);
  expect(view.getByText("Estimated cost (USD)").nextElementSibling?.textContent).toBe("—");
});
"""


def edits(source: str, replacements: tuple[tuple[str, str], ...]) -> str:
    for old, new in replacements:
        source = replace(source, old, new)
    return source


def fixture(source: str) -> str:
    """Upgrade only legacy-shaped fixture objects, leaving new fixtures unchanged."""
    if "estimated_calls:" not in source:
        return source
    source = re.sub(
        r"(?m)^(\s*)input_tokens: ([\d_]+),$",
        r"\1input_tokens: \2,\n\1all_input_tokens: \2,",
        source,
    )
    return re.sub(
        r"(?m)^(\s*)estimated_calls: (\d+),$",
        r"\1confirmed_calls: \2,\n\1priced_calls: \2,",
        source,
    )


def build_changes(root: Path) -> dict[Path, str]:
    """Validate every patch before returning any writes, preserving unrelated edits."""
    result: dict[Path, str] = {}
    files = (
        "src/models/runner.ts",
        "src/components/ComposerUsage.tsx",
        "src/components/WorkflowStats.tsx",
        "tests/composer-usage.test.tsx",
        "tests/runner-actions.test.ts",
        "tests/session-sync.test.ts",
        "tests/workspace-input.test.tsx",
    )
    for name in files:
        path = root / "frontend" / name
        source = path.read_text(encoding="utf-8")
        updated = source
        if name == "src/models/runner.ts":
            updated = edits(source, MODEL_EDITS)
        elif name == "src/components/ComposerUsage.tsx":
            if "const model = context?.catalog_model;" not in source:
                updated = edits(source, COMPOSER_EDITS)
            updated = updated.replace("input_tokens_estimate", "input_tokens")
            updated = updated.replace("usage?.totals.", "usage?.totals?.")
        elif name == "src/components/WorkflowStats.tsx":
            updated = edits(source, STATS_EDITS)
        else:
            updated = fixture(source)
            if name == "tests/workspace-input.test.tsx":
                updated = updated.replace(
                    "No usage reported yet.", "No finalized usage reported yet."
                )
            if name == "tests/composer-usage.test.tsx":
                updated = updated.replace(
                    "...session.usage!.totals,", "...session.usage!.totals!,"
                )
                for value, model in (
                    ("12_345", '"main-model"'),
                    ("450", '"new-main-model"'),
                    ("0", "null"),
                    ("input", '"main-model"'),
                ):
                    updated = updated.replace(
                        f"input_tokens_estimate: {value},",
                        f"input_tokens: {value},\n      output_tokens: 0,\n      catalog_model: {model},",
                    )
                updated = updated.replace('  [null, 200_000, "— / 200k"],\n', "")
        if updated != source:
            result[path] = updated
    regression = root / "frontend/tests/confirmed-usage.test.tsx"
    if regression.exists():
        if regression.read_text(encoding="utf-8") != REGRESSIONS:
            if (
                "pending estimates do not replace measurements"
                not in regression.read_text(encoding="utf-8")
            ):
                raise ValueError(f"Refusing to overwrite existing test: {regression}")
    else:
        result[regression] = REGRESSIONS
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, nargs="?", default=Path("~/work/code"))
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write validated changes; default is a dry run",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Test the patch in a temporary copy without changing the checkout",
    )
    args = parser.parse_args()
    root = args.root.expanduser().resolve()
    try:
        changes = build_changes(root)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"No files changed: {exc}\n")
    if args.validate:
        with tempfile.TemporaryDirectory(prefix="confirmed-usage-ui-") as directory:
            staging = Path(directory)
            frontend = staging / "frontend"
            shutil.copytree(
                root / "frontend",
                frontend,
                ignore=shutil.ignore_patterns("node_modules", "dist", ".git"),
            )
            (frontend / "node_modules").symlink_to(
                root / "frontend/node_modules", target_is_directory=True
            )
            for path, content in changes.items():
                (staging / path.relative_to(root)).write_text(content, encoding="utf-8")
            changed = [str(path.relative_to(root / "frontend")) for path in changes]
            commands = [
                ["npx", "--no-install", "prettier", "--write", *changed],
                [
                    "npm",
                    "test",
                    "--",
                    "tests/composer-usage.test.tsx",
                    "tests/confirmed-usage.test.tsx",
                    "tests/workspace-input.test.tsx",
                    "tests/runner-actions.test.ts",
                    "tests/session-sync.test.ts",
                ],
                ["npm", "run", "build"],
            ]
            for command in commands:
                subprocess.run(command, cwd=frontend, check=True)
        print("Validation passed in a temporary copy; original checkout unchanged.")
    for path, content in changes.items():
        print(
            f"{'Updating' if args.apply else 'Would update'} {path.relative_to(root)}"
        )
        if args.apply:
            path.write_text(content, encoding="utf-8")
    if not args.apply and changes:
        print("Dry run only. Re-run with --apply to write these changes.")
    elif not changes:
        print("UI usage patch is already applied.")


if __name__ == "__main__":
    main()
