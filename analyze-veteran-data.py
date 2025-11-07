from __future__ import annotations

from pathlib import Path
import pandas as pd
"""analyze veteran data and produce per-100k and percentage CSVs for various metrics."""

METRICS = [
	"allVeterans",
	"peacetimeVeterans",
	"wartimeVeterans",
	"ww2",
	"koreanWar",
	"vietnamWar",
	"gulfWar",
]


def camel_to_kebab(name: str) -> str:
	# camel to kebab-case ie allVeterans -> all-veterans
	out_chars: list[str] = []
	for ch in name:
		if ch.isupper():
			if out_chars and out_chars[-1] != "-":
				out_chars.append("-")
			out_chars.append(ch.lower())
		else:
			out_chars.append(ch)
	return "".join(out_chars)


def make_output_for_metric(df: pd.DataFrame, metric: str, out_dir: Path) -> Path:
	# csv for each metric 
	if "population" not in df.columns:
		raise KeyError("Input CSV must contain a 'population' column")

	# compute values (avoid division by zero)
	pop = df["population"].astype(float)
	metric_vals = pd.to_numeric(df[metric], errors="coerce").fillna(0.0)

	per100k = (metric_vals / pop) * 100000
	pct = (metric_vals / pop) * 100

	# keep the original/base metric values (as integers when possible)
	base_vals = pd.to_numeric(df[metric], errors="coerce").fillna(0).astype(int)

	out_df = pd.DataFrame(
		{
			"state": df["state"],
			f"{metric}Per100k": per100k,
			f"{metric}Pct": pct,
			f"{metric}": base_vals,
		}
	)

	# sort descending by per100k
	out_df = out_df.sort_values(by=f"{metric}Per100k", ascending=False)

	# make sure output dir exists
	out_dir.mkdir(parents=True, exist_ok=True)

	kebab = camel_to_kebab(metric)
	out_path = out_dir / f"{kebab}-rate.csv"

	# write CSV with no index
	out_df.to_csv(out_path, index=False)
	return out_path


def main() -> None:
	repo_root = Path(__file__).resolve().parent
	data_csv = repo_root / "data" / "veteran-data-2023.csv"
	if not data_csv.exists():
		raise FileNotFoundError(f"Input file not found: {data_csv}")

	df = pd.read_csv(data_csv)

	out_dir = repo_root / "output"

	written = []
	for metric in METRICS:
		if metric not in df.columns:
			print(f"Warning: metric '{metric}' not found in CSV — skipping")
			continue
		path = make_output_for_metric(df, metric, out_dir)
		written.append(path)
		print(f"Wrote: {path}")

	print(f"Completed. Wrote {len(written)} files to {out_dir}")


if __name__ == "__main__":
	main()

