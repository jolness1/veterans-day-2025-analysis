from __future__ import annotations

from pathlib import Path
import pandas as pd
"""analyze veteran data and produce per-100k and percentage CSVs for various metrics.

This script now writes two sets of outputs:
- `/output/all-population` — rates calculated using the `population` column (existing behaviour)
- `/output/adult-population` — rates calculated using the `adultPopulation` column

Adult outputs use column name suffix `Adults` for the rate and pct columns and
the filenames use the `-adult-rate.csv` suffix, e.g. `all-veterans-adult-rate.csv`.
"""

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


def make_output_for_metric(
	df: pd.DataFrame,
	metric: str,
	out_dir: Path,
	pop_col: str = "population",
	per_label_suffix: str = "",
	file_suffix: str = "-rate",
) -> Path:
	"""Create CSV for one metric using the given population column.

	Parameters
	- df: input DataFrame
	- metric: base metric column name in df
	- out_dir: directory to write the CSV
	- pop_col: which population column to use (e.g. 'population' or 'adultPopulation')
	- per_label_suffix: suffix appended to the Per100k and Pct column labels (e.g. 'Adults')
	- file_suffix: suffix added to the filename (without .csv), e.g. '-rate' or '-adult-rate'
	"""
	if pop_col not in df.columns:
		raise KeyError(f"Input CSV must contain a '{pop_col}' column")

	# compute values (avoid division by zero)
	pop = pd.to_numeric(df[pop_col], errors="coerce").astype(float)
	metric_vals = pd.to_numeric(df[metric], errors="coerce").fillna(0.0)

	per100k = (metric_vals / pop) * 100000
	pct = (metric_vals / pop) * 100

	# keep the original/base metric values (as integers when possible)
	base_vals = pd.to_numeric(df[metric], errors="coerce").fillna(0).astype(int)

	per_col = f"{metric}Per100k{per_label_suffix}"
	pct_col = f"{metric}Pct{per_label_suffix}"

	out_df = pd.DataFrame(
		{
			"state": df["state"],
			per_col: per100k,
			pct_col: pct,
			f"{metric}": base_vals,
		}
	)

	# sort descending by per100k column
	out_df = out_df.sort_values(by=per_col, ascending=False)

	# make sure output dir exists
	out_dir.mkdir(parents=True, exist_ok=True)

	kebab = camel_to_kebab(metric)
	out_path = out_dir / f"{kebab}{file_suffix}.csv"

	# write CSV with no index
	out_df.to_csv(out_path, index=False)
	return out_path


def main() -> None:
	repo_root = Path(__file__).resolve().parent
	data_csv = repo_root / "data" / "veteran-data-2023.csv"
	if not data_csv.exists():
		raise FileNotFoundError(f"Input file not found: {data_csv}")

	df = pd.read_csv(data_csv)

	# 1) Write current population-based outputs into output/all-population
	out_dir_all = repo_root / "output" / "all-population"

	written_all = []
	for metric in METRICS:
		if metric not in df.columns:
			print(f"Warning: metric '{metric}' not found in CSV — skipping")
			continue
		path = make_output_for_metric(
			df, metric, out_dir_all, pop_col="population", per_label_suffix="", file_suffix="-rate"
		)
		written_all.append(path)
		print(f"Wrote: {path}")

	print(f"Completed population-based outputs. Wrote {len(written_all)} files to {out_dir_all}")

	# 2) Write adultPopulation-based outputs into output/adult-population
	out_dir_adult = repo_root / "output" / "adult-population"

	written_adult = []
	for metric in METRICS:
		if metric not in df.columns:
			# Already warned above, but keep check for robustness
			print(f"Warning: metric '{metric}' not found in CSV — skipping adult output")
			continue
		# Per/Percent column labels get the 'Adults' suffix, filenames use '-adult-rate'
		path = make_output_for_metric(
			df,
			metric,
			out_dir_adult,
			pop_col="adultPopulation",
			per_label_suffix="Adults",
			file_suffix="-adult-rate",
		)
		written_adult.append(path)
		print(f"Wrote: {path}")

	print(f"Completed adult-population outputs. Wrote {len(written_adult)} files to {out_dir_adult}")


if __name__ == "__main__":
	main()

