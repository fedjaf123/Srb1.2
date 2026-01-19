import json
from datetime import datetime
from pathlib import Path

import pandas as pd


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def main() -> int:
    base = Path("Kalkulacije_kartice_art/izlaz")
    marza_csv = base / "kalkulacije_marza_agregat.csv"
    prod_csv = base / "prodaja_avg_i_gubitak.csv"
    zero_csv = base / "kartice_zero_intervali.csv"

    today = datetime.utcnow().isoformat()
    df_marza = _read_csv(marza_csv)
    df_prod = _read_csv(prod_csv)
    df_zero = _read_csv(zero_csv)

    payload = {
        "generated_at": today,
        "totals": {
            "artikli_marza": len(df_marza),
            "artikli_prodaja": len(df_prod),
            "intervali_zero": len(df_zero),
        },
        "top_marza": [],
        "top_nedostupnosti": [],
        "zero_intervals": [],
        "prodaja": [],
    }

    if not df_marza.empty:
        top_marza = (
            df_marza.sort_values("marza_sum", ascending=False)
            .head(20)
            .to_dict("records")
        )
        payload["top_marza"] = [
            {
                "SKU": row.get("SKU"),
                "Artikal": row.get("Artikal"),
                "marza_sum": row.get("marza_sum"),
                "kolicina_sum": row.get("kolicina_sum"),
            }
            for row in top_marza
        ]

    if not df_prod.empty:
        top_unavailable = (
            df_prod.sort_values("Dani bez zalihe", ascending=False)
            .head(20)
            .to_dict("records")
        )
        payload["top_nedostupnosti"] = [
            {
                "SKU": row.get("SKU"),
                "Dani bez zalihe": row.get("Dani bez zalihe"),
                "Procjena gubitka neto": row.get("Procjena gubitka neto"),
                "Prosek dnevno": row.get("Prosek dnevno"),
            }
            for row in top_unavailable
        ]
        payload["prodaja"] = [
            {
                "SKU": row.get("SKU"),
                "Total prodato": row.get("Total prodato"),
                "Prosek dnevno": row.get("Prosek dnevno"),
                "Ukupno neto": row.get("Ukupno neto"),
                "Prosek neto": row.get("Prosek neto"),
                "Dani bez zalihe": row.get("Dani bez zalihe"),
            }
            for _, row in df_prod.iterrows()
        ]

    if not df_zero.empty:
        payload["zero_intervals"] = [
            {
                "SKU": row.get("SKU"),
                "Artikal": row.get("Artikal"),
                "Zero od": row.get("Zero od"),
                "Zero do": row.get("Zero do"),
                "Dani bez zalihe": row.get("Dani bez zalihe"),
            }
            for _, row in df_zero.iterrows()
        ]

    out_path = base / "analytics_feed.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Generated {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
