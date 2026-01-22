# README (Codex notes)

Ovaj fajl je kratki podsjetnik za sljedeće Codex sesije: gdje je šta u aplikaciji, šta je “izvor istine”, kako se regenerišu metrike i koje fajlove gledati.

## Da li je pametno?
Da — ovo štedi vrijeme i smanjuje šansu da ponovo napravimo iste greške (npr. `POCETNO` datum u karticama, pogrešno računanje popusta, pogrešan izvor cijene).

## Šta je aplikacija
- Desktop app (CustomTkinter) za evidenciju/analitiku poslovanja.
- Kombinuje: SP Narudžbe (prodaja+cijene), Kartice artikala (kretanje zaliha/return), SP Prijemi (realno zaprimanje), Minimax (marža/trošak), Banka XML (refund kao “finansijska istina”).

## Izvori istine (dogovor)
- **SP Prijemi (`SP Prijemi/*.xlsx`)**: istina za *pristigla količina* i *Datum verifikacije* (od kad je artikal fizički dostupan).
- **Kartice artikala (PDF)**: istina za *stock timeline*, *OOS flag*, *količine izdavanja/prijema* i *povrate* (negativna izdavanja).
- **Kartice artikala (PDF)**: izvor za *nabavnu cijenu* po SKU iz `PS-` (prijem vrednost / prijem kolicina), za procjenu nabavne vrijednosti i izgubljenog profita tokom OOS.
- **SP Narudžbe (u `SRB1.1-razvoj.db`)**: istina za *prodajnu cijenu / neto vrijednost* po SKU (qty + popusti). SP nema kompletne povrate.
- **Banka XML**: istina za refund isplate (tab Povrati zadržan).

## Jedna baza
- Trenutno cilj: **sve u `SRB1.1-razvoj.db`**.
- `build_sku_daily_metrics.py` i UI “Regenerisi metrike” sada koriste `state["db_path"]` (tj. `SRB1.1-razvoj.db`) kao DB za SP cijene.


---

# ✅ Codex pravila ponašanja (OBAVEZNO)

Ovo su “guardrails” da Codex ne pravi destruktivne izmjene i ne troši nepotrebno tokene.

## 1) Prije bilo kakvih izmjena – obavezni koraci
Codex uvijek mora:
1. Pročitati ovaj README (minimum sekcije: **Izvori istine**, **Jedna baza**, **Važne zamke / pravila**).
2. Napisati kratak plan (10–15 linija max):
   - šta mijenja
   - koje fajlove dira
   - zašto
   - šta može puknuti
3. Ako je zadatak veliki: razbiti na 2–5 manjih koraka.

## 2) Pravilo: “NE mijenjaj logiku bez jasnog dokaza”
Zabranjeno je mijenjati bez dokaza:
- OOS / lost sales logiku
- ignorisanje `POCETNO`
- uslov “OOS i lost sales se ne računaju prije `prvi_verifikovan`”
- izvor cijena (SP Narudžbe u DB)
- način obračuna popusta

Dozvoljeno samo ako:
- se prvo pokaže gdje je trenutna logika (fajl + funkcija)
- se objasni zašto je bug
- se pokaže barem 1 sanity check / validacija

## 3) Pravilo: “Jedna istina = jedna implementacija”
Ne smije se praviti paralelna implementacija već postojećeg:
- helper funkcije
- query helpera
- pipeline wrappera

Prvo naći šta postoji, pa dopuniti.

## 4) Token kontrola / kontekst
Codex ne smije:
- učitavati cijeli repo
- slati “full dump” fajlova
- tražiti 20 fajlova odjednom

Umjesto toga:
- tražiti 1 relevantan fajl + eventualno 1 dependency fajl
- fokusirati se samo na blok koda gdje je bug

## 5) Pravilo: DB path je kritičan
Ako se radi bilo šta sa DB:
- obavezno provjeriti da je DB target **`SRB1.1-razvoj.db`**
- koristiti `state["db_path"]`
- zabranjeno hardkodirati drugi DB

## 6) Standard promjene
Svaka promjena mora imati:
- “šta je promijenjeno”
- “zašto”
- “šta može puknuti”
- “kako testirati”

## 7) Obavezna validacija nakon izmjene pipeline-a
Ako se dira:
- `extract_kalkulacije_kartice.py`
- `build_sku_daily_metrics.py`
- pipeline u `srb_modules/pipelines.py`

Obavezno uraditi sanity check:
- output CSV ide u `Kalkulacije_kartice_art/izlaz/`
- `kartice_zero_intervali.csv` intervali ne startaju iz `POCETNO`
- `lost_sales_qty` se ne pojavljuje kad SKU nije OOS
- OOS/lost sales ne postoje prije `prvi_verifikovan`


## Glavni fajlovi
### UI / App
- `SRB1.1-razvoj.py`
  - Pokreće UI, tabove, akcije i exporte.
  - Sadrži većinu logike (još uvijek “monolit”), ali smo krenuli sa modulima.

### Moduli (novi)
- `srb_modules/db.py`
  - DB helperi: `connect_db`, `init_db(schema_sql)`, `ensure_column`, `file_hash`, `app_state`, `task_progress`.
- `srb_modules/pipelines.py`
  - Pipeline: `run_regenerate_sku_metrics_process(...)` (pokreće ekstrakciju + build metrika, upisuje progress).
- `srb_modules/ui_helpers.py`
  - UI helperi za kalendar/range picker: `add_calendar_picker`, `pick_date_range_dialog`.
  - Standardizacija datuma: `parse_user_date` + `format_user_date` (dd-mm-yyyy).
- `srb_modules/queries.py`
  - Read-only SQL/query helperi za UI (bez import/match side-effecta).
  - Prebačeno: `date_expr`, `date_filter_clause`, `get_kpis`, `get_top_customers`, `get_top_products`, `get_sp_bank_monthly`.
  - UI liste (read-only): `get_refund_top_*`, `build_refund_item_totals`, `get_unpicked_*`, `get_needs_invoice_orders`, `get_unmatched_orders_list`.
- `srb_modules/ui_context.py`
  - `UIContext` shared state za modularizaciju UI-a (status/progress, executor, callbacki).
- `srb_modules/ui_poslovanje.py`
  - UI builder za tab `Poslovanje` (`build_poslovanje_tab(...)`), widgete sprema u `ctx.state["poslovanje_widgets"]`.
- `srb_modules/ui_nepreuzete.py`
  - UI builder za tab `Nepreuzete` (`build_nepreuzete_tab(...)`), widgete sprema u `ctx.state["nepreuzete_widgets"]`.
- `srb_modules/ui_povrati.py`
  - UI builder za tab `Povrati` (`build_povrati_tab(...)`), widgete sprema u `ctx.state["povrati_widgets"]`.
- `srb_modules/ui_troskovi.py`
  - UI builder za tab `Troskovi` (`build_troskovi_tab(...)`), widgete sprema u `ctx.state["troskovi_widgets"]`.
- `srb_modules/ui_prodaja.py`
  - UI builder za tab `Prodaja` (`build_prodaja_tab(...)`): kreira podtabove (OOS/Trending/Sniženja) i vraća widgete/var-ove u `ctx.state["prodaja_widgets"]`.
  - Refresh/export logika za Prodaja je u `srb_modules/ui_prodaja_logic.py` i čita widgete preko `ctx.state["prodaja_widgets"]` (bez closure zavisnosti).
- `srb_modules/ui_prodaja_logic.py`
  - `init_prodaja_logic(...)` vraća: refresh funkcije + export (OOS excel) za tab `Prodaja`.

### Import moduli (novo)
- `srb_modules/import_common.py`
  - Shared: `start_import(...)` + `append_reject(...)`.
- `srb_modules/import_sp.py`
  - SP importeri: `import_sp_orders`, `import_sp_payments`, `import_sp_returns`.
- `srb_modules/import_sp_prijemi.py`
  - SP Prijemi importer: `import_sp_prijem` + `import_sp_prijemi_folder`.
  - Dedup: file-level (`import_runs.file_hash`) + receipt-level replace (key = `Šifra klijenta` + `Datum dodavanja` fallback `Datum verifikacije`).
- `srb_modules/import_minimax.py`
  - Minimax importeri: `import_minimax`, `import_minimax_items` (storno ostaje u `SRB1.1-razvoj.py` kao callback).
- `srb_modules/import_bank_xml.py`
  - Bank XML importer: `import_bank_xml`.
- `srb_modules/import_kartice_events.py`
  - Kartice artikala importer: `import_kartice_events_csv` (CSV -> DB), UPSERT po stabilnom `event_key`.

### Parseri i metrike
- `extract_kalkulacije_kartice.py`
  - Parsira Kartice (PDF) i SP Prijemi (xlsx) i pravi CSV output.
  - Snima `Kalkulacije_kartice_art/izlaz/kartice_meta.json` sa rasponom `KARTICA ZALIHA dd.mm.yyyy - dd.mm.yyyy` i `Konačno stanje na dan ...` (koristi se kao “do kojeg datuma je export”).
  - Bitno: ignoriše `POCETNO` kao početak OOS, i koristi `prvi_verifikovan` (SP Prijemi) da ne brojimo OOS prije prve dostupnosti.
- `build_sku_daily_metrics.py`
  - Gradi `sku_daily_metrics.csv` iz `kartice_events.csv` + `sp_prijemi_summary.csv` + DB (SP Narudžbe).
  - Lost sales: baseline (EWMA + Control Group), `lost_sales_qty` se ne računa kad nije OOS.
  - `sp_unit_net_price` se forward-fill po SKU (jer cijena postoji samo na prodajnim danima).

## Output fajlovi (CSV)
Sve ide u `Kalkulacije_kartice_art/izlaz/`:
- `kartice_events.csv` (event log po SKU)
- `kartice_sku_summary.csv` (sa merge iz SP Prijemi summary)
- `kartice_zero_intervali.csv` (zero intervali; sada ne startaju iz `POCETNO`)
- `sp_prijemi_detail.csv`, `sp_prijemi_summary.csv`
- `sku_daily_metrics.csv` (glavni input za UI Prodaja)
- `sku_controls_audit.csv` (audit control-group)
- `sku_promo_periods.csv`

## UI: Prodaja
Tab `Prodaja` ima podtabove:
- **Out of stock gubitci**
  - Lijevo: Top lista (Period/Sve vrijeme + Top 5/10).
  - Desno: search SKU/naziv + detalji po SKU.
  - Export: `Export Excel (OOS)` u `exports/`.
- **Trending proizvodi**
  - `Odaberi period` (range picker), `Top 5/10` lista.
  - Desno: search + dropdown sugestije + chart (kumulativna potražnja, zeleni vs narandžasti period).
- **Analiza sniženja**
  - range picker + pre-period mjeseci.

## Dugmad za podatke
- `Osvjezi novim podacima`: samo refresh UI (briše cache i ponovo učita CSV/DB), ne pravi nove CSV.
- `Regenerisi metrike`: pokreće pipeline (extract + build), ima progress (task `regen_metrics`).
  - Incremental guard: ako nema promjena u ulaznim fajlovima (PDF/SP Prijemi/DB), pita da li želiš “force” ili preskače regeneraciju.

## SP Prijemi u DB (za budući “sve u jednoj bazi”)
- Tabele: `sp_prijemi_receipts`, `sp_prijemi_lines` (schema u `SRB1.1-razvoj.py`).
- Receipt je “atomic”: svaki `.xlsx` je 1 prijem; ako dođe korekcija istog prijema, importer zamijeni sve linije za taj receipt_key (nema dupliranja u metrikama).
- CLI:
  - Import foldera: `python SRB1.1-razvoj.py import-sp-prijemi "Sp Prijemi"`
  - Import pojedinačnog fajla: `python SRB1.1-razvoj.py import-sp-prijemi "Sp Prijemi\\file.xlsx"`

## Važne “zamke” / pravila
- `POCETNO` red iz PDF kartica (često 01.04.2024) ne smije se tretirati kao realni OOS start.
- OOS i lost sales se ne računaju prije `prvi_verifikovan` iz SP Prijemi.
- Cijena se računa iz SP Narudžbi u DB:
  - prvo order-level `Popust` (extra_discount), pa item-level `Popust proizvoda` (discount)
  - sve se množi sa `qty`.

## Uvoz: gap warning (nedostaju brojevi)
- SP Narudžbe i Minimax import sada prijavljuju `gap_warning` u reject logu ako u fajlu postoji “rupa” u rednim brojevima u odnosu na trenutni maksimum u DB.
- Ovo je upozorenje (ne blokira import), ali signalizira da fali neki export/fajl i da podaci možda nisu kompletni.

## Brzi workflow (operativno)
1) Ubaci najnoviji PDF kartice u `Kartice artikala/`.
2) Ubaci nove prijeme (`.xlsx`) u `SP Prijemi/`.
3) U app-u klik `Regenerisi metrike`.
4) Pregledaj `Prodaja` tabove i exporte.

## Kartice artikala u DB (CSV -> DB)
- Tabela: `kartice_events` (schema u `SRB1.1-razvoj.py`).
- Dedup:
  - file-level: `import_runs.file_hash` za `Kartice-Events-CSV`
  - row-level: `event_key = sha1(SKU|Datum|Broj|Tip|Smer|Referenca)` + UPSERT
- CLI:
  - `python SRB1.1-razvoj.py import-kartice-events "Kalkulacije_kartice_art\\izlaz\\kartice_events.csv"`

## Sledeći refactor koraci (plan)
- Nastaviti izvlačenje: DB query funkcije u `srb_modules/queries.py`.
- Konkretno: read-only liste za UI (refund top, unpicked liste, needs_invoice/unmatched liste).
- UI podijeliti po tabovima u `srb_modules/ui/` (npr. `ui_prodaja.py`).
- Pipeline incremental (obradi samo novo) + upis metrika u DB.
