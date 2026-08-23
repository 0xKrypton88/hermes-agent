# ENG-118 — fryst migrationsexekveringskontrakt

**Fryst:** 2026-08-23T09:24:11+02:00

**Beslut:** `STOP_BEFORE_DATA_MUTATION`

**Maintenance window:** begärt omedelbart, men kan inte öppnas säkert under förbudet mot Hermes Gateway-restart.

**Kodbas:** `7653663c828fa277d4d2d88760e5bd5e21ef8b9a`

**Branch:** `codex/eng-118-migration-stop-contract`

**Worktree:** `D:\Hermes\worktrees\hermes-eng118-migration-stop-contract`

Detta dokument fryser exekveringskontraktet före all datastore-, config- eller writer-mutation. Endast read-only-inventering och kontrollplansdokumentation har utförts. Ingen backup, restore, migration, adoption, cutover, rollback, deploy, push, extern effekt eller Gateway-restart har utförts.

## 1. Source store, schema/version och canonical identity

- **Kandidat till source för legacy durable state:** `C:\Users\Glantz\AppData\Local\hermes\state.db`.
- **Aktiv miljöindikator:** Gateway PID `7716` kör `D:\Hermes\releases\hermes-eng83-7653663c\...\python.exe -m hermes_cli.main gateway run`; parent wrapper PID `3132`.
- **SQLite:** `PRAGMA user_version=0`, `application_id=0`, `PRAGMA integrity_check=ok`; tabellen `schema_version` innehåller `26`.
- **Read-only population 2026-08-23 ca 09:25 +02:** `async_delegations=51` (`completed=42`, `unknown=6`, `error=2`, `running=1`), `delivery_obligations=228` (`delivered=228`), `session_turn_leases=0`, `compression_locks=2`, `sessions=1275`, `messages=325390`.
- **Observerad filstorlek:** `6,448,783,360` byte.
- **Observerad SHA-256:** `1310181492b7b644e374ad92b2e15a26fccd93f4215b52f770b3d3e758c3f18a`, men **inte godkänd som freeze-checksum** eftersom aktiv Gateway/writer inte var quiesced under hashningen.
- **Canonical identity:** **OKÄND/SAKNAS**. `state_meta` har ingen verifierbar storage-id/canonical identity. Därför får kandidatfilen inte adopteras som canonical source.
- **Andra stores är explicit ej klassificerade som source:** bland annat `C:\Users\Glantz\AppData\Local\hermes\cursor_cloud_runs\runs.sqlite3` (`cursor_runs=154`, SHA-256 `5231cea2cda5636f208304e67a713cbbc022901cd231f8e357341f03bd4a96e4`) och `cron\executions.db` (`executions=1000`). De har annan känd funktion och får inte migreras implicit.

## 2. Target store/miljö och schema/version

- Aktiv `C:\Users\Glantz\AppData\Local\hermes\config.yaml` har **ingen** `durable_jobs`-sektion (`enabled`, backend, sqlite/postgres path, schema och storage-id saknas).
- Candidate-default är fail-closed: `durable_jobs.enabled=false`, `dispatch_enabled=false`, backend/path/schema/storage-id `None`.
- **Target store:** INTE PROVISIONERAD.
- **Target environment identity:** OKÄND.
- **Target application schema:** designunderlag anger durable application schema `9`; inget target-schema har skapats eller verifierats.
- **Target checksum/readback/counts:** EJ TILLÄMPLIGT före provisioning; inga target-objekt har muterats.

## 3. Writer-owner och quiesce/drain utan externa effekter

- Aktiv Gateway writer: PID `7716`, start token `178733569822`; Slack och webhook är anslutna enligt `gateway_state.json` och anger `writer_pid=7716`.
- Aktiv schemalagd writer: cronjob `bfa5968c0471` är `enabled=true`, `state=scheduled`, kör var femte minut och uppdaterade `jobs.json` 2026-08-23T09:23:13+02:00.
- Aktiv legacy durable post: `async_delegations` innehåller `running=1`; `unknown=6`; två compression locks finns.
- `gateway_state.json` rapporterar `active_agents=0`, men detta bevisar inte writer-quiesce.
- Durable lane är inte attachad/provisionerad genom aktiv config. Koden attachar lane mot Gateway runner under runtime-livscykeln; någon verifierad hot-cutovermekanism till en ny canonical writer utan Gateway-livscykelbyte har inte påvisats.
- **Quiesce/drain:** INTE BEVISAD och kan inte säkert genomföras under absoluta förbudet mot Gateway-restart. Gateway eller externa adapters har inte stoppats.
- **External-effects replay safety:** INTE BEVISAD på grund av `running=1`, `unknown=6` och anslutna Slack/webhook-adapters. Inga adapters anropades av denna körning.

## 4. Backup, retention, hash och restore-rehearsal

- **Planerad, ej skapad path:** `C:\Users\Glantz\AppData\Local\hermes\backups\eng118-20260823T092411+0200\state.db`.
- **Planerad behörighet:** endast aktuell användare och SYSTEM; arv avstängt.
- **Planerad retention:** minst genom ENG-121/stabilisering och minst 30 dagar, därefter separat explicit arkivbeslut.
- **Backup hash:** SAKNAS — backup togs inte eftersom source inte kunde quiescas och canonical identity är okänd.
- **Restore-rehearsal:** INTE UTFÖRD. Att kopiera en aktiv 6.45 GB SQLite-fil utan verifierad online-backup/freeze skulle inte bevisa RPO=0.
- **Original:** orört av ENG-118-körningen.

## 5. RPO, RTO och rollbacksteg

- **Krav:** RPO=0 för durable state.
- **Verifierat RPO:** INTE UPPNÅTT; aktiv writer och okänd source identity blockerar freeze point.
- **Konservativt RTO:** OKÄNT tills backup+isolated restore+integrity/readback har tidtagits. Inget RTO-värde får uppfinnas.
- **Avsett rollbacksteg efter framtida godkänd cutover:** (a) frys ny writer, (b) verifiera inga in-flight leases/effects, (c) återställ source från verifierad RPO=0-snapshot eller återaktivera orörd source, (d) återbind exakt en verifierad writer-authority, (e) bevisa att target writer fail-closed, (f) readback counts/checksums/integritet.
- **Rollback idag:** trivial kontrollplansrollback eftersom ingen data/config/writer ändrades; ta bort denna branch/worktree/Linear-kommentar. Ingen dataåterställning behövs.

## 6. Pre/post counts, checksums, referential integrity och reconciliation

- Pre-counts anges i punkt 1. Full deduplicerad migrationspopulation är INTE definierad: 51 legacy delegation records kan inte likställas med target job/checkpoint/retry/interrupt/effect-schema utan mapping.
- Pre-integrity: SQLite `PRAGMA integrity_check=ok`, men ingen applikationsspecifik referential-integrity- eller identity-kontroll är godkänd.
- Pre-checksum är instabil/ej fryst medan writer är aktiv.
- Post-counts/checksums/referential integrity: SAKNAS eftersom migration inte startades.
- Leases: `session_turn_leases=0`; `compression_locks=2` kvarstår.
- Retries/interrupts/checkpoints: EJ KLASSIFICERADE/RECONCILERADE.
- Unresolved effects: `async_delegations running=1`, `unknown=6`; externa adapters är anslutna. Detta är en explicit stop-condition även om alla 228 delivery obligations är `delivered`.
- Isolerat checkpoint-resume med adapters `fail-if-called` och idempotent retry: EJ KÖRT, eftersom population/target/restore-grind inte är grön.

## 7. Rollback authority och stop conditions

- **Rollback authority:** användarens uttryckliga maintenance-window/cutover-authority, men inga destruktiva steg eller Gateway-restart är delegerade. Exekverande session är ensam writer för ENG-118-kontrollplansartefakter.
- **STOP nu, före data/config/writer-mutation, på samtliga följande oberoende grunder:**
  1. canonical source identity är okänd,
  2. target store/miljö/storage-id är inte provisionerad eller verifierad,
  3. aktiv Gateway writer kan inte bevisat quiescas/dräneras utan förbjuden Gateway-restart,
  4. en legacy delegation är `running`, sex är `unknown`, två compression locks finns,
  5. externa Slack/webhook-adapters är anslutna och replay-frihet är inte bevisad,
  6. RPO=0-backup och isolerad restore/readback saknas,
  7. stabil source-checksum, target checksum, full mapping och referential reconciliation saknas,
  8. exakt-en-writer cutover och fail-closed gammal writer-path kan inte verifieras utan runtime authority-bindning.

## Exakt next_action

Planera ett nytt maintenance window där Gateway-livscykelåtgärd är explicit tillåten **eller** leverera och verifiera en redan installerad, autentiserad hot-quiesce/hot-bind-mekanism som inte kräver restart. Före ny mutation ska operatören: fastställa source canonical identity/storage-id; klassificera och avsluta/reconcila `running=1`, `unknown=6` och locks utan externa replay; provisionera target identity/schema med dispatch av och adapters `fail-if-called`; quiesca alla writers inklusive cron; skapa en konsistent RPO=0-snapshot via verifierad SQLite backup/freeze; restore-repetera isolerat; och först därefter köra migration, idempotent resume/retry, single-writer cutover och rollback rehearsal.

## Primärkällor och hashes

- Active config: `C:\Users\Glantz\AppData\Local\hermes\config.yaml`, SHA-256 `f3fb0b78ff2b959dc086d7ac853841e496476b88a6120b205c71af9cf12d309e`.
- Gateway state: `C:\Users\Glantz\AppData\Local\hermes\gateway_state.json`, SHA-256 `ccbf8c811a174ff5fee3c5478ecfd1d9b564fd128343dcd5b58d81b3833d5be8`.
- Cron config: `C:\Users\Glantz\AppData\Local\hermes\cron\jobs.json`, SHA-256 `3360d7cbffde1fbff969e79f27910fdc2963853f48b315e072f3d89b9391bdd3`.
- Bootstrap manifest: `D:\Hermes\artifacts\eng115-waypoint\bootstrap-manifest.json`, SHA-256 `2ccdbc6e6b5fa6b5c66c5797ec6086d6af53b0950c9fde4348ceacf3aefb075f`.
- ENG-117 contract matrix: `D:\Hermes\artifacts\eng117-provenance\contract-matrix.json`, SHA-256 `1e27351dc74d4d23f461a772ae00129e111c907bb579cba207b972e3604bd91d`.

## DoD-resultat

- Definierad/deduplicerad population: **FAIL/UNKNOWN**.
- Schema/version/checksum/readback: **PARTIAL** (source schema/integrity läst; canonical freeze och target saknas).
- Minst ett checkpointat jobb återupptaget isolerat: **NOT RUN — gated**.
- Rollback återställer state och writer authority utan dataförlust: **NOT RUN — gated**.
- Canonical writer cutover: **NOT RUN — förbjuden restart krävs eller verifierad hot-bind saknas**.

**Sanningsenlig status: BLOCKED.**
