# Bundled replay data

`IoTProcessed_Data.csv` is a regular file included in this repository for the simulated gateway and offline evaluator.
It was copied unchanged from the training project's dataset; no symlink, download or sibling checkout is required.

- Raw data rows: 37,922. Replay drops two rows with blank timestamps, leaving 37,920 samples.
- Sensor columns: `tempreature` (mapped to `temperature`), `humidity`, `water_level`, `N`, `P`, `K`.
- Actuator label columns are used by the offline evaluator, never as model inputs.

Default config: `dataset.path: datasets/IoTProcessed_Data.csv`. Paths resolve relative to the config file.
For real sensors, replace the simulated gateway's data source; backend inference does not read this CSV.
Provenance is also recorded in `models/provenance.json` and the exported model metadata.
