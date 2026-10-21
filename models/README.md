# MLP deployment bundle

`mlp_v1/` contains the deployment artifacts exported from `AIoT-AI-models/artifacts/mlp_v1/`:

- `model_metadata.json`: schema, version and tensor metadata.
- `preprocessing.json`: feature order and train-only mean/scale/range.
- `thresholds.json`: validation-selected ON thresholds.
- `model_fp32.tflite` and `model_int8.tflite`: exports of the same checkpoint.

The default adapter uses `iot_agri/backend/models/mlp_runtime.py`, an inference-only snapshot of the source runtime.
Its preprocessing, quantization and thresholds retain the source implementation's numerical behavior.
The `aiot_ai` package and source checkout are not required. Provenance is recorded in `provenance.json`.
Startup validates the required inference files, preprocessing, thresholds, feature/actuator order and tensor format.
No Keras model, dataset or training output is needed here.

To deploy another exported run, copy these five files into a **new** directory and change `inference.params.model_dir`.
Keep metadata and files from the same run. Test both variants and actuator/feature mapping before use.
Model exports are transferred manually as deployment artifacts; changes in the training repository never change the
running platform automatically. A compatible new bundle does not require installing or updating a training package.
If the export format changes, update the local runtime and regression fixtures together, and record the new provenance.
See `docs/INDEPENDENT_DEPLOYMENT.md` for the independent workflow.
