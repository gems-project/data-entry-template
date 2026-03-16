# GEMS Data Pipeline Flow (01–08)

```mermaid
graph TB
    N01[01 DiscoverAndGate]
    O1[(gemsGateResults)]
    N02[02 BronzeIngest]
    O2[(bronze opsSheetUnits opsStudyVersions)]
    N03[03 ValidationStoplight]
    O3[(gemsValidationStoplight opsEmailQueueStoplight opsChecklistActions)]
    N04[04 SilverTransform]
    O4[(silver opsSilverTransformLog)]
    N05[05 SilverQC]
    O5[(silverQcSummary silverQcDetails)]
    N06[06 EmailNotifications]
    O6[(opsEmailQueueQc opsGoldCandidates)]
    N07[07 SendEmails]
    O7[(opsEmailQueueQc sent)]
    N08[08 GoldStudies]
    O8[(gold opsGoldStudies opsGoldBuildLog)]

    N01 --> O1 --> N02 --> O2 --> N03 --> O3 --> N04 --> O4 --> N05 --> O5 --> N06 --> O6 --> N07 --> O7 --> N08 --> O8
```

## Summary

| Step | Notebook | Purpose | Key Outputs |
|------|----------|---------|-------------|
| **01** | DiscoverAndGate | Gate workbooks by DUA + Checklist + hash | `gemsGateResults` |
| **02** | BronzeIngest | Ingest to raw Bronze tables | `bronze*`, `opsSheetUnits`, `opsStudyVersions` |
| **03** | ValidationStoplight | Validate core + promised data | `gemsValidationStoplight`, `opsEmailQueueStoplight`, `opsChecklistActions` |
| **04** | SilverTransform | Clean Bronze → Silver | `silver*`, `opsSilverTransformLog` |
| **05** | SilverQC | Rulebook validation | `silverQcSummary`, `silverQcDetails` |
| **06** | EmailNotifications | Build emails, identify Gold candidates | `opsEmailQueueQc`, `opsGoldCandidates` |
| **07** | SendEmails | Send via Gmail SMTP | Updates `opsEmailQueueQc` |
| **08** | GoldStudies | Promote to Gold | `gold*`, `opsGoldStudies`, `opsGoldBuildLog` |
