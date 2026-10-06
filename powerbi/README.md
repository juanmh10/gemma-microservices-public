# Power BI

The completed report connects to BigQuery in Import mode using the configured
Google account and its Terraform-managed IAM permissions. Refresh the model
after new analyses are indexed to update the dashboard.

| Published asset | Purpose |
| --- | --- |
| [report/definition](report/definition) | Native JSON layout and field bindings exported from the local report. |
| [report/StaticResources](report/StaticResources) | JSON theme resource referenced by the report definition. |
| [power-bi.png](power-bi.png) | Reviewed aggregate dashboard preview with volume, causes, omissions and batch trends. |

The local `Painel_Consolidado_Final.pbix` remains available to the operator but is
ignored and blocked from publication: it contains an imported binary model and
security bindings. The exported report JSON is a design snapshot, not a complete
Power BI project or semantic model. It contains no imported cache or connection
credentials. Configure the model and authenticate privately in Power BI.

See [data flow and access](../docs/powerbi.md) and
[repository hygiene/data boundaries](../docs/data-boundaries.md). Keep credentials,
private configuration and imported report binaries outside version control.
