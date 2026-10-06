# BigQuery to Power BI data flow and refresh

Power BI is connected to the existing BigQuery analytical dataset and the
dashboard is built, as confirmed by the operator. Access uses the configured
Google account email through Terraform-managed IAM. The report can receive
newly indexed analyses through Import model refresh.

The three Power BI IAM resources are applied for the privately configured reader.
The custom project role grants query creation, project metadata, quota use and
the three BigQuery Storage API read-session permissions. Dashboard authoring
is maintained in Power BI; Terraform manages its Google Cloud access.

## Data updates and dashboard

1. Worker B persists a completed analysis report and completion metadata in GCS.
2. The indexer projects the completed analysis into BigQuery's
   `poc_gemma_analytics.analysis_runs` and `analysis_reports` tables.
3. Power BI refresh reads those tables using the authorized Google identity and
   recalculates the imported model while preserving report definitions.
4. The dashboard displays the updated volume, cause distribution and ranking,
   missed causes (false negatives), and trends over time and by batch.

Updates follow completed pipeline batches and Power BI refresh. The Import
model is a snapshot between refreshes; no continuous push or automatic refresh
schedule is configured by this repository. Refresh does not start pipeline work.

The [Power BI folder](../powerbi/README.md) publishes the native JSON report
layout/resources and a [dashboard screenshot](../powerbi/power-bi.png). The local
PBIX is ignored because it contains an imported binary semantic model and
security bindings. The screenshot records the assembled report, including a
900-record batch, rather than a live view of the current BigQuery contents.
See [data boundaries](data-boundaries.md) for privacy scope and sensitive fields.

## Access configuration

Set `powerbi_reader` in ignored private Application deployment variables to the IAM
principal of the Google user signing into the connector, or a dedicated Google
group containing that user. The required prefix is `user` or `group`, followed
by a colon and the account email. Keep real addresses outside version control.
The default `null` creates no Power BI resources.

Terraform prepares three resources in the existing Application state:

| Resource | Scope and purpose |
| --- | --- |
| Dataset IAM member | `roles/bigquery.dataViewer` on `poc_gemma_analytics`: read data and metadata, including its current `analysis_runs` and `analysis_reports` tables and future tables in this dataset. |
| Dedicated custom role | Query job creation, quota use and project discovery. No dataset/table creation or mutation permissions. |
| Project IAM member | Binds the custom role on the fixed implementation project only. Data access remains scoped to the analytics dataset. |

The role deliberately avoids broader BigQuery User/Job User grants. Query jobs produce temporary
results and can incur charges; job creation alone does not grant writes to
source tables. IAM grants are additive: an existing Owner/Editor or inherited
write grant still permits writes. Use a dedicated reader identity when strict
read-only isolation is required.

Prepare a saved plan using the current private deployment configuration:

```sh
terraform -chdir=terraform/components/application plan -input=false -out=powerbi.tfplan
terraform -chdir=terraform/components/application show powerbi.tfplan
```

Retain the current runtime flags, image digests and other deployment values.
Do not substitute the incomplete example profile or use a targeted apply.
Expect only three additions when first enabling access, with no changes to
runtime, tables, buckets or existing IAM. Review any different result before
proceeding. Plans and state contain private identities and must stay ignored.
Confirm active cloud identity/project, current reader grants and BigQuery API availability before planning against the live environment.
This configuration does not enable APIs. Any missing API needs its own reviewed
Terraform change and cloud approval.

Apply the exact saved plan only after concrete cloud-plan approval. Verify the
two IAM grants through scoped cloud reads and then import both tables using the
reader's Power BI credentials. For a new reader, validate a successful import
and a subsequent refresh against the live environment.

To revoke this module's access, set `powerbi_reader` back to `null`, review a
saved plan removing only these three resources, approve and apply it. This
does not remove permissions granted elsewhere or data already imported into
Power BI. Changing the principal must also be reviewed as an IAM change.

## Power BI configuration

Use Get Data > Google BigQuery, authenticate with the configured Google account
and choose **Import**. Set Billing Project ID and Project ID to the fixed
project returned by the `powerbi_source` Terraform output. Select
`poc_gemma_analytics.analysis_runs` and `analysis_reports`. Keep **Use Storage API**
enabled in the connector advanced options. Its read-session permissions are
included in the dedicated project role.
Do not configure a persistent large-results destination dataset: source write
permissions are intentionally absent.

Import stores a local model snapshot. Build measures, relationships and support
tables in that model. Refresh reads the latest indexed table contents and
recalculates the model while retaining its report definitions. It requires valid
credentials and network access during refresh; reports otherwise use the imported
snapshot. Changes to the source schema may require Power Query/model edits.
Power BI Service refresh requires its own stored data-source credentials and
optional schedule. Terraform here does not configure that service.

Some projected fields are JSON strings. Parse and expand them in Power Query as
needed. BigQuery reflects completed indexed analyses; refreshing does not launch
the pipeline or create newer analyses. The source contains aggregate history
and report projections, not raw conversation rows.

Queries and possible data transfer can incur usage charges;
IAM is not a query budget or hard spending limit. Limit imported columns/history
and refresh frequency according to the dashboard's needs. This change adds no
compute service or storage resource.

See the [Microsoft connector documentation](https://learn.microsoft.com/en-us/power-query/connectors/google-bigquery)
and [BigQuery IAM reference](https://docs.cloud.google.com/bigquery/docs/access-control)
for connector settings and permission semantics.
