# Stable Tables Targeted

This generator builds the BigEye `biconfig.yml` and `metadata.yaml` files for targeted stable tables. The reason for this appproach is to mitigate any potential negative impacts should there be increased future costs associated with freshness and volume checks.

## Conifguration

Modify the `bqetl_project.yaml` file. Example:

```yaml
monitoring:
  stable_tables_targeted:
    mozilla_vpn_external:
    - waitlist_v1
```

## Running the generator

To test this generator run `./bqetl generate stable_tables_targeted`.

Output:



