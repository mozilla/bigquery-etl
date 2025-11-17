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

<img width="442" height="258" alt="Screenshot 2025-11-17 at 10 46 34 AM" src="https://github.com/user-attachments/assets/13218167-fcd1-4fed-bbf2-d32537aad872" />
