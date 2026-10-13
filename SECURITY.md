# Credentials and local data

Keep cookies and API keys in ignored `.env` files or environment variables. Never commit raw scrape output, logs, browser profiles, local agent settings, or internal research documents. The release contains code and generic examples only.

`check_public.py` scans the Git tracked files when run inside this repository, including files staged for the first commit; before Git initialization it uses an explicit release allowlist. It reports paths and categories, never matching secret values. This is a best-effort guard, not proof that all possible sensitive content is absent. CI also rejects data files and unexpected release files.

If a credential was disclosed, invalidate/rotate it with the provider. Removing a secret from a new release does not invalidate an older copy. Avoid including sensitive values in public issues.
