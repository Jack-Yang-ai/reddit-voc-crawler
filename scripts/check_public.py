"""Best-effort release scan. Prints filenames/categories, never secret values."""
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ALLOWED = {
    'README.md', 'SECURITY.md', '.env.example', '.gitignore', 'requirements.txt',
    '.github/workflows/ci.yml', 'examples/taxonomy.json', 'tests/test_pipeline.py',
    *('scripts/' + name for name in (
        'reddit.py', 'openclaw_reddit.py', 'research_batch.py', 'annotate.py',
        'mine_assign.py', 'brand_scope.py', 'annotate_brand_scope.py',
        'settings.py', 'build_report.py', 'check_public.py')),
}
PATTERNS = {
    'credential': re.compile(r'(?:sk-[A-Za-z0-9_-]{16,}|gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})'),
    'session': re.compile(r'(?:reddit_session|token_v2|session_tracker)\s*='),
    'private path': re.compile(r'/(?:Users|home)/[^\s/]+/'),
    'internal document': re.compile(r'https?://[^\s/]*(?:feishu|larksuite|doubao)\.[^\s]+|(?:open_id|ou_)[A-Za-z0-9]{12,}'),
    'email': re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'),
    'private key': re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'),
}


def main():
    if (ROOT / '.git').exists():
        result = subprocess.run(['git', 'ls-files', '-z'], cwd=ROOT, check=True, capture_output=True)
        paths = [Path(p) for p in result.stdout.decode().split('\0') if p]
    else:
        paths = [Path(p) for p in sorted(ALLOWED)]
    issues = [(path, "missing release file") for path in sorted(ALLOWED - {p.as_posix() for p in paths})]
    for path in paths:
        if path.as_posix() not in ALLOWED:
            issues.append((str(path), 'unexpected release file'))
            continue
        text = (ROOT / path).read_text(encoding='utf-8')
        for category, pattern in PATTERNS.items():
            if pattern.search(text):
                issues.append((str(path), category))
    for path, category in issues:
        print(f'FAIL {path}: {category}')
    if issues:
        raise SystemExit(1)
    print(f'PASS: {len(paths)} release files checked; no detected sensitive content')


if __name__ == '__main__':
    main()
