"""Frozen v1 coding-task catalog for real-model evaluation.

The evaluator source is materialized outside the agent worktree. These tasks are
small repository fixtures, not SWE-bench, and their results must be labelled as
ForgeReplay Coding Tasks v1.
"""

from __future__ import annotations

import hashlib
import json
import textwrap
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class CodingTask:
    task_id: str
    split: str
    category: str
    difficulty: str
    prompt: str
    seed_files: dict[str, str]
    evaluator_source: str
    expected_changed_files: tuple[str, ...]


def _task(
    task_id: str,
    split: str,
    category: str,
    prompt: str,
    source: str,
    tests: str,
    *,
    extra_files: dict[str, str] | None = None,
    expected: tuple[str, ...] = ("solution.py",),
    difficulty: str = "small",
) -> CodingTask:
    files = {"solution.py": textwrap.dedent(source).lstrip()}
    files.update(extra_files or {})
    evaluator = (
        "import sys\n"
        "from pathlib import Path\n"
        "sys.path.insert(0, str(Path(sys.argv[1]).resolve()))\n"
        + textwrap.dedent(tests).lstrip()
    )
    return CodingTask(
        task_id=task_id,
        split=split,
        category=category,
        difficulty=difficulty,
        prompt=prompt,
        seed_files=files,
        evaluator_source=evaluator,
        expected_changed_files=expected,
    )


TASKS: tuple[CodingTask, ...] = (
    _task(
        "single-001", "dev", "single_file_bug", "Fix clamp so both boundaries are inclusive and reversed bounds are rejected.",
        """
        def clamp(value, low, high):
            if low > high:
                return value
            return min(high - 1, max(low + 1, value))
        """,
        """
        from solution import clamp
        assert clamp(0, 0, 10) == 0
        assert clamp(10, 0, 10) == 10
        assert clamp(4, 0, 10) == 4
        try: clamp(3, 9, 1)
        except ValueError: pass
        else: raise AssertionError('reversed bounds must fail')
        """,
    ),
    _task(
        "single-002", "dev", "single_file_bug", "Fix chunks: reject non-positive sizes and do not emit an empty final chunk.",
        """
        def chunks(items, size):
            return [items[i:i + size] for i in range(0, len(items) + 1, size)]
        """,
        """
        from solution import chunks
        assert chunks([], 2) == []
        assert chunks([1,2,3,4], 2) == [[1,2],[3,4]]
        assert chunks([1,2,3], 2) == [[1,2],[3]]
        for size in (0, -1):
            try: chunks([1], size)
            except ValueError: pass
            else: raise AssertionError('invalid size')
        """,
    ),
    _task(
        "single-003", "dev", "single_file_bug", "Make is_palindrome Unicode-aware and ignore punctuation and letter case.",
        """
        def is_palindrome(text):
            cleaned = text.replace(' ', '').lower()
            return cleaned == cleaned[::-1]
        """,
        """
        from solution import is_palindrome
        assert is_palindrome('A man, a plan, a canal: Panama!')
        assert is_palindrome('上海自来水来自海上')
        assert not is_palindrome('agent runtime')
        """,
    ),
    _task(
        "single-004", "held_out", "single_file_bug", "Fix moving_average input validation and return one value for a full-width window.",
        """
        def moving_average(values, width):
            return [sum(values[i:i+width]) / width for i in range(len(values) - width)]
        """,
        """
        from solution import moving_average
        assert moving_average([1,2,3], 3) == [2]
        assert moving_average([1,2,3,4], 2) == [1.5,2.5,3.5]
        for width in (0, 4):
            try: moving_average([1,2,3], width)
            except ValueError: pass
            else: raise AssertionError('invalid width')
        """,
    ),
    _task(
        "data-001", "dev", "data_parsing", "Implement parse_csv_row using Python CSV semantics, including quotes and escaped quotes.",
        "def parse_csv_row(line):\n    return line.split(',')\n",
        """
        from solution import parse_csv_row
        assert parse_csv_row('a,"b,c","d""e"') == ['a','b,c','d"e']
        assert parse_csv_row('') == []
        """,
    ),
    _task(
        "data-002", "dev", "data_parsing", "Fix deep_merge so nested mappings merge recursively without mutating either input.",
        """
        def deep_merge(left, right):
            left.update(right)
            return left
        """,
        """
        from solution import deep_merge
        a={'db':{'host':'a','port':1},'x':1}; b={'db':{'port':2},'y':3}
        out=deep_merge(a,b)
        assert out == {'db':{'host':'a','port':2},'x':1,'y':3}
        assert a == {'db':{'host':'a','port':1},'x':1} and b == {'db':{'port':2},'y':3}
        """,
    ),
    _task(
        "data-003", "dev", "data_parsing", "Implement get_path for dotted dict/list paths and support a default for missing segments.",
        """
        def get_path(value, path, default=None):
            return value.get(path, default)
        """,
        """
        from solution import get_path
        data={'users':[{'name':'Ada'},{'name':'Lin'}]}
        assert get_path(data,'users.1.name') == 'Lin'
        assert get_path(data,'users.3.name','missing') == 'missing'
        assert get_path(data,'','fallback') is data
        """,
    ),
    _task(
        "data-004", "held_out", "data_parsing", "Fix parse_log_line to parse ISO timestamps, levels, and messages containing colons.",
        """
        def parse_log_line(line):
            timestamp, level, message = line.split(':')
            return {'timestamp': timestamp, 'level': level, 'message': message}
        """,
        """
        from solution import parse_log_line
        out=parse_log_line('2026-08-18T10:20:30Z INFO worker: completed: ok')
        assert out == {'timestamp':'2026-08-18T10:20:30Z','level':'INFO','message':'worker: completed: ok'}
        try: parse_log_line('bad line')
        except ValueError: pass
        else: raise AssertionError('malformed input')
        """,
    ),
    _task(
        "api-001", "dev", "cli_api_contract", "Implement strict parse_bool accepting common booleans and rejecting ambiguous values.",
        "def parse_bool(value):\n    return bool(value)\n",
        """
        from solution import parse_bool
        for value in ('true','TRUE','1','yes','on',True): assert parse_bool(value) is True
        for value in ('false','FALSE','0','no','off',False): assert parse_bool(value) is False
        try: parse_bool('sometimes')
        except ValueError: pass
        else: raise AssertionError('ambiguous')
        """,
    ),
    _task(
        "api-002", "dev", "cli_api_contract", "Normalize HTTP status inputs to integers in 100..599 and reject booleans.",
        "def normalize_status(value):\n    return int(value)\n",
        """
        from solution import normalize_status
        assert normalize_status('404') == 404 and normalize_status(200) == 200
        for value in (True, 99, 600, 'x'):
            try: normalize_status(value)
            except (TypeError, ValueError): pass
            else: raise AssertionError(value)
        """,
    ),
    _task(
        "api-003", "dev", "cli_api_contract", "Fix build_url to preserve base path, encode query values, and omit None values.",
        """
        def build_url(base, path, query):
            return base + '/' + path + '?' + '&'.join(f'{k}={v}' for k,v in query.items())
        """,
        """
        from solution import build_url
        assert build_url('https://x.test/api/','/items',{'q':'a b','page':2,'skip':None}) == 'https://x.test/api/items?q=a+b&page=2'
        assert build_url('https://x.test','health',{}) == 'https://x.test/health'
        """,
    ),
    _task(
        "api-004", "held_out", "cli_api_contract", "Implement validate_port: accept integer-like non-booleans in 1..65535 and return int.",
        "def validate_port(value):\n    return value\n",
        """
        from solution import validate_port
        assert validate_port('443') == 443 and validate_port(1) == 1
        for value in (True, 0, 65536, '1.5'):
            try: validate_port(value)
            except (TypeError, ValueError): pass
            else: raise AssertionError(value)
        """,
    ),
    _task(
        "multi-001", "dev", "multi_file_change", "Rename User.full_name to display_name and update the formatter without keeping the old API.",
        """
        class User:
            def __init__(self, first, last): self.first, self.last = first, last
            def full_name(self): return f'{self.first} {self.last}'
        """,
        """
        from solution import User
        from formatter import format_user
        user=User('Ada','Lovelace')
        assert user.display_name() == 'Ada Lovelace'
        assert format_user(user) == 'ADA LOVELACE'
        assert not hasattr(user, 'full_name')
        """,
        extra_files={"formatter.py": "def format_user(user):\n    return user.full_name().upper()\n"},
        expected=("solution.py", "formatter.py"),
    ),
    _task(
        "multi-002", "dev", "multi_file_change", "Move the shared timeout default to settings.py and use it from both clients.",
        "from client_a import request_a\nfrom client_b import request_b\n",
        """
        import settings, client_a, client_b
        assert settings.DEFAULT_TIMEOUT == 15
        assert client_a.request_a() == 15 and client_b.request_b() == 15
        assert not hasattr(client_a,'DEFAULT_TIMEOUT') and not hasattr(client_b,'DEFAULT_TIMEOUT')
        """,
        extra_files={
            "client_a.py": "DEFAULT_TIMEOUT=10\ndef request_a(): return DEFAULT_TIMEOUT\n",
            "client_b.py": "DEFAULT_TIMEOUT=30\ndef request_b(): return DEFAULT_TIMEOUT\n",
            "settings.py": "# shared settings\n",
        },
        expected=("client_a.py", "client_b.py", "settings.py"),
    ),
    _task(
        "multi-003", "dev", "multi_file_change", "Add a typed Result object in models.py and return it from service.compute.",
        "from service import compute\n",
        """
        from dataclasses import is_dataclass
        from models import Result
        from service import compute
        value=compute(2,3)
        assert is_dataclass(value) and isinstance(value, Result)
        assert value.value == 5 and value.ok is True
        """,
        extra_files={"models.py": "# domain models\n", "service.py": "def compute(a,b): return {'value':a+b,'ok':True}\n"},
        expected=("models.py", "service.py"),
    ),
    _task(
        "multi-004", "held_out", "multi_file_change", "Replace the legacy normalize helper with text_utils.normalize and update all consumers.",
        "def normalize(value): return value.strip().lower()\n",
        """
        import consumer_a, consumer_b, text_utils
        assert text_utils.normalize('  A B  ') == 'a b'
        assert consumer_a.key(' X ') == 'x' and consumer_b.equal(' A ','a')
        """,
        extra_files={
            "consumer_a.py": "from solution import normalize\ndef key(x): return normalize(x)\n",
            "consumer_b.py": "from solution import normalize\ndef equal(a,b): return normalize(a)==normalize(b)\n",
            "text_utils.py": "# canonical text utilities\n",
        },
        expected=("consumer_a.py", "consumer_b.py", "text_utils.py"),
    ),
    _task(
        "reliability-001", "dev", "reliability", "Fix retry so it validates attempts, retries only the configured exception, and returns the successful value.",
        """
        def retry(fn, attempts, retry_on=(Exception,)):
            for _ in range(attempts):
                try: fn()
                except Exception: pass
        """,
        """
        from solution import retry
        calls=[]
        def flaky():
            calls.append(1)
            if len(calls)<3: raise TimeoutError()
            return 'ok'
        assert retry(flaky,3,(TimeoutError,)) == 'ok' and len(calls)==3
        try: retry(lambda: 1,0)
        except ValueError: pass
        else: raise AssertionError('attempts')
        """,
    ),
    _task(
        "reliability-002", "dev", "reliability", "Fix TTLCache expiration using the injected clock; misses must not refresh TTL.",
        """
        class TTLCache:
            def __init__(self, ttl, clock): self.ttl,self.clock,self.data=ttl,clock,{}
            def set(self,k,v): self.data[k]=(v,self.clock())
            def get(self,k,default=None): return self.data.get(k,(default,0))[0]
        """,
        """
        from solution import TTLCache
        now=[0.0]; c=TTLCache(5,lambda:now[0]); c.set('x',1)
        now[0]=4.9; assert c.get('x')==1
        now[0]=5.0; assert c.get('x') is None
        assert c.get('missing','d')=='d'
        """,
    ),
    _task(
        "reliability-003", "dev", "reliability", "Ensure read_resource always closes the resource, including when read raises.",
        """
        def read_resource(factory):
            resource=factory()
            return resource.read()
        """,
        """
        from solution import read_resource
        class R:
            def __init__(self,fail=False): self.closed=False; self.fail=fail
            def read(self):
                if self.fail: raise OSError('x')
                return 'ok'
            def close(self): self.closed=True
        r=R(); assert read_resource(lambda:r)=='ok' and r.closed
        bad=R(True)
        try: read_resource(lambda:bad)
        except OSError: pass
        assert bad.closed
        """,
    ),
    _task(
        "reliability-004", "held_out", "reliability", "Make once thread-safe so concurrent callers execute the wrapped function exactly once.",
        """
        def once(fn):
            value=[]
            def wrapped():
                if not value: value.append(fn())
                return value[0]
            return wrapped
        """,
        """
        import threading, time
        from solution import once
        calls=[]
        @once
        def build(): time.sleep(.02); calls.append(1); return object()
        out=[]; threads=[threading.Thread(target=lambda:out.append(build())) for _ in range(20)]
        [t.start() for t in threads]; [t.join() for t in threads]
        assert len(calls)==1 and len({id(x) for x in out})==1
        """,
    ),
    _task(
        "security-001", "dev", "security_engineering", "Implement safe_join to reject absolute paths and traversal outside root, including symlink escapes.",
        """
        def safe_join(root, relative):
            return root / relative
        """,
        """
        import os, tempfile
        from pathlib import Path
        from solution import safe_join
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)/'root'; root.mkdir(); outside=Path(d)/'outside'; outside.mkdir()
            assert safe_join(root,'a.txt') == (root/'a.txt').resolve()
            for value in ('../outside/x', str(outside/'x')):
                try: safe_join(root,value)
                except ValueError: pass
                else: raise AssertionError(value)
            try: (root/'link').symlink_to(outside, target_is_directory=True)
            except OSError: pass
            else:
                try: safe_join(root,'link/x')
                except ValueError: pass
                else: raise AssertionError('symlink escape')
        """,
    ),
    _task(
        "security-002", "held_out", "security_engineering", "Redact authorization, token, password, and api_key values recursively without mutating input.",
        """
        def redact(value):
            return value
        """,
        """
        from solution import redact
        data={'Authorization':'Bearer secret','nested':{'password':'p','ok':1},'items':[{'api_key':'k'},{'token':'t'}]}
        out=redact(data)
        assert out['Authorization']=='[REDACTED]' and out['nested']['password']=='[REDACTED]'
        assert out['items'][0]['api_key']=='[REDACTED]' and out['items'][1]['token']=='[REDACTED]'
        assert data['nested']['password']=='p'
        """,
    ),
    _task(
        "security-003", "held_out", "security_engineering", "Implement sanitize_filename for Windows and POSIX: strip paths, reserved names, controls, and trailing dots/spaces.",
        """
        def sanitize_filename(value):
            return value
        """,
        """
        from solution import sanitize_filename
        assert sanitize_filename('../../a.txt') == 'a.txt'
        assert sanitize_filename(r'C:\\temp\\x?.txt') == 'x_.txt'
        assert sanitize_filename(' report. ') == 'report'
        assert sanitize_filename('CON').lower() != 'con'
        try: sanitize_filename('..')
        except ValueError: pass
        else: raise AssertionError('empty/reserved')
        """,
    ),
    _task(
        "security-004", "held_out", "security_engineering", "Implement atomic_write_text using same-directory replacement and cleanup temporary files on failure.",
        """
        def atomic_write_text(path, text):
            temporary = path.with_suffix('.tmp')
            temporary.write_text(text)
            path.write_text(text)
        """,
        """
        import tempfile
        from pathlib import Path
        from solution import atomic_write_text
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'value.txt'; p.write_text('old')
            atomic_write_text(p,'new')
            assert p.read_text()=='new'
            assert [x for x in Path(d).iterdir()] == [p]
        """,
    ),
)


def public_manifest() -> dict:
    tasks = []
    for task in TASKS:
        item = asdict(task)
        item.pop("evaluator_source")
        item["seed_files"] = sorted(item["seed_files"])
        tasks.append(item)
    return {
        "schema_version": 1,
        "suite": "forge-replay-coding-tasks-v1",
        "task_count": len(TASKS),
        "split_counts": {
            split: sum(task.split == split for task in TASKS)
            for split in ("dev", "held_out")
        },
        "tasks": tasks,
    }


def catalog_sha256() -> str:
    private_manifest = [asdict(task) for task in TASKS]
    canonical = json.dumps(private_manifest, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
