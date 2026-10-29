#!/usr/bin/env python3
"""`foundation-release.sh prune`: remove the releases the retention contract does not keep.

Moved out of foundation-release.sh unchanged (the line limit); see `prune_releases` there for what
it keeps, refuses and how it exits. Run from the control checkout, never from a release.
"""
import contextlib, importlib.machinery, importlib.util, json, os, re, shutil, stat, subprocess, sys
from pathlib import Path

SHA = re.compile(r"[0-9a-f]{40}")
contract_path, release_root, control_releases, admission_path = map(Path, sys.argv[1:5])
IMAGES = ("foundation-outbox-publisher", "foundation-tippecanoe")


def refuse(message):
    print(f"prune refused: {message}; nothing was removed", file=sys.stderr)
    sys.exit(65)


build_lock = contextlib.ExitStack()
try:
    loader = importlib.machinery.SourceFileLoader("foundation_release_admission", str(admission_path))
    admission = importlib.util.module_from_spec(importlib.util.spec_from_loader(loader.name, loader))
    loader.exec_module(admission)
    build_lock.enter_context(admission.release_build_lock())
except ValueError as error:
    print(f"prune-skipped: {error}; nothing was removed. Run prune after the build finishes.")
    sys.exit(0)
except (OSError, ImportError, SyntaxError, AttributeError) as error:
    refuse(f"cannot take the release build lock through {admission_path}: {error!r}")


try:
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    emergency, keep_newest = contract["emergency_release"], contract["keep_newest"]
except (OSError, ValueError, KeyError, TypeError) as error:
    refuse(f"retention contract {contract_path} is unreadable: {error!r}")
if not isinstance(emergency, str) or not SHA.fullmatch(emergency):
    refuse(f"{contract_path}: emergency_release must be a 40-character Git SHA")
if type(keep_newest) is not int or not 1 <= keep_newest <= 20:
    refuse(f"{contract_path}: keep_newest must be an integer from 1 to 20")


def link_release(link, required):
    if not link.is_symlink():
        if required or link.exists():
            refuse(f"{link} is not a release link")
        return None
    match = re.fullmatch(r"releases/([0-9a-f]{40})", os.readlink(link))
    if not match:
        refuse(f"{link} does not point at releases/<sha>: {os.readlink(link)}")
    return match[1]


def ids_in(directory):
    if not directory.is_dir():
        return set()
    return {entry.name for entry in directory.iterdir() if SHA.fullmatch(entry.name)}


def disk_bytes(path):
    total, seen = 0, set()
    for root, _, files in os.walk(path):
        for name in [root, *(os.path.join(root, file) for file in files)]:
            info = os.lstat(name)
            if (info.st_dev, info.st_ino) not in seen:
                seen.add((info.st_dev, info.st_ino))
                total += info.st_blocks * 512
    return total


def remove_tree(path):
    """Admitted releases and artifacts are read-only; lift that only on what is being removed.

    A release is also immutable (chattr +i, root ADR-0184), lifted with admission's own code, the
    code that set it; on a tree that was never sealed it changes nothing.
    """
    if not path.exists() and not path.is_symlink():
        return 0
    if path.is_symlink() or not path.is_dir():
        raise OSError(f"{path} is not a plain directory")
    try:
        admission.set_immutable(path, False)
    except ValueError as error:
        raise OSError(str(error)) from error
    size = disk_bytes(path)
    for root, _, _ in os.walk(path):
        os.chmod(root, stat.S_IRWXU)
    shutil.rmtree(path)
    return size


def docker(*args):
    try:
        return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=300)
    except (OSError, subprocess.SubprocessError) as error:
        return subprocess.CompletedProcess(args, 127, "", str(error))


class Unanswered(Exception):
    """Docker answered, but not with what was asked: nothing may be decided from it."""


def image(reference):
    """(id, size) of a local image, or None when Docker does not have it."""
    found = docker("image", "inspect", "--format", "{{.Id}} {{.Size}}", reference)
    if found.returncode != 0:
        return None
    fields = found.stdout.split()
    if len(fields) != 2 or not fields[1].isdigit():
        raise Unanswered(f"docker image inspect {reference} answered {found.stdout.strip()[:200]!r}")
    return fields[0], int(fields[1])


def image_or_none(reference):
    try:
        return image(reference)
    except Unanswered:
        return None


def newest(ids, *roots):
    def installed(release_id):
        for root in roots:
            path = root / release_id
            if path.exists():
                return path.lstat().st_mtime
        return 0
    return sorted(ids, key=lambda release_id: (installed(release_id), release_id), reverse=True)[:keep_newest]


# What every running process holds: working directory, executable, root, open files, arguments and
# environment. A unit started from an older `current` still runs out of that release's directory.
def process_references():
    held = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit() or int(pid) == os.getpid():
            continue
        base, strings = Path("/proc", pid), []
        for name in ("cwd", "exe", "root"):
            try:
                strings.append(os.readlink(base / name))
            except OSError:
                pass
        try:
            for fd in os.listdir(base / "fd"):
                try:
                    strings.append(os.readlink(base / "fd" / fd))
                except OSError:
                    pass
        except OSError:
            pass
        for name in ("cmdline", "environ"):
            try:
                strings.extend((base / name).read_bytes().decode("utf-8", "replace").split("\0"))
            except OSError:
                pass
        try:
            command = (base / "comm").read_text(encoding="utf-8").strip()
        except OSError:
            command = "?"
        held.append((f"process {pid} ({command})", [value for value in strings if value]))
    return held


def container_references():
    running = docker("ps", "-q", "--no-trunc")
    if running.returncode != 0:
        refuse(f"docker cannot list running containers: {running.stderr.strip()}")
    ids = running.stdout.split()
    if not ids:
        return []
    found = docker("inspect", "--format",
                   "{{.Name}} {{.Image}} {{.Config.Image}}{{range .Mounts}} {{.Source}}{{end}}", *ids)
    if found.returncode != 0:
        refuse(f"docker cannot inspect running containers: {found.stderr.strip()}")
    held = []
    for line in found.stdout.splitlines():
        fields = line.split()
        if fields:
            held.append((f"container {fields[0].lstrip('/')}", fields[1:]))
    return held


def holder(paths, images, held):
    for who, strings in held:
        for value in strings:
            if value in images or any(path in value for path in paths):
                return who
    return None


def recorded_images(release_id):
    """Image IDs this release's build.json names."""
    try:
        build = json.loads((release_root / "artifacts" / release_id / "build.json").read_text(encoding="utf-8"))
        return {value for key, value in build.items() if key.endswith("_image") and isinstance(value, str)}
    except (OSError, ValueError, AttributeError):
        return set()


releases = release_root / "releases"
current = link_release(release_root / "current", required=True)
previous = link_release(release_root / "previous", required=False)
present = ids_in(releases) | ids_in(release_root / "artifacts") | ids_in(release_root / "config")
keep = {}
keep[current] = "current"
if previous:
    keep.setdefault(previous, "previous")
keep.setdefault(emergency, "emergency")  # retention rule: the emergency return's release
for release_id in newest(present, releases, release_root / "artifacts", release_root / "config"):
    keep.setdefault(release_id, "newest")

control_current = None
if control_releases.is_dir():
    control_current = link_release(control_releases.parent / "current", required=True)
control_present = ids_in(control_releases)
control_keep = {}
if control_current:
    control_keep[control_current] = "current"
for release_id in newest(control_present, control_releases):
    control_keep.setdefault(release_id, "newest")

# A cached build gives two releases the same image ID (tippecanoe rarely changes). An image a kept
# release's build.json names belongs to that release: it neither ties an old release to a running
# container nor goes when the old release's last tag on it does.
kept_images = set().union(*(recorded_images(release_id) for release_id in keep))
kept_tagged = {found[0] for release_id in keep for name in IMAGES
               if (found := image_or_none(f"{name}:{release_id}"))}

held = process_references() + container_references()
removed = refused = failed = 0
freed = 0
for release_id in sorted(present):
    if release_id in keep:
        print(f"prune kept {release_id} ({keep[release_id]})")
        continue
    paths = [str(root / release_id) for root in (releases, release_root / "artifacts", release_root / "config")]
    try:
        tags = {f"{name}:{release_id}": image(f"{name}:{release_id}") for name in IMAGES}
    except Unanswered as error:
        print(f"prune refused {release_id}: {error}", file=sys.stderr)
        refused += 1
        continue
    own = recorded_images(release_id) | {found[0] for found in tags.values() if found}
    who = holder(paths, (own - kept_images) | set(tags), held)
    if who:
        print(f"prune refused {release_id}: still referenced by {who}", file=sys.stderr)
        refused += 1
        continue
    try:
        sizes = {"release": remove_tree(releases / release_id),
                 "artifacts": remove_tree(release_root / "artifacts" / release_id),
                 "config": remove_tree(release_root / "config" / release_id),
                 "images": 0}
    except OSError as error:
        print(f"prune failed {release_id}: {error}", file=sys.stderr)
        failed += 1
        continue
    # The tag goes; the image goes with it unless another tag or any container still uses it,
    # which Docker decides and reports. The last tag on an image a kept release built stays:
    # removing it would delete that release's image.
    for tag, found in tags.items():
        if not found:
            continue
        if found[0] in kept_images and found[0] not in kept_tagged:
            print(f"prune kept image {tag}: {found[0]} is a kept release's build and no kept tag holds it")
            continue
        gone = docker("image", "rm", tag)
        if gone.returncode != 0:
            print(f"prune kept image {tag}: {gone.stderr.strip()}", file=sys.stderr)
        elif image_or_none(found[0]) is None:
            sizes["images"] += found[1]
    freed += sum(sizes.values())
    removed += 1
    print(f"prune removed {release_id} " + " ".join(f"{key}={value}" for key, value in sizes.items()))

for release_id in sorted(control_present):
    if release_id in control_keep:
        print(f"prune kept control {release_id} ({control_keep[release_id]})")
        continue
    path = control_releases / release_id
    who = holder([str(path)], set(), held)
    if who:
        print(f"prune refused control {release_id}: still referenced by {who}", file=sys.stderr)
        refused += 1
        continue
    try:
        size = remove_tree(path)
    except OSError as error:
        print(f"prune failed control {release_id}: {error}", file=sys.stderr)
        failed += 1
        continue
    freed += size
    removed += 1
    print(f"prune removed control {release_id} release={size}")

verdict = "prune-ok" if not refused and not failed else "prune-incomplete"
print(f"{verdict} removed={removed} refused={refused} failed={failed} freed_bytes={freed} "
      f"freed_gib={freed / 2**30:.2f}")
sys.exit(0 if verdict == "prune-ok" else 75)
