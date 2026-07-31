#!/usr/bin/env bash
# The safety rail: what the campaign refuses to run on, and the only
# destructive operation it owns (--fresh).
#
# ADR 0009 ground rule 1: the driver refuses unless the mount is the
# designated one, it is xfs, it is writable by the invoking user, and a
# sentinel file proves intent. The scripts never mkfs, never mount, never
# sudo, and never write outside the store root and the results directory.
#
# The single escape hatch is QSSRT_UNSAFE_ALLOW_ANY_PATH=1, which turns the
# hardware checks into SKIP lines so the harness can be exercised against a
# tempdir. A run that uses it is stamped NOT CAMPAIGN GRADE in its verdict:
# a smoke verdict must never be mistakable for a campaign verdict.

# True when the hardware rail has been waived for a smoke run.
rail_waived() { [ "${QSSRT_UNSAFE_ALLOW_ANY_PATH:-0}" = "1" ]; }

# Is `path` a mount point? findmnt when it exists, otherwise the st_dev
# comparison against the parent -- the same test the ADR describes.
rail_is_mountpoint() {
    local path="$1"
    if qssrt_have findmnt; then
        findmnt -no TARGET "$path" >/dev/null 2>&1 && return 0
        return 1
    fi
    [ -d "$path" ] || return 1
    local here parent
    here=$(stat -c %d "$path" 2>/dev/null) || return 1
    parent=$(stat -c %d "$path/.." 2>/dev/null) || return 1
    [ "$here" != "$parent" ]
}

# Filesystem type of the mount holding `path`.
rail_fstype() { stat -f -c %T "$1" 2>/dev/null; }

# Checks 1-4 of the rail: the hardware itself. Each one is an individual
# check line so a refusal names which condition failed.
rail_check_hardware() {
    local mount="$QSSRT_MOUNT"

    if rail_waived; then
        check_skip "mount $mount is a mount point" "QSSRT_UNSAFE_ALLOW_ANY_PATH=1"
        check_skip "mount $mount is $QSSRT_REQUIRE_FSTYPE" "QSSRT_UNSAFE_ALLOW_ANY_PATH=1"
        check_skip "mount $mount is owned and writable by $(id -un)" \
            "QSSRT_UNSAFE_ALLOW_ANY_PATH=1"
        check_skip "sentinel $QSSRT_SENTINEL exists" "QSSRT_UNSAFE_ALLOW_ANY_PATH=1"
        return 0
    fi

    if rail_is_mountpoint "$mount"; then
        check_pass "mount $mount is a mount point"
    else
        check_fail "mount $mount is a mount point" \
            "not a mount point; the campaign runs on the designated disk only"
    fi

    local fstype
    fstype=$(rail_fstype "$mount")
    assert_eq "mount $mount is $QSSRT_REQUIRE_FSTYPE" "$QSSRT_REQUIRE_FSTYPE" "$fstype"

    local owner
    owner=$(stat -c %u "$mount" 2>/dev/null)
    if [ -w "$mount" ] && [ "$owner" = "$(id -u)" ]; then
        check_pass "mount $mount is owned and writable by $(id -un)"
    else
        check_fail "mount $mount is owned and writable by $(id -un)" \
            "uid $(id -u) versus owner uid ${owner:-none}, writable=$([ -w "$mount" ] && echo yes || echo no)"
    fi

    if [ -e "$QSSRT_SENTINEL" ]; then
        check_pass "sentinel $QSSRT_SENTINEL exists"
    else
        check_fail "sentinel $QSSRT_SENTINEL exists" \
            "create it once, by hand, as proof of intent: touch $QSSRT_SENTINEL"
    fi
}

# Check 5: the store root is a directory strictly below the mount.
#
# ADR ground rule 1 says the store root is "exactly /s3"; answered review
# ask 4 says --fresh wipes "the store's own directories (never the mount)".
# A store rooted at the mount point has no own directory to wipe, so the
# later and more specific decision wins: the mount is /s3 and the store is a
# directory under it.
rail_check_store_root() {
    local root="$QSSRT_STORE_ROOT" mount="$QSSRT_MOUNT"
    if [ "$root" = "$mount" ]; then
        check_fail "store root is below the mount, not the mount itself" \
            "store root $root is the mount; --fresh could not tell them apart"
        return 1
    fi
    if rail_waived; then
        check_skip "store root $root is under $mount" "QSSRT_UNSAFE_ALLOW_ANY_PATH=1"
        return 0
    fi
    case "$root/" in
    "$mount"/*)
        check_pass "store root $root is under $mount"
        ;;
    *)
        check_fail "store root $root is under $mount" \
            "the campaign writes nothing outside the mount and the results dir"
        return 1
        ;;
    esac
}

# The campaign's own children of the store root. Anything else living there
# means this is not our directory, and --fresh refuses it.
QSSRT_STORE_CHILDREN=(s3 resp)

# Is a store present at `path` (both databases)?
rail_store_exists() {
    [ -d "$1/db" ] && [ -d "$1/blocks/db" ]
}

# Does the store root hold anything at all?
rail_store_root_empty() {
    [ ! -d "$QSSRT_STORE_ROOT" ] && return 0
    [ -z "$(ls -A "$QSSRT_STORE_ROOT" 2>/dev/null)" ]
}

# --fresh: wipe the store's own directory, never the mount.
#
# Refuses unless every one of these holds:
#   - the store root is below the mount and is not the mount (checked above);
#   - it is empty (nothing to do), or
#   - it holds a real qss store AND every top-level entry is one this
#     campaign creates.
#
# The mount is never an argument to a delete, in any code path here.
rail_fresh() {
    local root="$QSSRT_STORE_ROOT" entry keep

    if [ "$root" = "/" ] || [ "$root" = "$QSSRT_MOUNT" ] || [ -z "$root" ]; then
        check_fail "--fresh refuses to wipe $root" "that is the mount or the root"
        return 1
    fi

    if rail_store_root_empty; then
        mkdir -p "$root"
        check_pass "--fresh: store root was already empty" "$root"
        return 0
    fi

    for entry in "$root"/*; do
        [ -e "$entry" ] || continue
        keep=0
        for child in "${QSSRT_STORE_CHILDREN[@]}"; do
            [ "$(basename "$entry")" = "$child" ] && keep=1
        done
        if [ "$keep" = 0 ]; then
            check_fail "--fresh refuses an unrecognised entry" \
                "$entry is not something this campaign created"
            return 1
        fi
    done

    if [ -d "$root/s3" ] && ! rail_store_exists "$root/s3"; then
        check_fail "--fresh refuses a directory that is not a qss store" \
            "$root/s3 has no db/ and blocks/db/; wipe it by hand if you meant to"
        return 1
    fi

    if [ -d "$root/s3" ]; then
        if fsck_can_open "$root/s3"; then
            check_pass "--fresh: fsck confirms $root/s3 is a qss store"
        else
            check_fail "--fresh: fsck could not confirm $root/s3 is a qss store" \
                "refusing to wipe something fsck will not open"
            return 1
        fi
    fi

    for child in "${QSSRT_STORE_CHILDREN[@]}"; do
        if [ -e "$root/$child" ]; then
            rm -rf "${root:?}/${child:?}" || {
                check_fail "--fresh: removing $root/$child" "rm failed"
                return 1
            }
        fi
    done
    mkdir -p "$root"
    check_pass "--fresh: store wiped" "$root"
}
