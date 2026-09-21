//! Subject grammar and matching, in process. The black-box suite can only probe
//! the corners the protocol can reach; this is the whole table.

use nats_server_rs::subjects::{has_wildcard, is_valid, is_valid_publish, matches, tokens};

#[test]
fn validity_matches_the_reference_grammar() {
    let valid = [
        "foo",
        "foo.bar",
        "*",
        ">",
        "foo.>",
        "foo.*",
        "*.>",
        "*.*.bar",
        "foo.*x",     // a literal token that happens to contain a star
        "a.b.c.d.e.f",
        "F1_2-3",
        "ünïcode.✓",  // the reference only rejects control characters and blanks
    ];
    let invalid = [
        "",
        "foo..bar",
        ".foo",
        "foo.",
        ">",
        "foo.>.bar", // `>` only as the final token
        "foo.>.",
        "..",
        ".",
    ];
    for s in valid {
        assert!(is_valid(s.as_bytes()), "{s:?} must be valid");
    }
    // ">" alone *is* valid; it is in the valid list. The invalid list must not
    // contradict it, so drop the duplicate before asserting.
    for s in invalid {
        if s == ">" {
            continue;
        }
        assert!(!is_valid(s.as_bytes()), "{s:?} must be invalid");
    }
}

#[test]
fn publish_subjects_must_be_literal() {
    for s in ["foo", "foo.bar", "foo.*x"] {
        assert!(is_valid_publish(s.as_bytes()), "{s:?}");
    }
    for s in ["*", ">", "foo.*", "foo.>", "*.>", "foo..bar", ""] {
        assert!(!is_valid_publish(s.as_bytes()), "{s:?}");
    }
    for s in ["foo.*", ">", "*.x"] {
        assert!(has_wildcard(s.as_bytes()), "{s:?}");
    }
    for s in ["foo", "foo.*x", "foo.bar"] {
        assert!(!has_wildcard(s.as_bytes()), "{s:?}");
    }
}

#[test]
fn matching_is_token_wise() {
    let cases: &[(&str, &str, bool)] = &[
        ("foo", "foo", true),
        ("foo", "bar", false),
        ("foo", "foo.bar", false),        // no implicit prefix match
        ("foo", "foobar", false),         // and not a string prefix either
        ("foo.bar", "foo.bar", true),
        ("foo.*", "foo.bar", true),
        ("foo.*", "foo.bar.baz", false),  // `*` is exactly one token
        ("foo.*", "foo", false),
        ("*", "foo", true),
        ("*", "foo.bar", false),
        ("*", "a.b.c", false),
        ("*.bar", "foo.bar", true),
        ("foo.>", "foo.bar", true),
        ("foo.>", "foo.bar.baz", true),
        ("foo.>", "foo", false), // `>` needs at least one token of its own
        (">", "foo", true),
        (">", "foo.bar.baz", true),
        ("foo.*.>", "foo.bar", false),
        ("foo.*.>", "foo.bar.baz", true),
        ("foo.*.>", "foo.baz", false),
        ("foo.*x", "foo.*x", true), // both sides literal
        ("foo.*x", "foo.bar", false),
    ];
    for (filter, subject, want) in cases {
        assert_eq!(
            matches(filter.as_bytes(), subject.as_bytes()),
            *want,
            "{filter:?} vs {subject:?}"
        );
    }
}

/// Cross-check the fast matcher against a slow, obvious one over a generated
/// space. The slow one is only ever used here, so agreement is the evidence.
#[test]
fn matches_a_naive_reference_matcher() {
    fn naive(filter: &str, subject: &str) -> bool {
        let f: Vec<&str> = filter.split('.').collect();
        let s: Vec<&str> = subject.split('.').collect();
        for i in 0..f.len() {
            if f[i] == ">" {
                return i < s.len() && f.len() - 1 == i;
            }
            if i >= s.len() {
                return false;
            }
            if f[i] != "*" && f[i] != s[i] {
                return false;
            }
        }
        f.len() == s.len()
    }
    let parts = ["a", "b", "*", ">", "ab"];
    let mut built: Vec<String> = Vec::new();
    for n in 1..=3 {
        for i in 0..parts.len() {
            for j in 0..parts.len() {
                for k in 0..parts.len() {
                    let mut v = vec![parts[i]];
                    if n > 1 {
                        v.push(parts[j]);
                    }
                    if n > 2 {
                        v.push(parts[k]);
                    }
                    built.push(v.join("."));
                }
            }
        }
    }
    let mut checked = 0;
    for filter in &built {
        if !is_valid(filter.as_bytes()) {
            continue;
        }
        for subject in &built {
            if !is_valid(subject.as_bytes()) || has_wildcard(subject.as_bytes()) {
                continue;
            }
            assert_eq!(
                matches(filter.as_bytes(), subject.as_bytes()),
                naive(filter, subject),
                "filter {filter:?} subject {subject:?}"
            );
            checked += 1;
        }
    }
    assert!(checked > 1_000, "only checked {checked} pairs");
}

#[test]
fn token_counts_are_the_separator_count_plus_one() {
    assert_eq!(tokens(b"foo"), 1);
    assert_eq!(tokens(b"foo.bar"), 2);
    assert_eq!(tokens(b"a.b.c.d"), 4);
    assert_eq!(tokens(b""), 0);
}
