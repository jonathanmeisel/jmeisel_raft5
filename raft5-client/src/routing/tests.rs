use super::*;

fn member(id: &str, client_address: &str) -> ClusterMember {
    ClusterMember {
        server_id: id.to_string(),
        address: format!("http://{id}.peer:1"),
        client_address: client_address.to_string(),
        voting: true,
    }
}

fn router(seeds: &[&str]) -> Router {
    Router::new(seeds, None).expect("seeds are valid addresses")
}

#[test]
fn a_client_needs_somewhere_to_start() {
    assert!(matches!(
        Router::new(Vec::<String>::new(), None),
        Err(Error::NoSeeds)
    ));
    assert!(matches!(
        Router::new(["not a url"], None),
        Err(Error::InvalidAddress(_))
    ));
}

/// The cluster is the authority on its own membership: a seed it doesn't
/// list has left (or never joined), and a member we were never seeded
/// with is somewhere new to ask.
#[test]
fn adopting_a_configuration_replaces_the_seed_list() {
    let router = router(&["http://a:1", "http://gone:1"]);

    router.adopt(vec![
        member("a", "http://a:1"),
        member("c", "http://c:1"), // never seeded
    ]);

    let endpoints = router.endpoints();
    assert_eq!(endpoints.len(), 2, "{endpoints:?}");
    assert!(endpoints.iter().any(|e| e.contains("//a:1")));
    assert!(endpoints.iter().any(|e| e.contains("//c:1")));
    assert!(
        !endpoints.iter().any(|e| e.contains("gone")),
        "a seed the cluster no longer lists should be dropped: {endpoints:?}"
    );
}

/// What makes a `leader_hint` actionable: it names a server, and only
/// the configuration says where that server's client API is.
#[tokio::test]
async fn a_hint_is_followed_once_the_configuration_names_the_server() {
    let router = router(&["http://a:1"]);

    // Nothing known yet, so the hint can't be resolved and the leader
    // has to be discovered instead -- which, with nothing listening,
    // fails rather than dialing anywhere.
    assert!(matches!(
        router.leader(Some("b".to_string())).await,
        Err(Error::Unavailable(_))
    ));

    router.adopt(vec![member("a", "http://a:1"), member("b", "http://b:1")]);
    let leader = router
        .leader(Some("b".to_string()))
        .await
        .expect("a hint we have an address for needs no discovery");
    assert!(leader.key.contains("//b:1"), "{}", leader.key);
    assert_eq!(
        router.cached().map(|l| l.key),
        Some(leader.key),
        "the connection to the new leader should be kept for the next call"
    );
}

// `connect_lazy` dials nothing, but it still registers with the
// reactor, so remembering a leader needs a runtime under it.
#[tokio::test]
async fn a_leader_that_leaves_the_cluster_is_forgotten() {
    let router = router(&["http://a:1", "http://b:1"]);
    router.adopt(vec![member("a", "http://a:1"), member("b", "http://b:1")]);
    let node = router.nodes().into_iter().next().unwrap();
    router.remember(&node);
    assert!(router.cached().is_some());

    router.adopt(vec![member("b", "http://b:1")]);
    assert!(
        router.cached().is_none(),
        "a cached leader that is no longer a member should not be dialed again"
    );
}

/// A node with no configuration at all -- not bootstrapped, not yet
/// added to a cluster -- knows less than the client does.
#[test]
fn an_empty_configuration_is_not_adopted() {
    let router = router(&["http://a:1"]);
    router.adopt(Vec::new());
    assert_eq!(router.endpoints().len(), 1);
}
