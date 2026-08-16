#![cfg(target_os = "linux")]

use std::path::PathBuf;

use blockd_core::authority::{PlacementRecord, VnodeAuthority, VnodeId, VnodePlacement};
use blockd_core::engine::{commit_vnode_closure, read_vnode_closure, read_vnode_member};
use blockd_core::layout;
use blockd_core::types::{HostId, VsetId};
use blockd_core::vnode_member::VnodeMemberRecord;
use blockd_core::world::Blobs;
use blockd_exec::ProductionContext;
use blockd_runtime::world::FileBlobs;

fn test_root() -> PathBuf {
    std::env::temp_dir().join(format!(
        "blockd-vnode-authority-{}-{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ))
}

#[tokio::test]
async fn fsynced_generation_and_closure_survive_reopen_and_a_torn_tail() {
    let root = test_root();
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale test root");
    }
    let placement = PlacementRecord::new(
        91,
        1,
        vec![VnodePlacement {
            vnode: VnodeId(0),
            members: [HostId(1), HostId(2), HostId(3)],
            next_members: None,
        }],
    )
    .expect("valid placement");
    let authority = VnodeAuthority {
        cluster_id: 91,
        placement_epoch: 1,
        vnode: VnodeId(0),
        generation: 7,
        primary: HostId(1),
        primary_session: 44,
        primary_host_epoch: 3,
    };
    let blobs = FileBlobs::new(&root);
    ProductionContext::new(|_| {})
        .scope({
            let placement = placement.clone();
            let blobs = blobs.clone();
            async move {
                Blobs::append(
                    &blobs,
                    layout::vnode_member_blob(VnodeId(0)),
                    VnodeMemberRecord::new(authority).encode(&placement),
                )
                .await
            }
        })
        .await
        .expect("persist initial generation");
    let closure = ProductionContext::new(|_| {})
        .scope({
            let placement = placement.clone();
            let blobs = blobs.clone();
            async move {
                commit_vnode_closure(
                    &blobs,
                    &placement,
                    authority,
                    VsetId(7),
                    55,
                    b"durable protected closure".to_vec(),
                )
                .await
            }
        })
        .await
        .expect("commit closure");

    let newer = VnodeMemberRecord {
        authority: VnodeAuthority {
            generation: 8,
            primary: HostId(2),
            primary_session: 45,
            primary_host_epoch: 4,
            ..authority
        },
        closures: vec![closure],
    }
    .encode(&placement);
    ProductionContext::new(|_| {})
        .scope({
            let blobs = blobs.clone();
            let torn = newer[..newer.len() / 2].to_vec();
            async move { Blobs::append(&blobs, layout::vnode_member_blob(VnodeId(0)), torn).await }
        })
        .await
        .expect("append crash-torn next generation");

    let reopened = FileBlobs::new(&root);
    let recovered = ProductionContext::new(|_| {})
        .scope({
            let placement = placement.clone();
            let reopened = reopened.clone();
            async move { read_vnode_member(&reopened, &placement, VnodeId(0)).await }
        })
        .await
        .expect("read member log")
        .expect("member state exists");
    assert_eq!(recovered.authority, authority);
    assert_eq!(recovered.closure(VsetId(7)), Some(closure));
    assert_eq!(
        ProductionContext::new(|_| {})
            .scope({
                let reopened = reopened.clone();
                async move { read_vnode_closure(&reopened, VnodeId(0), closure).await }
            })
            .await
            .expect("read verified closure"),
        b"durable protected closure"
    );

    std::fs::remove_dir_all(root).expect("clean test root");
}
